use chrono::{DateTime, Utc};
use futures_util::FutureExt as _;
use sqlx::{Acquire, PgPool, Row, SqlitePool};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use buzz_core::CommunityId;

use crate::{
    action::AuditAction,
    entry::{AuditEntry, NewAuditEntry},
    error::AuditError,
    hash::compute_hash,
};

/// Per-community advisory lock key. Derived in Postgres from the community UUID
/// so two communities never serialize each other's audit writes (which would be
/// both a throughput bottleneck and a cross-tenant timing oracle). The lock is
/// taken with `pg_advisory_lock(hashtextextended(...))` — see [`AuditService::log`].
const AUDIT_LOCK_NAMESPACE: &str = "buzz_audit:";

/// `BEGIN IMMEDIATE` for the SQLite append transaction: take the write lock at
/// transaction start rather than at the first write, so the read-head + insert
/// cycle never upgrades a read transaction mid-flight.
const SQLITE_BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";

/// Storage backend behind an [`AuditService`]. The chain logic
/// ([`compute_hash`]) is shared; only row storage and append serialization
/// differ per backend.
enum AuditBackend {
    /// Multi-process Postgres deployment: appends serialize on a
    /// per-community `pg_advisory_lock`.
    Pg { pool: PgPool },
    /// Single-process SQLite Solo profile: appends serialize on an in-process
    /// mutex held across the read-head + insert transaction. The Solo profile
    /// runs exactly one relay process over the database file, so an
    /// in-process lock is a complete substitute for the advisory lock — there
    /// is no second process that could interleave an append. (It is coarser —
    /// all communities share one lock — which is acceptable at Solo scale.)
    Sqlite {
        pool: SqlitePool,
        append_lock: tokio::sync::Mutex<()>,
    },
}

/// Append-only, per-community hash-chain audit log.
///
/// Each community has an independent chain keyed `(community_id, seq)`. On the
/// Postgres backend ([`AuditService::new`]), writes for one community are
/// serialized by a per-community advisory lock so the chain stays consistent
/// across relay processes; different communities proceed in parallel. On the
/// SQLite backend ([`AuditService::new_sqlite`], single-process Solo profile),
/// appends serialize on an in-process mutex instead. The hash chain itself
/// ([`compute_hash`]) is backend-neutral: the same logical entries produce the
/// same chain on either backend.
pub struct AuditService {
    backend: AuditBackend,
}

impl AuditService {
    /// Creates a new Postgres-backed `AuditService` using the given connection
    /// pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            backend: AuditBackend::Pg { pool },
        }
    }

    /// Creates a new SQLite-backed `AuditService` (single-process Solo
    /// profile) using the given connection pool.
    ///
    /// The pool must point at a database that has had buzz-db's SQLite
    /// migrations applied (the `audit_log` table ships in migration 0002).
    pub fn new_sqlite(pool: SqlitePool) -> Self {
        Self {
            backend: AuditBackend::Sqlite {
                pool,
                append_lock: tokio::sync::Mutex::new(()),
            },
        }
    }

    /// Append a new entry to the calling community's chain.
    ///
    /// Postgres: serialized per-community via `pg_advisory_lock`. SQLite:
    /// serialized by the service's append mutex (see [`AuditBackend::Sqlite`]).
    #[instrument(skip(self, entry), fields(action = %entry.action))]
    pub async fn log(&self, entry: NewAuditEntry) -> Result<AuditEntry, AuditError> {
        match &self.backend {
            AuditBackend::Pg { pool } => self.log_pg(pool, entry).await,
            AuditBackend::Sqlite { pool, append_lock } => {
                self.log_sqlite(pool, append_lock, entry).await
            }
        }
    }

    /// Postgres append path. Advisory locks are session-scoped, so we acquire
    /// before the transaction and release after commit (or on any error path).
    async fn log_pg(&self, pool: &PgPool, entry: NewAuditEntry) -> Result<AuditEntry, AuditError> {
        let mut conn = pool.acquire().await?;

        // Per-community advisory lock: hash the namespaced community id to an
        // i64 lock key inside Postgres. Communities lock independently.
        let lock_key = format!("{AUDIT_LOCK_NAMESPACE}{}", entry.community_id);
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *conn)
            .await?;

        // Run the chain append and release the lock regardless of outcome.
        // catch_unwind so a panic still releases the lock before the connection
        // returns to the pool.
        let result = std::panic::AssertUnwindSafe(self.log_inner(&mut conn, entry))
            .catch_unwind()
            .await;

        let _ = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(&lock_key)
            .execute(&mut *conn)
            .await;

        match result {
            Ok(inner_result) => inner_result,
            Err(panic_payload) => std::panic::resume_unwind(panic_payload),
        }
    }

    async fn log_inner(
        &self,
        conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        entry: NewAuditEntry,
    ) -> Result<AuditEntry, AuditError> {
        let mut tx = conn.begin().await?;

        // The stored row keys on the raw UUID; the typed `CommunityId` on the
        // input is the provenance fence, dereferenced here at the DB boundary.
        let community_id = *entry.community_id.as_uuid();

        // Head of THIS community's chain — scoped by community_id.
        let head = sqlx::query(
            "SELECT seq, hash FROM audit_log
             WHERE community_id = $1
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(community_id)
        .fetch_optional(&mut *tx)
        .await?;

        let (prev_seq, prev_hash): (i64, Option<Vec<u8>>) = match head {
            Some(row) => (
                row.get::<i64, _>("seq"),
                Some(row.get::<Vec<u8>, _>("hash")),
            ),
            None => (0, None), // community's first entry
        };
        let seq = prev_seq + 1;

        let created_at: DateTime<Utc> = Utc::now();

        let mut audit_entry = AuditEntry {
            community_id,
            seq,
            hash: Vec::new(),
            prev_hash,
            action: entry.action,
            actor_pubkey: entry.actor_pubkey,
            object_id: entry.object_id,
            detail: entry.detail,
            created_at,
        };

        audit_entry.hash = compute_hash(&audit_entry)?.to_vec();

        debug!(seq, "writing audit entry");

        sqlx::query(
            r#"
            INSERT INTO audit_log
                (community_id, seq, hash, prev_hash, action, actor_pubkey, object_id, detail, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(audit_entry.community_id)
        .bind(audit_entry.seq)
        .bind(&audit_entry.hash)
        .bind(audit_entry.prev_hash.as_deref())
        .bind(audit_entry.action.as_str())
        .bind(audit_entry.actor_pubkey.as_deref())
        .bind(audit_entry.object_id.as_deref())
        .bind(&audit_entry.detail)
        .bind(audit_entry.created_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(audit_entry)
    }

    /// SQLite append path (single-process Solo profile).
    ///
    /// Why the mutex is sufficient: the Solo profile runs exactly one relay
    /// process over the SQLite file, so holding an in-process mutex across the
    /// read-head + insert transaction serializes every append the way the
    /// Postgres arm's advisory lock does across processes. `BEGIN IMMEDIATE`
    /// additionally takes SQLite's write lock up front, so even a hypothetical
    /// second writer outside this process could not interleave between the
    /// head read and the insert — it would fail the unique `(community_id,
    /// seq)` key rather than corrupt the chain.
    async fn log_sqlite(
        &self,
        pool: &SqlitePool,
        append_lock: &tokio::sync::Mutex<()>,
        entry: NewAuditEntry,
    ) -> Result<AuditEntry, AuditError> {
        let _guard = append_lock.lock().await;

        let mut tx = pool.begin_with(SQLITE_BEGIN_IMMEDIATE).await?;

        // The stored row keys on the raw UUID (lowercase hyphenated TEXT on
        // sqlite); the typed `CommunityId` on the input is the provenance
        // fence, dereferenced here at the DB boundary.
        let community_id = *entry.community_id.as_uuid();
        let community_text = community_id.to_string();

        // Head of THIS community's chain — scoped by community_id.
        let head = sqlx::query(
            "SELECT seq, hash FROM audit_log
             WHERE community_id = ?1
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(&community_text)
        .fetch_optional(&mut *tx)
        .await?;

        let (prev_seq, prev_hash): (i64, Option<Vec<u8>>) = match head {
            Some(row) => (row.try_get("seq")?, Some(row.try_get("hash")?)),
            None => (0, None), // community's first entry
        };
        let seq = prev_seq + 1;

        // created_at is stored as INTEGER unix seconds (buzz-db sqlite
        // convention), so truncate to whole seconds BEFORE hashing — the
        // stored value must round-trip to exactly the value that was hashed,
        // or verification would recompute a different hash. The fallback
        // branch is unreachable for any real system clock (the current time
        // is always representable); it errors rather than silently hashing a
        // value that could not be stored faithfully.
        let now = Utc::now();
        let created_at = DateTime::<Utc>::from_timestamp(now.timestamp(), 0).ok_or_else(|| {
            AuditError::Database(sqlx::Error::Protocol(
                "system clock outside the representable unix-seconds range".into(),
            ))
        })?;

        let mut audit_entry = AuditEntry {
            community_id,
            seq,
            hash: Vec::new(),
            prev_hash,
            action: entry.action,
            actor_pubkey: entry.actor_pubkey,
            object_id: entry.object_id,
            detail: entry.detail,
            created_at,
        };

        audit_entry.hash = compute_hash(&audit_entry)?.to_vec();

        debug!(seq, "writing audit entry");

        sqlx::query(
            "INSERT INTO audit_log
                 (community_id, seq, hash, prev_hash, action, actor_pubkey, object_id, detail, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(&community_text)
        .bind(audit_entry.seq)
        .bind(&audit_entry.hash)
        .bind(audit_entry.prev_hash.as_deref())
        .bind(audit_entry.action.as_str())
        .bind(audit_entry.actor_pubkey.as_deref())
        .bind(audit_entry.object_id.as_deref())
        .bind(&audit_entry.detail)
        .bind(audit_entry.created_at.timestamp())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(audit_entry)
    }

    /// Fetch one community's chain segment `[from_seq, to_seq]`, ordered by
    /// sequence number, decoded into [`AuditEntry`] values.
    async fn fetch_range(
        &self,
        community: CommunityId,
        from_seq: i64,
        to_seq: i64,
    ) -> Result<Vec<AuditEntry>, AuditError> {
        match &self.backend {
            AuditBackend::Pg { pool } => {
                let rows = sqlx::query(
                    r#"
                    SELECT community_id, seq, hash, prev_hash, action, actor_pubkey,
                           object_id, detail, created_at
                    FROM audit_log
                    WHERE community_id = $1 AND seq BETWEEN $2 AND $3
                    ORDER BY seq ASC
                    "#,
                )
                .bind(community.as_uuid())
                .bind(from_seq)
                .bind(to_seq)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_audit_entry).collect()
            }
            AuditBackend::Sqlite { pool, .. } => {
                let rows = sqlx::query(
                    "SELECT community_id, seq, hash, prev_hash, action, actor_pubkey,
                            object_id, detail, created_at
                     FROM audit_log
                     WHERE community_id = ?1 AND seq BETWEEN ?2 AND ?3
                     ORDER BY seq ASC",
                )
                .bind(community.as_uuid().to_string())
                .bind(from_seq)
                .bind(to_seq)
                .fetch_all(pool)
                .await?;
                rows.iter().map(sqlite_row_to_audit_entry).collect()
            }
        }
    }

    /// Verify the hash chain for one community over `[from_seq, to_seq]`.
    ///
    /// Reads exactly that community's chain — it can never observe another
    /// community's entries or head. Returns `Ok(false)` if the range is empty,
    /// `Ok(true)` if the segment is internally consistent.
    #[instrument(skip(self))]
    pub async fn verify_chain(
        &self,
        community: CommunityId,
        from_seq: i64,
        to_seq: i64,
    ) -> Result<bool, AuditError> {
        let entries = self.fetch_range(community, from_seq, to_seq).await?;

        if entries.is_empty() {
            return Ok(false);
        }

        let mut expected_prev: Option<Vec<u8>> = None;

        for entry in entries {
            if let Some(ref expected) = expected_prev {
                // The previous entry's hash must equal this entry's prev_hash.
                if entry.prev_hash.as_deref() != Some(expected.as_slice()) {
                    return Err(AuditError::ChainViolation { seq: entry.seq });
                }
            }

            let computed = compute_hash(&entry)?;
            if computed.as_slice() != entry.hash.as_slice() {
                return Err(AuditError::HashMismatch { seq: entry.seq });
            }

            expected_prev = Some(entry.hash);
        }

        Ok(true)
    }

    /// Returns up to `limit` entries from one community's chain starting at
    /// `from_seq`, ordered by sequence number. Scoped to `community` — never
    /// returns another community's rows.
    #[instrument(skip(self))]
    pub async fn get_entries(
        &self,
        community: CommunityId,
        from_seq: i64,
        limit: i64,
    ) -> Result<Vec<AuditEntry>, AuditError> {
        match &self.backend {
            AuditBackend::Pg { pool } => {
                let rows = sqlx::query(
                    r#"
                    SELECT community_id, seq, hash, prev_hash, action, actor_pubkey,
                           object_id, detail, created_at
                    FROM audit_log
                    WHERE community_id = $1 AND seq >= $2
                    ORDER BY seq ASC
                    LIMIT $3
                    "#,
                )
                .bind(community.as_uuid())
                .bind(from_seq)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_audit_entry).collect()
            }
            AuditBackend::Sqlite { pool, .. } => {
                let rows = sqlx::query(
                    "SELECT community_id, seq, hash, prev_hash, action, actor_pubkey,
                            object_id, detail, created_at
                     FROM audit_log
                     WHERE community_id = ?1 AND seq >= ?2
                     ORDER BY seq ASC
                     LIMIT ?3",
                )
                .bind(community.as_uuid().to_string())
                .bind(from_seq)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.iter().map(sqlite_row_to_audit_entry).collect()
            }
        }
    }
}

fn row_to_audit_entry(row: &sqlx::postgres::PgRow) -> Result<AuditEntry, AuditError> {
    let action_str: String = row.get("action");
    let action: AuditAction = action_str.parse().map_err(|_| {
        warn!("unknown action in audit log");
        AuditError::UnknownAction
    })?;

    Ok(AuditEntry {
        community_id: row.get::<Uuid, _>("community_id"),
        seq: row.get("seq"),
        hash: row.get("hash"),
        prev_hash: row.get("prev_hash"),
        action,
        actor_pubkey: row.get("actor_pubkey"),
        object_id: row.get("object_id"),
        detail: row.get("detail"),
        created_at: row.get("created_at"),
    })
}

/// A stored-value decode failure on the sqlite arm, surfaced through the
/// existing [`AuditError::Database`] variant.
fn sqlite_decode_err(msg: String) -> AuditError {
    AuditError::Database(sqlx::Error::Decode(msg.into()))
}

/// Decode a sqlite `audit_log` row (buzz-db sqlite conventions: UUID as
/// lowercase hyphenated TEXT, timestamps as INTEGER unix seconds, bytes as
/// BLOB, JSON as TEXT) into an [`AuditEntry`].
fn sqlite_row_to_audit_entry(row: &sqlx::sqlite::SqliteRow) -> Result<AuditEntry, AuditError> {
    let action_str: String = row.try_get("action")?;
    let action: AuditAction = action_str.parse().map_err(|_| {
        warn!("unknown action in audit log");
        AuditError::UnknownAction
    })?;

    let community_text: String = row.try_get("community_id")?;
    let community_id = Uuid::parse_str(&community_text)
        .map_err(|e| sqlite_decode_err(format!("invalid community_id uuid text: {e}")))?;

    let created_secs: i64 = row.try_get("created_at")?;
    let created_at = DateTime::<Utc>::from_timestamp(created_secs, 0)
        .ok_or_else(|| sqlite_decode_err(format!("created_at out of range: {created_secs}")))?;

    Ok(AuditEntry {
        community_id,
        seq: row.try_get("seq")?,
        hash: row.try_get("hash")?,
        prev_hash: row.try_get("prev_hash")?,
        action,
        actor_pubkey: row.try_get("actor_pubkey")?,
        object_id: row.try_get("object_id")?,
        detail: row.try_get("detail")?,
        created_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::AuditAction;
    use crate::entry::NewAuditEntry;
    use std::sync::OnceLock;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    // The per-community advisory lock means different communities don't contend,
    // but tests share one table; serialize them so seq assertions are stable.
    static DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    fn db_lock() -> &'static Mutex<()> {
        DB_LOCK.get_or_init(|| Mutex::new(()))
    }

    async fn test_pool() -> Option<PgPool> {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://buzz:buzz_dev@localhost:5432/buzz".into());
        PgPool::connect(&url).await.ok()
    }

    /// A `community_id` known to exist in `communities` (FK target). Inserts a
    /// throwaway community row with a unique host and returns its id.
    async fn make_community(pool: &PgPool) -> Uuid {
        let id = Uuid::new_v4();
        let host = format!("test-{id}.example");
        sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
            .bind(id)
            .bind(host)
            .execute(pool)
            .await
            .expect("insert test community");
        id
    }

    fn new_entry(community_id: Uuid, action: AuditAction) -> NewAuditEntry {
        NewAuditEntry {
            community_id: CommunityId::from_uuid(community_id),
            action,
            actor_pubkey: Some(vec![0xab; 32]),
            object_id: Some(format!("obj_{}", Uuid::new_v4())),
            detail: serde_json::json!({"test": true}),
        }
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn community_chain_starts_at_seq_1_with_null_prev() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let c = make_community(&pool).await;

        let e = svc
            .log(new_entry(c, AuditAction::EventCreated))
            .await
            .unwrap();
        assert_eq!(e.seq, 1, "first entry in a community starts at seq 1");
        assert!(e.prev_hash.is_none(), "genesis entry has NULL prev_hash");
        assert_eq!(e.hash.len(), 32);
        assert_eq!(e.community_id, c);
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn chain_links_within_one_community() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let c = make_community(&pool).await;

        let e1 = svc
            .log(new_entry(c, AuditAction::EventCreated))
            .await
            .unwrap();
        let e2 = svc
            .log(new_entry(c, AuditAction::ChannelCreated))
            .await
            .unwrap();
        let e3 = svc
            .log(new_entry(c, AuditAction::MemberAdded))
            .await
            .unwrap();

        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert_eq!(e3.seq, 3);
        assert!(e1.prev_hash.is_none());
        assert_eq!(e2.prev_hash.as_deref(), Some(e1.hash.as_slice()));
        assert_eq!(e3.prev_hash.as_deref(), Some(e2.hash.as_slice()));
        assert!(svc
            .verify_chain(CommunityId::from_uuid(c), 1, 3)
            .await
            .unwrap());
    }

    /// THE isolation property: two communities keep independent chains. Each
    /// starts at seq 1; interleaving writes does not link them; verifying one
    /// never traverses the other.
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn chains_are_independent_per_community() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let a = make_community(&pool).await;
        let b = make_community(&pool).await;

        // Interleave A and B writes.
        let a1 = svc
            .log(new_entry(a, AuditAction::EventCreated))
            .await
            .unwrap();
        let b1 = svc
            .log(new_entry(b, AuditAction::EventCreated))
            .await
            .unwrap();
        let a2 = svc
            .log(new_entry(a, AuditAction::ChannelCreated))
            .await
            .unwrap();
        let b2 = svc
            .log(new_entry(b, AuditAction::ChannelCreated))
            .await
            .unwrap();

        // Each community's seq is independent and starts at 1.
        assert_eq!((a1.seq, a2.seq), (1, 2));
        assert_eq!((b1.seq, b2.seq), (1, 2));

        // A's chain links only within A; B's only within B. A2 must NOT chain to
        // B1 even though B1 was written between A1 and A2.
        assert_eq!(a2.prev_hash.as_deref(), Some(a1.hash.as_slice()));
        assert_eq!(b2.prev_hash.as_deref(), Some(b1.hash.as_slice()));
        assert_ne!(a2.prev_hash, b1.prev_hash);

        // Verifying A's chain traverses only A; same for B.
        assert!(svc
            .verify_chain(CommunityId::from_uuid(a), 1, 2)
            .await
            .unwrap());
        assert!(svc
            .verify_chain(CommunityId::from_uuid(b), 1, 2)
            .await
            .unwrap());

        // get_entries scoped to A returns only A's rows.
        let a_rows = svc
            .get_entries(CommunityId::from_uuid(a), 1, 100)
            .await
            .unwrap();
        assert!(
            a_rows.iter().all(|e| e.community_id == a),
            "A read leaked another community"
        );
        assert_eq!(a_rows.len(), 2);
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn verify_detects_tampering_within_a_community() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let c = make_community(&pool).await;

        svc.log(new_entry(c, AuditAction::EventCreated))
            .await
            .unwrap();
        let e2 = svc
            .log(new_entry(c, AuditAction::EventDeleted))
            .await
            .unwrap();
        svc.log(new_entry(c, AuditAction::ChannelDeleted))
            .await
            .unwrap();

        // Tamper with e2's stored actor_pubkey.
        let tampered: Vec<u8> = vec![0xff; 32];
        sqlx::query("UPDATE audit_log SET actor_pubkey = $1 WHERE community_id = $2 AND seq = $3")
            .bind(tampered)
            .bind(c)
            .bind(e2.seq)
            .execute(&pool)
            .await
            .unwrap();

        let r = svc.verify_chain(CommunityId::from_uuid(c), 1, 3).await;
        assert!(matches!(r, Err(AuditError::HashMismatch { seq }) if seq == e2.seq));
    }

    /// A row forged with another community's id cannot pass verification against
    /// the chain it was stamped for, because community_id is hashed in. (Models
    /// "a row can't be replayed across chains and still verify".)
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn cross_community_row_does_not_verify() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let a = make_community(&pool).await;
        let b = make_community(&pool).await;

        let a1 = svc
            .log(new_entry(a, AuditAction::EventCreated))
            .await
            .unwrap();

        // Forge: copy A's seq-1 row's hash into B's chain at seq 1.
        sqlx::query(
            "INSERT INTO audit_log (community_id, seq, hash, prev_hash, action, actor_pubkey, object_id, detail, created_at)
             VALUES ($1, 1, $2, NULL, $3, $4, $5, $6, NOW())",
        )
        .bind(b)
        .bind(&a1.hash) // A's hash, which was computed over community_id = A
        .bind(a1.action.as_str())
        .bind(a1.actor_pubkey.as_deref())
        .bind(a1.object_id.as_deref())
        .bind(&a1.detail)
        .execute(&pool)
        .await
        .unwrap();

        // Verifying B's chain recomputes the hash with community_id = B, which
        // won't match A's stored hash → HashMismatch. The forge is rejected.
        let r = svc.verify_chain(CommunityId::from_uuid(b), 1, 1).await;
        assert!(matches!(r, Err(AuditError::HashMismatch { seq: 1 })));
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn verify_empty_range_is_false() {
        let _g = db_lock().lock().await;
        let Some(pool) = test_pool().await else {
            return;
        };
        let svc = AuditService::new(pool.clone());
        let c = make_community(&pool).await;
        // No entries for this fresh community.
        assert!(!svc
            .verify_chain(CommunityId::from_uuid(c), 1, 100)
            .await
            .unwrap());
    }
}

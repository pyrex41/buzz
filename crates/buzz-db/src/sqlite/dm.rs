//! SQLite arms for direct-message channel persistence (WP5, Phase 2d).
//!
//! Port of the Solo-reachable operations in [`crate::dm`]: `open_dm`
//! (get-or-create by canonical participant set), `hide_dm` / `unhide_dm`
//! (per-member `hidden_at` visibility), and `list_hidden_dms` (NIP-DV
//! visibility snapshot input). `list_dms_for_user` has zero production call
//! sites (call-site inventory §2) and is deliberately not ported.
//!
//! Semantics match the Postgres arm:
//!
//! - The participant set is canonicalized in Rust
//!   ([`crate::dm::compute_participant_hash`]: sort + dedup + SHA-256), so the
//!   same set always maps to the same `participant_hash` and therefore the
//!   same channel — `open_dm` returns `(channel, was_created)` accordingly.
//! - The Postgres find-or-create ran inside a plain transaction, relying on
//!   the unique `(community_id, participant_hash)` index for race safety;
//!   here the create path uses `BEGIN IMMEDIATE` — SQLite's single writer
//!   serializes the re-check + insert, and the same unique index
//!   (`idx_channels_dm_hash`) backstops it.
//! - `hidden_at` is INTEGER unix seconds (`NOW()` → `unixepoch()`);
//!   membership upserts re-activate removed rows without touching
//!   `hidden_at`, exactly like the Postgres `ON CONFLICT … DO UPDATE`.
//!
//! One ordering divergence: `list_hidden_dms` orders by the TEXT channel id
//! (lexicographic on the lowercase hyphenated form) instead of Postgres UUID
//! byte order. The consumer (NIP-DV snapshot builder) only needs a
//! deterministic order, which both provide.

use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use super::channel::{begin_immediate, datetime_from_unix, opt_datetime_from_unix};
use super::event::{community_text, parse_uuid_text, uuid_text};
use crate::channel::ChannelRecord;
use crate::dm::compute_participant_hash;
use crate::error::{DbError, Result};
use buzz_core::CommunityId;

/// Channel column list for DM reads (TEXT enums read as-is, no casts).
const DM_CHANNEL_COLUMNS: &str = "id, name, channel_type, visibility, description, canvas, \
     created_by, created_at, updated_at, archived_at, deleted_at, \
     nip29_group_id, topic_required, max_members, \
     topic, topic_set_by, topic_set_at, \
     purpose, purpose_set_by, purpose_set_at, \
     ttl_seconds, ttl_deadline";

fn row_to_channel_record(row: sqlx::sqlite::SqliteRow) -> Result<ChannelRecord> {
    let id: String = row.try_get("id")?;
    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    Ok(ChannelRecord {
        id: parse_uuid_text(&id)?,
        name: row.try_get("name")?,
        channel_type: row.try_get("channel_type")?,
        visibility: row.try_get("visibility")?,
        description: row.try_get("description")?,
        canvas: row.try_get("canvas")?,
        created_by: row.try_get("created_by")?,
        created_at: datetime_from_unix(created_at)?,
        updated_at: datetime_from_unix(updated_at)?,
        archived_at: opt_datetime_from_unix(row.try_get("archived_at")?)?,
        deleted_at: opt_datetime_from_unix(row.try_get("deleted_at")?)?,
        nip29_group_id: row.try_get("nip29_group_id")?,
        topic_required: row.try_get("topic_required")?,
        max_members: row.try_get("max_members")?,
        topic: row.try_get("topic")?,
        topic_set_by: row.try_get("topic_set_by")?,
        topic_set_at: opt_datetime_from_unix(row.try_get("topic_set_at")?)?,
        purpose: row.try_get("purpose")?,
        purpose_set_by: row.try_get("purpose_set_by")?,
        purpose_set_at: opt_datetime_from_unix(row.try_get("purpose_set_at")?)?,
        ttl_seconds: row.try_get("ttl_seconds")?,
        ttl_deadline: opt_datetime_from_unix(row.try_get("ttl_deadline")?)?,
    })
}

/// Find a live DM channel by its canonical participant hash.
async fn find_dm_by_hash<'e, E>(
    executor: E,
    community_id: CommunityId,
    participant_hash: &[u8],
) -> Result<Option<ChannelRecord>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let sql = format!(
        "SELECT {DM_CHANNEL_COLUMNS} FROM channels \
         WHERE community_id = ?1 AND participant_hash = ?2 \
           AND channel_type = 'dm' AND deleted_at IS NULL \
         LIMIT 1"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community_id))
        .bind(participant_hash)
        .fetch_optional(executor)
        .await?;
    row.map(row_to_channel_record).transpose()
}

/// Create a new DM channel for `participants`, or return the existing one for
/// the same canonical set. Mirrors [`crate::dm::create_dm`]: 2–9 participants,
/// 32-byte pubkeys, all participants added as `member` rows (removed rows are
/// re-activated).
async fn create_dm(
    pool: &SqlitePool,
    community_id: CommunityId,
    participants: &[&[u8]],
    created_by: &[u8],
) -> Result<ChannelRecord> {
    if participants.len() < 2 {
        return Err(DbError::InvalidData(
            "DM requires at least 2 participants".to_string(),
        ));
    }
    if participants.len() > 9 {
        return Err(DbError::InvalidData(
            "DM supports at most 9 participants".to_string(),
        ));
    }
    for pk in participants {
        if pk.len() != 32 {
            return Err(DbError::InvalidData(format!(
                "pubkey must be 32 bytes, got {}",
                pk.len()
            )));
        }
    }

    let hash = compute_participant_hash(participants);

    let mut tx: Transaction<'_, Sqlite> = begin_immediate(pool).await?;

    // Idempotency re-check inside the write transaction.
    if let Some(existing) = find_dm_by_hash(tx.as_mut(), community_id, hash.as_slice()).await? {
        tx.commit().await?;
        return Ok(existing);
    }

    let name = if participants.len() == 2 {
        "DM".to_string()
    } else {
        format!("Group DM ({})", participants.len())
    };

    let id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO channels \
             (id, community_id, name, channel_type, visibility, created_by, participant_hash) \
         VALUES (?1, ?2, ?3, 'dm', 'private', ?4, ?5)",
    )
    .bind(uuid_text(id))
    .bind(community_text(community_id))
    .bind(&name)
    .bind(created_by)
    .bind(hash.as_slice())
    .execute(tx.as_mut())
    .await?;

    // Add all participants as members with role='member', re-activating any
    // previously removed rows (same ON CONFLICT shape as Postgres).
    for pk in participants {
        sqlx::query(
            "INSERT INTO channel_members (community_id, channel_id, pubkey, role, invited_by) \
             VALUES (?1, ?2, ?3, 'member', ?4) \
             ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE SET \
                 removed_at = NULL, \
                 removed_by = NULL, \
                 role = excluded.role",
        )
        .bind(community_text(community_id))
        .bind(uuid_text(id))
        .bind(*pk)
        .bind(created_by)
        .execute(tx.as_mut())
        .await?;
    }

    let sql =
        format!("SELECT {DM_CHANNEL_COLUMNS} FROM channels WHERE community_id = ?1 AND id = ?2");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community_id))
        .bind(uuid_text(id))
        .fetch_one(tx.as_mut())
        .await?;

    let record = row_to_channel_record(row)?;
    tx.commit().await?;
    Ok(record)
}

/// Open or retrieve a DM for the given set of participants.
///
/// `created_by` is merged into `pubkeys` if not already present, so the
/// caller is always a participant in their own DM. Returns
/// `(channel, was_created)`: `true` when a new DM was created, `false` when
/// an existing DM for the same canonical set was returned (in which case the
/// caller's `hidden_at` is cleared so the DM reappears in their sidebar).
pub(crate) async fn open_dm(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkeys: &[&[u8]],
    created_by: &[u8],
) -> Result<(ChannelRecord, bool)> {
    // Merge created_by into the participant set (dedup handled by
    // compute_participant_hash).
    let mut all: Vec<&[u8]> = pubkeys.to_vec();
    if !all.contains(&created_by) {
        all.push(created_by);
    }

    // Enforce max before hitting the DB.
    if all.len() > 9 {
        return Err(DbError::InvalidData(
            "DM supports at most 9 participants".to_string(),
        ));
    }

    let hash = compute_participant_hash(&all);

    // Fast path: existing DM, no write transaction.
    if let Some(existing) = find_dm_by_hash(pool, community_id, hash.as_slice()).await? {
        // Clear hidden_at for the caller so the DM reappears in their sidebar.
        unhide_dm(pool, community_id, existing.id, created_by).await?;
        return Ok((existing, false));
    }

    let channel = create_dm(pool, community_id, &all, created_by).await?;
    Ok((channel, true))
}

/// Hide a DM for a specific user by setting `hidden_at = unixepoch()`.
///
/// The DM is not deleted — re-opening a DM with the same participants clears
/// `hidden_at`. Returns an error if the user is not an active member.
pub(crate) async fn hide_dm(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
) -> Result<()> {
    let result = sqlx::query(
        "UPDATE channel_members SET hidden_at = unixepoch() \
         WHERE community_id = ?1 AND channel_id = ?2 AND pubkey = ?3 AND removed_at IS NULL",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(channel_id))
    .bind(pubkey)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(DbError::NotFound(format!(
            "no active membership for channel {channel_id}"
        )));
    }

    Ok(())
}

/// Unhide a DM for a specific user by clearing `hidden_at`.
///
/// Called automatically when a user re-opens a DM via [`open_dm`]. No-op if
/// the membership is not currently hidden.
pub(crate) async fn unhide_dm(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
) -> Result<()> {
    sqlx::query(
        "UPDATE channel_members SET hidden_at = NULL \
         WHERE community_id = ?1 AND channel_id = ?2 AND pubkey = ?3 AND removed_at IS NULL",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(channel_id))
    .bind(pubkey)
    .execute(pool)
    .await?;

    Ok(())
}

/// Return the channel IDs of all DMs the given user currently has hidden
/// (`hidden_at IS NOT NULL`) while still being an active member. Feeds the
/// relay-signed NIP-DV visibility snapshot.
pub(crate) async fn list_hidden_dms(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(
        "SELECT cm.channel_id \
         FROM channel_members cm \
         JOIN channels c \
           ON c.community_id = cm.community_id AND c.id = cm.channel_id \
         WHERE cm.community_id = ?1 \
           AND cm.pubkey = ?2 \
           AND cm.removed_at IS NULL \
           AND cm.hidden_at IS NOT NULL \
           AND c.channel_type = 'dm' \
           AND c.deleted_at IS NULL \
         ORDER BY cm.channel_id",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            let id: String = r.try_get("channel_id")?;
            parse_uuid_text(&id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (SqlitePool, CommunityId) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        let ensured = crate::sqlite::community::ensure_configured_community(&pool, "dm.example")
            .await
            .expect("ensure community");
        (pool, ensured.id)
    }

    fn pk(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    async fn dm_channel_count(pool: &SqlitePool, community: CommunityId) -> i64 {
        let (n,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM channels WHERE community_id = ?1 AND channel_type = 'dm'",
        )
        .bind(community_text(community))
        .fetch_one(pool)
        .await
        .expect("count dm channels");
        n
    }

    #[tokio::test]
    async fn open_dm_same_set_is_idempotent() {
        let (pool, community) = setup().await;
        let a = pk(1);
        let b = pk(2);

        let (first, created) = open_dm(&pool, community, &[&a, &b], &a)
            .await
            .expect("first open");
        assert!(created, "first open must create the DM");
        assert_eq!(first.channel_type, "dm");
        assert_eq!(first.visibility, "private");
        assert_eq!(first.name, "DM");

        // Same set again — including via the created_by merge (b opens a DM
        // "with a", which canonicalizes to the same {a, b} set).
        let (second, created) = open_dm(&pool, community, &[&a], &b)
            .await
            .expect("second open");
        assert!(!created, "same participant set must not create a new DM");
        assert_eq!(second.id, first.id, "same set must map to the same channel");

        assert_eq!(dm_channel_count(&pool, community).await, 1);
    }

    #[tokio::test]
    async fn open_dm_different_set_creates_new_channel() {
        let (pool, community) = setup().await;
        let a = pk(1);
        let b = pk(2);
        let c = pk(3);

        let (ab, created_ab) = open_dm(&pool, community, &[&b], &a).await.expect("open ab");
        assert!(created_ab);

        let (ac, created_ac) = open_dm(&pool, community, &[&c], &a).await.expect("open ac");
        assert!(
            created_ac,
            "a different participant set must create a new DM"
        );
        assert_ne!(ab.id, ac.id);

        // Group DM {a, b, c} is a third distinct channel with a group name.
        let (abc, created_abc) = open_dm(&pool, community, &[&b, &c], &a)
            .await
            .expect("open abc");
        assert!(created_abc);
        assert_ne!(abc.id, ab.id);
        assert_ne!(abc.id, ac.id);
        assert_eq!(abc.name, "Group DM (3)");

        assert_eq!(dm_channel_count(&pool, community).await, 3);
    }

    #[tokio::test]
    async fn open_dm_is_participant_order_independent() {
        let (pool, community) = setup().await;
        let a = pk(1);
        let b = pk(2);
        let c = pk(3);

        let (first, created) = open_dm(&pool, community, &[&a, &b, &c], &a)
            .await
            .expect("open in one order");
        assert!(created);

        let (second, created) = open_dm(&pool, community, &[&c, &b, &a], &b)
            .await
            .expect("open in reverse order");
        assert!(!created, "participant order must not matter");
        assert_eq!(second.id, first.id);

        assert_eq!(dm_channel_count(&pool, community).await, 1);
    }

    #[tokio::test]
    async fn open_dm_enforces_participant_bounds() {
        let (pool, community) = setup().await;
        let a = pk(1);

        // Solo set (created_by only) fails the >=2 check in create.
        let err = open_dm(&pool, community, &[&a], &a)
            .await
            .expect_err("self-only DM must be rejected");
        assert!(matches!(err, DbError::InvalidData(_)), "got {err:?}");

        // More than 9 distinct participants is rejected before any write.
        let many: Vec<Vec<u8>> = (1..=10).map(pk).collect();
        let refs: Vec<&[u8]> = many.iter().map(|p| p.as_slice()).collect();
        let err = open_dm(&pool, community, &refs, &many[0])
            .await
            .expect_err(">9 participants must be rejected");
        assert!(matches!(err, DbError::InvalidData(_)), "got {err:?}");
        assert_eq!(dm_channel_count(&pool, community).await, 0);
    }

    #[tokio::test]
    async fn hide_unhide_round_trip() {
        let (pool, community) = setup().await;
        let a = pk(1);
        let b = pk(2);

        let (dm, _) = open_dm(&pool, community, &[&b], &a).await.expect("open");

        // Hide for a only; b's view is unaffected.
        hide_dm(&pool, community, dm.id, &a).await.expect("hide");
        assert_eq!(
            list_hidden_dms(&pool, community, &a).await.expect("list a"),
            vec![dm.id]
        );
        assert!(list_hidden_dms(&pool, community, &b)
            .await
            .expect("list b")
            .is_empty());

        // Unhide restores visibility.
        unhide_dm(&pool, community, dm.id, &a)
            .await
            .expect("unhide");
        assert!(list_hidden_dms(&pool, community, &a)
            .await
            .expect("list after unhide")
            .is_empty());

        // Re-opening a hidden DM clears hidden_at for the opener.
        hide_dm(&pool, community, dm.id, &a).await.expect("re-hide");
        let (reopened, created) = open_dm(&pool, community, &[&b], &a).await.expect("reopen");
        assert!(!created);
        assert_eq!(reopened.id, dm.id);
        assert!(
            list_hidden_dms(&pool, community, &a)
                .await
                .expect("list after reopen")
                .is_empty(),
            "open_dm must clear the caller's hidden_at"
        );
    }

    #[tokio::test]
    async fn hide_dm_requires_active_membership() {
        let (pool, community) = setup().await;
        let a = pk(1);
        let b = pk(2);
        let outsider = pk(9);

        let (dm, _) = open_dm(&pool, community, &[&b], &a).await.expect("open");

        let err = hide_dm(&pool, community, dm.id, &outsider)
            .await
            .expect_err("non-member hide must fail");
        assert!(matches!(err, DbError::NotFound(_)), "got {err:?}");
    }
}

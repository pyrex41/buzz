//! SQLite arms for communities / tenancy (WP1 — communities).
//!
//! Ports of the inline `Db` community methods in `crates/buzz-db/src/lib.rs`.
//! Conventions (see `docs/phase2/sqlite-schema-notes.md` and
//! `super::event`): UUIDs are lowercase hyphenated TEXT, timestamps are
//! INTEGER unix seconds, and the `communities.host` column is
//! `COLLATE NOCASE` — so plain `host = ?` equality already gives the
//! case-insensitive matching Postgres got from `lower(host)` expression
//! indexes (hosts are stored pre-normalized ASCII).
//!
//! Where Postgres serialized writers with `pg_advisory_xact_lock`
//! (`create_community_with_owner`'s per-owner limit check), this arm uses a
//! plain `BEGIN IMMEDIATE` transaction — SQLite's single writer serializes
//! the count-then-insert cycle end-to-end.
#![allow(dead_code)] // Called via the Db backend-dispatch seam (orchestrator-owned lib.rs); allow until every arm is wired.

use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use buzz_core::CommunityId;

use crate::error::Result;
use crate::{
    ArchivedCommunityRecord, CommunityRecord, CreateCommunityWithOwnerResult,
    CreatedCommunityRecord, EnsuredCommunityRecord, OwnedCommunityRecord,
    UnarchivedCommunityRecord,
};

use super::event::{
    community_text, datetime_from_secs, parse_uuid_text, uuid_text, BEGIN_IMMEDIATE,
};

/// Decode an `(id, host)` row into a [`CommunityRecord`].
fn community_record(row: &sqlx::sqlite::SqliteRow) -> Result<CommunityRecord> {
    let id: String = row.try_get("id")?;
    let host: String = row.try_get("host")?;
    Ok(CommunityRecord {
        id: CommunityId::from_uuid(parse_uuid_text(&id)?),
        host,
    })
}

/// Returns the active (non-archived) community mapped to a normalized host.
///
/// Case-insensitive match via the NOCASE `host` column (parity with the
/// Postgres `lower(host)` functional index).
pub(crate) async fn lookup_community_by_host(
    pool: &SqlitePool,
    normalized_host: &str,
) -> Result<Option<CommunityRecord>> {
    let row =
        sqlx::query("SELECT id, host FROM communities WHERE host = ?1 AND archived_at IS NULL")
            .bind(normalized_host)
            .fetch_optional(pool)
            .await?;
    row.as_ref().map(community_record).transpose()
}

/// Returns whether a community id exists in the active lifecycle state.
pub(crate) async fn is_community_active(
    pool: &SqlitePool,
    community_id: CommunityId,
) -> Result<bool> {
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM communities WHERE id = ?1 AND archived_at IS NULL)",
    )
    .bind(community_text(community_id))
    .fetch_one(pool)
    .await?;
    Ok(active)
}

/// Returns a community by host regardless of lifecycle state. Operator-plane only.
pub(crate) async fn lookup_community_by_host_for_management(
    pool: &SqlitePool,
    normalized_host: &str,
) -> Result<Option<CommunityRecord>> {
    let row = sqlx::query("SELECT id, host FROM communities WHERE host = ?1")
        .bind(normalized_host)
        .fetch_optional(pool)
        .await?;
    row.as_ref().map(community_record).transpose()
}

/// Lists communities where `owner_pubkey` currently holds the `owner` role.
/// Operator-plane helper — callers gate on deployment-level operator auth.
pub(crate) async fn list_communities_owned_by(
    pool: &SqlitePool,
    owner_pubkey: &str,
) -> Result<Vec<OwnedCommunityRecord>> {
    let owner_pubkey = owner_pubkey.to_ascii_lowercase();
    let rows = sqlx::query(
        "SELECT c.id, c.host, c.created_at, c.archived_at \
         FROM communities c \
         JOIN relay_members rm ON rm.community_id = c.id \
         WHERE rm.pubkey = ?1 AND rm.role = 'owner' \
         ORDER BY c.created_at ASC, c.host ASC",
    )
    .bind(owner_pubkey)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id")?;
            let created_at: i64 = row.try_get("created_at")?;
            let archived_at: Option<i64> = row.try_get("archived_at")?;
            Ok(OwnedCommunityRecord {
                id: CommunityId::from_uuid(parse_uuid_text(&id)?),
                host: row.try_get("host")?,
                created_at: datetime_from_secs(created_at)?,
                archived_at: archived_at.map(datetime_from_secs).transpose()?,
            })
        })
        .collect()
}

/// Returns the normalized host for an active community id (reverse lookup).
pub(crate) async fn lookup_community_host(
    pool: &SqlitePool,
    community_id: CommunityId,
) -> Result<Option<String>> {
    let host: Option<String> =
        sqlx::query_scalar("SELECT host FROM communities WHERE id = ?1 AND archived_at IS NULL")
            .bind(community_text(community_id))
            .fetch_optional(pool)
            .await?;
    Ok(host)
}

/// Returns the community's workspace icon (NIP-11 `icon`), if set and non-empty.
pub(crate) async fn get_community_icon(
    pool: &SqlitePool,
    community_id: CommunityId,
) -> Result<Option<String>> {
    let icon: Option<Option<String>> =
        sqlx::query_scalar("SELECT icon FROM communities WHERE id = ?1")
            .bind(community_text(community_id))
            .fetch_optional(pool)
            .await?;
    Ok(icon.flatten().filter(|icon| !icon.is_empty()))
}

/// Sets or clears (`None`) the community's workspace icon.
pub(crate) async fn set_community_icon(
    pool: &SqlitePool,
    community_id: CommunityId,
    icon: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE communities SET icon = ?2 WHERE id = ?1")
        .bind(community_text(community_id))
        .bind(icon)
        .execute(pool)
        .await?;
    Ok(())
}

/// Ensure a configured community host exists and return its row (startup /
/// config seeding path for N=1 deployments).
///
/// The Postgres arm detected insert-vs-existing with the `xmax = 0` system
/// column; SQLite has no equivalent, so this uses
/// `INSERT … ON CONFLICT (host) DO NOTHING RETURNING` — a returned row means
/// `created = true`; an empty result means the host already existed and a
/// follow-up SELECT (NOCASE) fetches it with `created = false`.
pub(crate) async fn ensure_configured_community(
    pool: &SqlitePool,
    normalized_host: &str,
) -> Result<EnsuredCommunityRecord> {
    let inserted = sqlx::query(
        "INSERT INTO communities (host) VALUES (?1) \
         ON CONFLICT (host) DO NOTHING \
         RETURNING id, host",
    )
    .bind(normalized_host)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = inserted {
        let record = community_record(&row)?;
        return Ok(EnsuredCommunityRecord {
            id: record.id,
            host: record.host,
            created: true,
        });
    }

    let row = sqlx::query("SELECT id, host FROM communities WHERE host = ?1")
        .bind(normalized_host)
        .fetch_one(pool)
        .await?;
    let record = community_record(&row)?;
    Ok(EnsuredCommunityRecord {
        id: record.id,
        host: record.host,
        created: false,
    })
}

/// Atomically creates a community and its initial owner.
///
/// The Postgres arm held a per-owner advisory lock while enforcing the
/// ownership limit; here the `BEGIN IMMEDIATE` write lock serializes the
/// whole count-then-insert transaction (single writer), so concurrent
/// creates for the same owner cannot both pass the limit check. Identical
/// create retries return the original record; host collisions and limit
/// failures remain distinguishable.
pub(crate) async fn create_community_with_owner(
    pool: &SqlitePool,
    normalized_host: &str,
    owner_pubkey: &str,
) -> Result<CreateCommunityWithOwnerResult> {
    let owner_pubkey = owner_pubkey.to_ascii_lowercase();
    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;

    let row = sqlx::query(
        "INSERT INTO communities (host) VALUES (?1) \
         ON CONFLICT (host) DO NOTHING \
         RETURNING id, host",
    )
    .bind(normalized_host)
    .fetch_optional(&mut *tx)
    .await?;

    let (id, host) = if let Some(row) = row {
        let record = community_record(&row)?;

        // Enforce the per-owner community limit before inserting the owner row.
        let owned_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM relay_members WHERE pubkey = ?1 AND role = 'owner'",
        )
        .bind(&owner_pubkey)
        .fetch_one(&mut *tx)
        .await?;

        if owned_count >= crate::relay_members::MAX_COMMUNITIES_PER_OWNER {
            tx.rollback().await?;
            return Ok(CreateCommunityWithOwnerResult::LimitReached);
        }

        sqlx::query(
            "INSERT INTO relay_members (community_id, pubkey, role, added_by) \
             VALUES (?1, ?2, 'owner', NULL)",
        )
        .bind(community_text(record.id))
        .bind(&owner_pubkey)
        .execute(&mut *tx)
        .await?;
        (record.id, record.host)
    } else {
        // Host taken: an identical retried create (same owner) succeeds
        // idempotently; anything else is a host collision.
        let existing = sqlx::query(
            "SELECT c.id, c.host \
             FROM communities c \
             JOIN relay_members rm ON rm.community_id = c.id \
             WHERE c.host = ?1 \
               AND lower(rm.pubkey) = lower(?2) \
               AND rm.role = 'owner' \
               AND c.archived_at IS NULL",
        )
        .bind(normalized_host)
        .bind(&owner_pubkey)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(existing) = existing else {
            tx.rollback().await?;
            return Ok(CreateCommunityWithOwnerResult::HostExists);
        };
        let record = community_record(&existing)?;
        (record.id, record.host)
    };

    tx.commit().await?;
    Ok(CreateCommunityWithOwnerResult::Created(
        CreatedCommunityRecord { id, host },
    ))
}

/// Idempotently archives a community when the asserted pubkey is its current
/// owner. The protected deployment host can never be archived. The first
/// archive stamp is durable (`COALESCE(archived_at, unixepoch())`).
///
/// Postgres `UPDATE … FROM` join ports directly (SQLite ≥ 3.33).
pub(crate) async fn archive_community_owned_by(
    pool: &SqlitePool,
    normalized_host: &str,
    owner_pubkey: &str,
    protected_deployment_host: &str,
) -> Result<Option<ArchivedCommunityRecord>> {
    let row = sqlx::query(
        "UPDATE communities AS c \
         SET archived_at = COALESCE(c.archived_at, unixepoch()) \
         FROM relay_members AS rm \
         WHERE c.host = ?1 \
           AND rm.community_id = c.id \
           AND lower(rm.pubkey) = lower(?2) \
           AND rm.role = 'owner' \
           AND lower(c.host) <> lower(?3) \
         RETURNING id, host, archived_at",
    )
    .bind(normalized_host)
    .bind(owner_pubkey)
    .bind(protected_deployment_host)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        let id: String = row.try_get("id")?;
        let archived_at: i64 = row.try_get("archived_at")?;
        Ok(ArchivedCommunityRecord {
            id: CommunityId::from_uuid(parse_uuid_text(&id)?),
            host: row.try_get("host")?,
            archived_at: datetime_from_secs(archived_at)?,
        })
    })
    .transpose()
}

/// Idempotently restores a community when the asserted pubkey is its current owner.
pub(crate) async fn unarchive_community_owned_by(
    pool: &SqlitePool,
    normalized_host: &str,
    owner_pubkey: &str,
) -> Result<Option<UnarchivedCommunityRecord>> {
    let row = sqlx::query(
        "UPDATE communities AS c \
         SET archived_at = NULL \
         FROM relay_members AS rm \
         WHERE c.host = ?1 \
           AND rm.community_id = c.id \
           AND lower(rm.pubkey) = lower(?2) \
           AND rm.role = 'owner' \
         RETURNING id, host",
    )
    .bind(normalized_host)
    .bind(owner_pubkey)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        let id: String = row.try_get("id")?;
        Ok(UnarchivedCommunityRecord {
            id: CommunityId::from_uuid(parse_uuid_text(&id)?),
            host: row.try_get("host")?,
        })
    })
    .transpose()
}

/// Returns the community that owns a channel, if the channel exists
/// (soft-deleted channels excluded).
pub(crate) async fn community_of_channel(
    pool: &SqlitePool,
    channel_id: Uuid,
) -> Result<Option<CommunityId>> {
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT community_id FROM channels WHERE id = ?1 AND deleted_at IS NULL",
    )
    .bind(uuid_text(channel_id))
    .fetch_optional(pool)
    .await?;
    raw.map(|s| Ok(CommunityId::from_uuid(parse_uuid_text(&s)?)))
        .transpose()
}

/// Batched [`community_of_channel`]: map of channel id → owning community for
/// every channel that exists (soft-deletes excluded).
///
/// Contract pinned by the Postgres tests: a channel missing from the map is a
/// coverage breach for the caller — never defaulted.
pub(crate) async fn communities_of_channels(
    pool: &SqlitePool,
    channel_ids: &[Uuid],
) -> Result<std::collections::HashMap<Uuid, CommunityId>> {
    if channel_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT id, community_id FROM channels WHERE deleted_at IS NULL AND id IN (",
    );
    let mut sep = qb.separated(", ");
    for id in channel_ids {
        sep.push_bind(uuid_text(*id));
    }
    qb.push(")");

    let rows = qb.build().fetch_all(pool).await?;
    let mut out = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let ch: String = row.try_get("id")?;
        let cm: String = row.try_get("community_id")?;
        out.insert(
            parse_uuid_text(&ch)?,
            CommunityId::from_uuid(parse_uuid_text(&cm)?),
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn add_owner(pool: &SqlitePool, community: CommunityId, pubkey_hex: &str) {
        sqlx::query(
            "INSERT INTO relay_members (community_id, pubkey, role) VALUES (?1, ?2, 'owner')",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey_hex)
        .execute(pool)
        .await
        .expect("insert owner");
    }

    #[tokio::test]
    async fn host_lookup_is_nocase_and_respects_archival() {
        let pool = setup_pool().await;
        let ensured = ensure_configured_community(&pool, "team.example.com")
            .await
            .expect("ensure");
        assert!(ensured.created);

        // NOCASE: different casing finds the same row.
        let hit = lookup_community_by_host(&pool, "TEAM.Example.COM")
            .await
            .expect("lookup")
            .expect("found");
        assert_eq!(hit.id, ensured.id);
        assert_eq!(hit.host, "team.example.com");

        // Reverse lookup + activity.
        assert_eq!(
            lookup_community_host(&pool, ensured.id)
                .await
                .expect("reverse"),
            Some("team.example.com".to_string())
        );
        assert!(is_community_active(&pool, ensured.id)
            .await
            .expect("active"));

        // Archive: data-plane lookup goes dark, management lookup still sees it.
        sqlx::query("UPDATE communities SET archived_at = unixepoch() WHERE id = ?1")
            .bind(ensured.id.as_uuid().to_string())
            .execute(&pool)
            .await
            .expect("archive");
        assert!(lookup_community_by_host(&pool, "team.example.com")
            .await
            .expect("lookup archived")
            .is_none());
        assert!(!is_community_active(&pool, ensured.id)
            .await
            .expect("inactive"));
        assert!(
            lookup_community_by_host_for_management(&pool, "Team.Example.Com")
                .await
                .expect("management lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn ensure_configured_community_is_idempotent_and_nocase() {
        let pool = setup_pool().await;
        let first = ensure_configured_community(&pool, "solo.example")
            .await
            .expect("first ensure");
        assert!(first.created);

        let second = ensure_configured_community(&pool, "solo.example")
            .await
            .expect("second ensure");
        assert!(!second.created);
        assert_eq!(second.id, first.id);
        assert_eq!(second.host, "solo.example");

        // NOCASE conflict target: different casing maps to the existing row.
        let third = ensure_configured_community(&pool, "SOLO.example")
            .await
            .expect("third ensure");
        assert!(!third.created);
        assert_eq!(third.id, first.id);

        // Verification item from the schema notes: the generated id must be
        // stored as lowercase hyphenated TEXT so the key format never
        // silently bifurcates.
        let (type_of, id_text): (String, String) =
            sqlx::query_as("SELECT typeof(id), id FROM communities WHERE host = ?1")
                .bind("solo.example")
                .fetch_one(&pool)
                .await
                .expect("id form");
        assert_eq!(type_of, "text");
        assert_eq!(id_text, id_text.to_lowercase());
        assert_eq!(id_text.len(), 36, "hyphenated uuid form: {id_text}");
        assert_eq!(
            parse_uuid_text(&id_text).expect("parse").to_string(),
            id_text
        );
    }

    #[tokio::test]
    async fn create_community_with_owner_limits_and_collisions() {
        let pool = setup_pool().await;
        let owner = "cd".repeat(32);
        let other = "ef".repeat(32);

        let created = create_community_with_owner(&pool, "one.example", &owner)
            .await
            .expect("create one");
        let CreateCommunityWithOwnerResult::Created(record) = created else {
            panic!("expected Created");
        };
        assert_eq!(record.host, "one.example");

        // Owner row landed.
        let owned = list_communities_owned_by(&pool, &owner)
            .await
            .expect("owned");
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].id, record.id);
        assert!(owned[0].archived_at.is_none());

        // Identical retried create returns the original record.
        let retry = create_community_with_owner(&pool, "one.example", &owner)
            .await
            .expect("retry");
        assert_eq!(
            retry,
            CreateCommunityWithOwnerResult::Created(CreatedCommunityRecord {
                id: record.id,
                host: record.host.clone(),
            })
        );

        // Someone else's host is a collision.
        assert_eq!(
            create_community_with_owner(&pool, "one.example", &other)
                .await
                .expect("collision"),
            CreateCommunityWithOwnerResult::HostExists
        );

        // Per-owner limit.
        for i in 2..=crate::relay_members::MAX_COMMUNITIES_PER_OWNER {
            let r = create_community_with_owner(&pool, &format!("n{i}.example"), &owner)
                .await
                .expect("create up to limit");
            assert!(matches!(r, CreateCommunityWithOwnerResult::Created(_)));
        }
        assert_eq!(
            create_community_with_owner(&pool, "over.example", &owner)
                .await
                .expect("over limit"),
            CreateCommunityWithOwnerResult::LimitReached
        );
        // The over-limit host must not have been leaked into the registry.
        assert!(
            lookup_community_by_host_for_management(&pool, "over.example")
                .await
                .expect("over lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn archive_and_unarchive_owned_by() {
        let pool = setup_pool().await;
        let owner = "12".repeat(32);
        let ensured = ensure_configured_community(&pool, "arch.example")
            .await
            .expect("ensure");
        add_owner(&pool, ensured.id, &owner).await;

        // Protected deployment host cannot be archived.
        assert!(
            archive_community_owned_by(&pool, "arch.example", &owner, "ARCH.example")
                .await
                .expect("protected archive")
                .is_none()
        );
        // Non-owner cannot archive.
        assert!(archive_community_owned_by(
            &pool,
            "arch.example",
            &"34".repeat(32),
            "deploy.example"
        )
        .await
        .expect("non-owner archive")
        .is_none());

        let archived = archive_community_owned_by(&pool, "arch.example", &owner, "deploy.example")
            .await
            .expect("archive")
            .expect("archived");
        assert_eq!(archived.id, ensured.id);

        // Idempotent: re-archiving keeps the first durable stamp.
        let again = archive_community_owned_by(&pool, "arch.example", &owner, "deploy.example")
            .await
            .expect("re-archive")
            .expect("still archived");
        assert_eq!(again.archived_at, archived.archived_at);

        let restored = unarchive_community_owned_by(&pool, "arch.example", &owner)
            .await
            .expect("unarchive")
            .expect("restored");
        assert_eq!(restored.id, ensured.id);
        assert!(is_community_active(&pool, ensured.id)
            .await
            .expect("active again"));
    }

    #[tokio::test]
    async fn icon_get_set_roundtrip() {
        let pool = setup_pool().await;
        let ensured = ensure_configured_community(&pool, "icon.example")
            .await
            .expect("ensure");

        assert!(get_community_icon(&pool, ensured.id)
            .await
            .expect("no icon")
            .is_none());
        set_community_icon(&pool, ensured.id, Some("data:image/png;base64,AA=="))
            .await
            .expect("set icon");
        assert_eq!(
            get_community_icon(&pool, ensured.id).await.expect("icon"),
            Some("data:image/png;base64,AA==".to_string())
        );
        // Empty string filters to None (parity with the Postgres arm).
        set_community_icon(&pool, ensured.id, Some(""))
            .await
            .expect("set empty");
        assert!(get_community_icon(&pool, ensured.id)
            .await
            .expect("empty icon")
            .is_none());
        set_community_icon(&pool, ensured.id, None)
            .await
            .expect("clear");
        assert!(get_community_icon(&pool, ensured.id)
            .await
            .expect("cleared icon")
            .is_none());
    }

    #[tokio::test]
    async fn communities_of_channels_omits_missing_and_deleted() {
        let pool = setup_pool().await;
        let ensured = ensure_configured_community(&pool, "chmap.example")
            .await
            .expect("ensure");
        let live = Uuid::new_v4();
        let deleted = Uuid::new_v4();
        for (id, deleted_at) in [(live, None::<i64>), (deleted, Some(1))] {
            sqlx::query(
                "INSERT INTO channels (id, community_id, name, created_by, deleted_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(id.to_string())
            .bind(ensured.id.as_uuid().to_string())
            .bind(format!("c-{}", id.simple()))
            .bind(vec![7u8; 32])
            .bind(deleted_at)
            .execute(&pool)
            .await
            .expect("insert channel");
        }
        let missing = Uuid::new_v4();

        let map = communities_of_channels(&pool, &[live, deleted, missing])
            .await
            .expect("map");
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&live), Some(&ensured.id));
        assert!(!map.contains_key(&deleted));
        assert!(!map.contains_key(&missing));

        assert_eq!(
            community_of_channel(&pool, live).await.expect("of channel"),
            Some(ensured.id)
        );
        assert_eq!(
            community_of_channel(&pool, deleted)
                .await
                .expect("of deleted channel"),
            None
        );
        assert!(communities_of_channels(&pool, &[])
            .await
            .expect("empty input")
            .is_empty());
    }
}

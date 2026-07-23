//! SQLite arms for community-scoped archived identities (WP8 — NIP-IA).
//!
//! Straight port of `crate::archived_identities`: an archive is a
//! community-local UI visibility hint keyed `(community_id, pubkey)` — not a
//! ban. All identity columns are lowercase hex TEXT, preserved verbatim;
//! `archived_at` becomes INTEGER unix seconds.

// TODO(dispatch): remove once the `Db` facade (lib.rs) wires these arms —
// until then nothing outside this module calls them.
#![allow(dead_code)]

use sqlx::{Row as _, SqlitePool};

use buzz_core::CommunityId;

use crate::archived_identities::ArchivedIdentity;
use crate::error::Result;

use super::event::{community_text, datetime_from_secs};

/// Returns `true` if `pubkey` (64-char hex) is archived in `community_id`.
pub(crate) async fn is_archived(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &str,
) -> Result<bool> {
    let archived: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM archived_identities \
         WHERE community_id = ?1 AND pubkey = ?2)",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .fetch_one(pool)
    .await?;
    Ok(archived)
}

/// Archives an identity in `community_id`.
///
/// Returns `true` if the row was inserted, `false` if the identity was
/// already archived in that community. Re-archiving is idempotent and does
/// not mutate the existing row (`ON CONFLICT DO NOTHING`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn archive(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &str,
    consent_path: &str,
    actor: &str,
    reason: Option<&str>,
    replaced_by: Option<&str>,
    request_event_id: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO archived_identities \
         (community_id, pubkey, consent_path, actor, reason, replaced_by, request_event_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT (community_id, pubkey) DO NOTHING",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .bind(consent_path)
    .bind(actor)
    .bind(reason)
    .bind(replaced_by)
    .bind(request_event_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Unarchives an identity from `community_id`.
///
/// Returns `true` if a row was deleted, `false` if the identity was not
/// archived in that community.
pub(crate) async fn unarchive(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &str,
) -> Result<bool> {
    let result =
        sqlx::query("DELETE FROM archived_identities WHERE community_id = ?1 AND pubkey = ?2")
            .bind(community_text(community_id))
            .bind(pubkey)
            .execute(pool)
            .await?;

    Ok(result.rows_affected() > 0)
}

/// Returns all identities archived in `community_id`, ordered by archive
/// time ascending.
pub(crate) async fn list_archived(
    pool: &SqlitePool,
    community_id: CommunityId,
) -> Result<Vec<ArchivedIdentity>> {
    let rows = sqlx::query(
        "SELECT pubkey, consent_path, actor, reason, replaced_by, request_event_id, archived_at \
         FROM archived_identities WHERE community_id = ?1 ORDER BY archived_at ASC",
    )
    .bind(community_text(community_id))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let archived_at: i64 = row.try_get("archived_at")?;
            Ok(ArchivedIdentity {
                pubkey: row.try_get("pubkey")?,
                consent_path: row.try_get("consent_path")?,
                actor: row.try_get("actor")?,
                reason: row.try_get("reason")?,
                replaced_by: row.try_get("replaced_by")?,
                request_event_id: row.try_get("request_event_id")?,
                archived_at: datetime_from_secs(archived_at)?,
            })
        })
        .collect()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr as _;
    use uuid::Uuid;

    async fn test_pool() -> SqlitePool {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .expect("options")
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn make_community(pool: &SqlitePool) -> CommunityId {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.to_string())
            .bind(format!("archive-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    #[tokio::test]
    async fn archived_identity_state_is_community_scoped() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let pubkey = "aa".repeat(32);
        let actor = "bb".repeat(32);
        let event_a = "cc".repeat(32);
        let event_b = "dd".repeat(32);

        assert!(archive(
            &pool,
            community_a,
            &pubkey,
            "self",
            &actor,
            Some("community A"),
            None,
            &event_a,
        )
        .await
        .expect("archive in community A"));
        // Re-archiving is idempotent and does not mutate the row.
        assert!(!archive(
            &pool,
            community_a,
            &pubkey,
            "owner",
            &actor,
            Some("changed reason"),
            None,
            &event_b,
        )
        .await
        .expect("re-archive in community A"));

        assert!(is_archived(&pool, community_a, &pubkey)
            .await
            .expect("is_archived in A"));
        assert!(!is_archived(&pool, community_b, &pubkey)
            .await
            .expect("is_archived in B"));

        let listed = list_archived(&pool, community_a).await.expect("list A");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].pubkey, pubkey);
        assert_eq!(listed[0].consent_path, "self", "first archive row kept");
        assert_eq!(listed[0].reason.as_deref(), Some("community A"));
        assert_eq!(listed[0].request_event_id, event_a);
        assert!(list_archived(&pool, community_b)
            .await
            .expect("list B")
            .is_empty());

        // Unarchiving in the wrong community is a no-op.
        assert!(!unarchive(&pool, community_b, &pubkey)
            .await
            .expect("unarchive absent B"));
        assert!(is_archived(&pool, community_a, &pubkey)
            .await
            .expect("B unarchive must not affect A"));

        // The same pubkey may be independently archived in community B.
        assert!(archive(
            &pool,
            community_b,
            &pubkey,
            "self",
            &actor,
            Some("community B"),
            None,
            &event_b,
        )
        .await
        .expect("archive same pubkey in community B"));
        assert!(unarchive(&pool, community_a, &pubkey)
            .await
            .expect("unarchive A"));
        assert!(!is_archived(&pool, community_a, &pubkey)
            .await
            .expect("A removed"));
        assert!(is_archived(&pool, community_b, &pubkey)
            .await
            .expect("A unarchive must not affect B"));
    }
}

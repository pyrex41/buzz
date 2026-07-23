//! SQLite arms for deployment-level product feedback (WP8).
//!
//! Straight port of `crate::product_feedback`, exposed under the `Db`-facade
//! names (`insert_product_feedback` / `list_product_feedback`). The table is
//! deliberately **operator-global** (registered in `_operator_global_tables`):
//! feedback retains its source community as provenance only, is idempotent
//! deployment-wide by signed event id, and the list runs across all
//! communities. `tags` is JSON TEXT; timestamps are INTEGER unix seconds.

// TODO(dispatch): remove once the `Db` facade (lib.rs) wires these arms —
// until then nothing outside this module calls them.
#![allow(dead_code)]

use sqlx::{Row as _, SqlitePool};
use uuid::Uuid;

use crate::error::Result;
use crate::product_feedback::{NewProductFeedback, ProductFeedbackRecord};
use crate::CommunityId;

use super::event::{community_text, datetime_from_secs, parse_uuid_text};

/// Insert product feedback, idempotent deployment-wide by signed event id.
///
/// The first accepted submission owns the provenance row; replaying the same
/// signed event through another community returns the same row id without
/// changing its source community (the no-op `DO UPDATE SET event_id =
/// excluded.event_id` exists purely so `RETURNING id` yields the existing
/// row on conflict, exactly like the Postgres arm).
pub(crate) async fn insert_product_feedback(
    pool: &SqlitePool,
    community: CommunityId,
    feedback: NewProductFeedback<'_>,
) -> Result<Uuid> {
    let tags_text = serde_json::to_string(feedback.tags)?;
    let row = sqlx::query(
        "INSERT INTO product_feedback \
             (community_id, event_id, submitter_pubkey, category, body, tags, event_created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT (event_id) DO UPDATE SET event_id = excluded.event_id \
         RETURNING id",
    )
    .bind(community_text(community))
    .bind(feedback.event_id)
    .bind(feedback.submitter_pubkey)
    .bind(feedback.category)
    .bind(feedback.body)
    .bind(tags_text)
    .bind(feedback.event_created_at.timestamp())
    .fetch_one(pool)
    .await?;

    let id: String = row.try_get("id")?;
    parse_uuid_text(&id)
}

/// List feedback across all communities, newest received first (bounded by
/// `limit`). Deployment-operator tooling only.
pub(crate) async fn list_product_feedback(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<ProductFeedbackRecord>> {
    let rows = sqlx::query(
        "SELECT id, community_id, event_id, submitter_pubkey, category, body, tags, \
                event_created_at, received_at \
         FROM product_feedback \
         ORDER BY received_at DESC, id \
         LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id")?;
            let community_id: String = row.try_get("community_id")?;
            let tags_text: String = row.try_get("tags")?;
            let event_created_at: i64 = row.try_get("event_created_at")?;
            let received_at: i64 = row.try_get("received_at")?;
            Ok(ProductFeedbackRecord {
                id: parse_uuid_text(&id)?,
                community_id: parse_uuid_text(&community_id)?,
                event_id: hex::encode(row.try_get::<Vec<u8>, _>("event_id")?),
                submitter_pubkey: hex::encode(row.try_get::<Vec<u8>, _>("submitter_pubkey")?),
                category: row.try_get("category")?,
                body: row.try_get("body")?,
                tags: serde_json::from_str(&tags_text)?,
                event_created_at: datetime_from_secs(event_created_at)?,
                received_at: datetime_from_secs(received_at)?,
            })
        })
        .collect()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone as _, Utc};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr as _;

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
            .bind(format!("feedback-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    fn event_id_bytes(fill: u8) -> [u8; 32] {
        [fill; 32]
    }

    #[tokio::test]
    async fn duplicate_event_keeps_first_community_provenance() {
        let pool = test_pool().await;
        let first = make_community(&pool).await;
        let second = make_community(&pool).await;

        let event_id = event_id_bytes(0x5A);
        let pubkey = [7u8; 32];
        let tags = serde_json::json!([["category", "bug"]]);
        let event_created_at = Utc
            .with_ymd_and_hms(2026, 7, 1, 10, 30, 0)
            .single()
            .expect("ts");
        let feedback = || NewProductFeedback {
            event_id: &event_id,
            submitter_pubkey: &pubkey,
            category: Some("bug"),
            body: "same signed feedback",
            tags: &tags,
            event_created_at,
        };

        let first_row = insert_product_feedback(&pool, first, feedback())
            .await
            .expect("first insert");
        let duplicate_row = insert_product_feedback(&pool, second, feedback())
            .await
            .expect("duplicate insert");
        assert_eq!(duplicate_row, first_row, "replay returns the same row id");

        let listed = list_product_feedback(&pool, 10).await.expect("list");
        assert_eq!(listed.len(), 1, "one provenance row deployment-wide");
        let record = &listed[0];
        assert_eq!(record.id, first_row);
        assert_eq!(
            record.community_id,
            *first.as_uuid(),
            "first accepted submission owns provenance"
        );
        assert_eq!(record.event_id, hex::encode(event_id));
        assert_eq!(record.submitter_pubkey, hex::encode(pubkey));
        assert_eq!(record.category.as_deref(), Some("bug"));
        assert_eq!(record.body, "same signed feedback");
        assert_eq!(record.tags, tags);
        assert_eq!(record.event_created_at, event_created_at);
    }

    #[tokio::test]
    async fn list_is_bounded_and_newest_first() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let tags = serde_json::json!([]);

        for i in 0..3u8 {
            let event_id = event_id_bytes(i + 1);
            let pubkey = [9u8; 32];
            // Distinct received_at instants so the DESC ordering is
            // deterministic (received_at defaults to unixepoch()).
            sqlx::query(
                "INSERT INTO product_feedback \
                 (community_id, event_id, submitter_pubkey, body, tags, event_created_at, \
                  received_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .bind(community_text(community))
            .bind(event_id.as_slice())
            .bind(pubkey.as_slice())
            .bind(format!("feedback {i}"))
            .bind(tags.to_string())
            .bind(1_700_000_000i64)
            .bind(1_700_000_000i64 + i64::from(i))
            .execute(&pool)
            .await
            .expect("seed feedback row");
        }

        let listed = list_product_feedback(&pool, 2).await.expect("list");
        assert_eq!(listed.len(), 2, "limit is applied");
        assert_eq!(listed[0].body, "feedback 2", "newest received first");
        assert_eq!(listed[1].body, "feedback 1");
    }
}

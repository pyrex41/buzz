//! SQLite backend for the single-node Solo profile (Hive plan Phase 2).
//!
//! Modules here mirror the Postgres submodules function-for-function as
//! their SQLite arms land (`Db` methods dispatch on `DbBackend`). Schema
//! lives in `schema.sql` (consolidated — no incremental migration lineage
//! yet; per ADR 0001 the Postgres migrations are not shared).

pub mod admin_moderation;
pub mod archived_identities;
pub mod channel;
pub mod community;
pub mod dm;
pub mod event;
pub mod feed;
pub mod git_repo;
pub mod moderation;
pub mod product_feedback;
pub mod reaction;
pub mod relay_members;
pub mod thread;
pub mod user;
pub mod workflow;

use sqlx::SqlitePool;

use crate::error::Result;

/// Embedded consolidated schema, applied idempotently at startup.
const SCHEMA: &str = include_str!("schema.sql");

/// Bring the SQLite schema to the current version.
///
/// Version-gated: the consolidated schema applies exactly once, recorded in
/// `schema_migrations`. Executed as one multi-statement script inside a
/// transaction — trigger bodies contain `;`, so the script is never split.
pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (\
             version INTEGER PRIMARY KEY,\
             applied_at INTEGER NOT NULL\
         )",
    )
    .execute(pool)
    .await?;
    let applied: Option<(i64,)> =
        sqlx::query_as("SELECT version FROM schema_migrations WHERE version = 1")
            .fetch_optional(pool)
            .await?;
    if applied.is_some() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(SCHEMA).execute(tx.as_mut()).await?;
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (1, unixepoch())")
        .execute(tx.as_mut())
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded schema must execute end-to-end through sqlx's bundled
    /// libsqlite3 (FTS5 virtual table + triggers included), and re-running
    /// it must be a no-op (idempotence).
    #[tokio::test]
    async fn schema_applies_and_is_idempotent() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect");
        run_migrations(&pool).await.expect("first apply");
        run_migrations(&pool)
            .await
            .expect("second apply (idempotent)");

        let (n,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM sqlite_master WHERE type = 'table'")
                .fetch_one(&pool)
                .await
                .expect("count tables");
        assert!(
            n >= 20,
            "expected the full serve-path schema, got {n} tables"
        );

        let (fts,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM sqlite_master WHERE name = 'events_fts'")
                .fetch_one(&pool)
                .await
                .expect("fts table present");
        assert_eq!(fts, 1, "events_fts virtual table must exist");
    }
}

#[cfg(test)]
mod db_surface_tests {
    //! End-to-end serve-path flow through the PUBLIC `Db` surface on the
    //! SQLite backend — proves the dispatch seam, not just the arms.

    use nostr::{EventBuilder, JsonUtil, Keys, Kind};

    use crate::{Db, EventQuery};
    use buzz_core::channel::{ChannelType, ChannelVisibility, MemberRole};

    fn temp_db_path() -> String {
        std::env::temp_dir()
            .join(format!("buzz-sqlite-e2e-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned()
    }

    #[tokio::test]
    async fn solo_serve_path_flow_via_public_db_surface() {
        let path = temp_db_path();
        let db = Db::new_sqlite(&path).await.expect("open sqlite db");
        db.migrate().await.expect("migrate");

        // Tenant bootstrap: host → community, idempotent.
        let ensured = db
            .ensure_configured_community("solo.example")
            .await
            .expect("ensure community");
        assert!(ensured.created, "first ensure must create the community");
        let again = db
            .ensure_configured_community("solo.example")
            .await
            .expect("ensure community again");
        assert!(!again.created, "second ensure must be idempotent");
        assert_eq!(ensured.id, again.id);
        let community = ensured.id;

        // Channel + membership.
        let keys = Keys::generate();
        let author = keys.public_key().to_bytes().to_vec();
        let channel = db
            .create_channel(
                community,
                "general",
                ChannelType::Stream,
                ChannelVisibility::Open,
                Some("solo smoke channel"),
                &author,
                None,
            )
            .await
            .expect("create channel");
        // The creator is auto-added with an elevated role by create_channel;
        // a second member joins at the base role (elevated grants require an
        // elevated inviter — guard ported faithfully from Postgres).
        let member = Keys::generate().public_key().to_bytes().to_vec();
        db.add_member(
            community,
            channel.id,
            &member,
            MemberRole::Member,
            Some(&author),
        )
        .await
        .expect("add member");
        for pubkey in [&author, &member] {
            assert!(db
                .is_member(community, channel.id, pubkey)
                .await
                .expect("is_member"));
        }

        // Event insert + channel-scoped query round trip.
        let event = EventBuilder::new(Kind::Custom(9), "hello from sqlite")
            .tags([nostr::Tag::parse(["h", &channel.id.to_string()]).expect("h tag")])
            .sign_with_keys(&keys)
            .expect("sign");
        let (stored, inserted) = db
            .insert_event(community, &event, Some(channel.id))
            .await
            .expect("insert event");
        assert!(inserted, "fresh event must insert");
        assert_eq!(stored.event.id, event.id);

        let mut q = EventQuery::for_community(community);
        q.channel_id = Some(channel.id);
        q.kinds = Some(vec![9]);
        let results = db.query_events(&q).await.expect("query events");
        assert_eq!(results.len(), 1, "channel-scoped query must find the event");
        assert_eq!(results[0].event.as_json(), event.as_json());

        // Duplicate insert is idempotent; count agrees.
        let (_, second) = db
            .insert_event(community, &event, Some(channel.id))
            .await
            .expect("re-insert event");
        assert!(!second, "duplicate insert must be a no-op");
        assert_eq!(db.count_events(&q).await.expect("count"), 1);

        // A pg-only method fails closed with the typed error, not a panic.
        let err = db
            .ensure_future_partitions(1)
            .await
            .expect_err("pg-only method must refuse on sqlite");
        assert!(
            matches!(err, crate::DbError::UnsupportedBackend(_)),
            "expected UnsupportedBackend, got {err:?}"
        );

        let _ = std::fs::remove_file(&path);
    }
}

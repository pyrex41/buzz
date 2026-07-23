//! SQLite backend for the single-node Solo profile (Hive plan Phase 2).
//!
//! Modules here mirror the Postgres submodules function-for-function as
//! their SQLite arms land (`Db` methods dispatch on `DbBackend`). Schema
//! lives in `schema.sql` (consolidated — no incremental migration lineage
//! yet; per ADR 0001 the Postgres migrations are not shared).

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

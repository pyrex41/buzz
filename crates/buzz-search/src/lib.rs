#![deny(unsafe_code)]
#![warn(missing_docs)]
//! Buzz search — community-scoped full-text search over Buzz events.
//!
//! Two backends, one contract:
//!
//! - **Postgres** (multi-node): the index lives in the `events` table:
//!   `search_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',
//!   content)) STORED`, with `GIN (search_tsv)` as the access path. Because
//!   the column is `GENERATED ALWAYS`, every row write *is* the index update
//!   — there is no separate indexer, no mpsc queue, no reindex job, no
//!   consistency window to reason about. A client cannot forge the tsvector
//!   out of sync with the content it signed.
//! - **SQLite** (Solo profile): the index is the `events_fts` FTS5
//!   external-content table, maintained by `AFTER INSERT/DELETE/UPDATE`
//!   triggers on `events` with a positive kind allowlist (see
//!   `buzz-db/src/sqlite/schema.sql`). Triggers fire inside the same write
//!   transaction, so the no-consistency-window property holds here too.
//!
//! This crate is the **query** side. Indexing is the SQL row insert — owned
//! by `buzz-db`. The relay refetches canonical events through `buzz-db`'s
//! scoped fetcher and runs access checks per hit; search is never the access
//! boundary (conformance row 50).
//!
//! ## Multi-tenant fence
//!
//! Every [`SearchQuery`] carries a [`CommunityId`]. There is no construction
//! path through this crate that omits it, and every SQL execution binds
//! `community_id = $ctx` as a leading predicate. A query bound to community
//! A cannot return events stored under community B, by construction.

/// Search error types.
pub mod error;
/// Search query execution (Postgres FTS).
pub mod query;
/// Search query execution (SQLite FTS5).
pub mod query_sqlite;

pub use buzz_core::CommunityId;
pub use error::SearchError;
pub use query::{search, ChannelScope, SearchHit, SearchMode, SearchQuery, SearchResult};

use sqlx::PgPool;

/// The database backend a [`SearchService`] executes against.
#[derive(Debug, Clone)]
enum SearchBackend {
    /// Postgres FTS over `events.search_tsv`.
    Pg(PgPool),
    /// SQLite FTS5 over the `events_fts` external-content table.
    Sqlite(sqlx::SqlitePool),
}

/// Thin handle around a database pool for community-scoped FTS.
///
/// Holds nothing the pool itself doesn't already own. The whole purpose of
/// this type is a stable injection point for the relay's `AppState`.
#[derive(Debug, Clone)]
pub struct SearchService {
    backend: SearchBackend,
}

impl SearchService {
    /// Build a search service over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            backend: SearchBackend::Pg(pool),
        }
    }

    /// Build a search service over an existing SQLite pool (Solo profile).
    ///
    /// The pool must point at the same database file as `buzz-db`'s SQLite
    /// backend — the `events_fts` index is maintained by that schema's
    /// triggers. See [`query_sqlite`] for the query semantics.
    pub fn new_sqlite(pool: sqlx::SqlitePool) -> Self {
        Self {
            backend: SearchBackend::Sqlite(pool),
        }
    }

    /// Execute a community-scoped FTS query.
    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResult, SearchError> {
        match &self.backend {
            SearchBackend::Pg(pool) => query::search(pool, query).await,
            SearchBackend::Sqlite(pool) => query_sqlite::search(pool, query).await,
        }
    }
}

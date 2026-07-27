//! Backend lifecycle hooks.

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

/// Boxed future used to keep the trait dyn-compatible (same pattern as
/// `buzz_auth::Nip98ReplayGuard`).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Backend-neutral storage error for lifecycle operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The underlying store failed or was unreachable.
    #[error("storage backend error: {0}")]
    Backend(String),
    /// Schema migration failed; the backend must not serve traffic.
    #[error("storage migration error: {0}")]
    Migration(String),
}

/// Lifecycle hooks every storage backend implements.
///
/// These exist so backend-internal machinery stays invisible to the relay
/// (ADR 0001, Decision 2): the Postgres backend runs its migration set,
/// partition pre-creation, and replica-fence probes behind these hooks; a
/// SQLite backend runs its own schema setup, WAL checkpoints, and PRAGMA
/// upkeep. The relay only knows *when* to call them, never *what* they do.
///
/// Singleton/leader election (today: Postgres advisory locks for the
/// usage-metrics leader) is deliberately not part of this trait yet — its
/// lease shape is decided in Phase 2 alongside the domain-store split.
pub trait StoreMaintenance: Send + Sync {
    /// Bring the schema to the current version. Called once at startup,
    /// before the relay serves traffic. Idempotent.
    fn migrate(&self) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Periodic upkeep, called from the relay's background maintenance loop.
    /// Postgres: ensure future partitions exist; SQLite: WAL checkpoint /
    /// incremental vacuum. Must be safe to call at any frequency.
    fn maintenance_tick(&self) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Liveness probe for readiness checks.
    fn ping(&self) -> BoxFuture<'_, Result<(), StorageError>>;
}

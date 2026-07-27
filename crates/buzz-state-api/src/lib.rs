//! Shared-state contract for Buzz relays.
//!
//! Redis serves two different roles in Buzz: a message *transport*
//! (`buzz-messaging-api`) and a shared *state* store — presence keys with
//! TTL, atomic rate-limit windows, and the fail-closed NIP-98 replay
//! seen-set. This crate abstracts the second role, so those concerns can run
//! on an in-process store on a single node (zero external services) and on
//! Redis in multi-node deployments. A pure transport like ZeroMQ is never
//! asked to hold state.
//!
//! Deployment rule (enforced by relay config validation, ADR 0001): the
//! in-process implementation is single-node only. The replay guard and rate
//! limiter are correctness fences that MUST be shared across all pods of a
//! deployment, and callers MUST fail closed on `Err`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use thiserror::Error;

/// Boxed future used to keep the trait dyn-compatible (same pattern as
/// `buzz_auth::Nip98ReplayGuard`).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Shared-state error. Backend-specific failures are flattened into
/// [`StateError::Backend`]; callers on correctness fences (replay guard,
/// rate limiting) MUST fail closed when they receive any error.
#[derive(Debug, Error)]
pub enum StateError {
    /// The underlying store failed or was unreachable.
    #[error("state backend error: {0}")]
    Backend(String),
    /// A stored value could not be interpreted.
    #[error("invalid state value: {0}")]
    InvalidValue(String),
}

/// Convenience alias for shared-state results.
pub type StateResult<T> = Result<T, StateError>;

/// Key/value store with per-key TTL, atomic set-if-absent, and fixed-window
/// counters.
///
/// Key layout is owned by the callers (presence, rate limiting, replay
/// guard), which keep today's Redis key formats byte-for-byte — e.g.
/// `buzz:{community}:presence:{pubkey}` — so the Redis implementation stays
/// wire-compatible with running deployments.
///
/// Backends: Redis (`buzz-pubsub`, production today) and in-process
/// (single-node, Hive plan Phase 1).
pub trait SharedState: Send + Sync {
    /// Set `key` to `value`, expiring after `ttl`. Overwrites any existing
    /// value and resets the TTL (Redis `SET … EX`).
    fn set_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<()>>;

    /// Atomically set `key` only if absent, expiring after `ttl` (Redis
    /// `SET … NX EX`). Returns `true` when newly inserted, `false` when an
    /// entry already existed.
    ///
    /// This backs the NIP-98 replay guard: implementations MUST make the
    /// check-and-set atomic — a read-then-write sequence loses to concurrent
    /// inserts and forfeits the freshness proof. Callers MUST fail closed on
    /// `Err`.
    fn set_nx_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<bool>>;

    /// Fetch the value at `key`, or `None` if absent or expired.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<Option<Vec<u8>>>>;

    /// Fetch many keys in one round trip (Redis `MGET`). The result has the
    /// same length and order as `keys`.
    fn get_many<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, StateResult<Vec<Option<Vec<u8>>>>>;

    /// Remove `key` if present.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<()>>;

    /// Atomically increment the fixed-window counter at `key`, setting the
    /// window expiry on first increment (the Redis implementation uses a Lua
    /// `INCR` + conditional `EXPIRE` script to avoid the crash window between
    /// the two commands). Returns the post-increment count and the remaining
    /// window duration.
    ///
    /// This backs rate limiting; callers MUST fail closed on `Err`.
    fn incr_window<'a>(
        &'a self,
        key: &'a str,
        window: Duration,
    ) -> BoxFuture<'a, StateResult<(u64, Duration)>>;
}

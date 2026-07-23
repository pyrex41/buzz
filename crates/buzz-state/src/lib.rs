//! Backend-generic shared-state consumers and the in-process backend.
//!
//! The consumers — [`PresenceStore`], [`StateRateLimiter`],
//! [`StateNip98ReplayGuard`] — hold an `Arc<dyn SharedState>` and reproduce
//! today's Redis key layouts byte-for-byte, so pointing them at the Redis
//! backend (`buzz_pubsub::shared_state::RedisSharedState`) is
//! wire-compatible with pre-trait deployments, while the in-process backend
//! ([`InProcessSharedState`]) gives a single-node relay the same semantics
//! with zero external services.
//!
//! Deployment rule (ADR 0001): the in-process backend is single-node only —
//! the replay guard and rate limiter are correctness fences that must be
//! shared across every pod of a deployment.

pub mod inproc;
pub mod presence;
pub mod rate_limiter;
pub mod replay;

pub use inproc::InProcessSharedState;
pub use presence::{presence_key, PresenceStore, PRESENCE_TTL_SECS};
pub use rate_limiter::StateRateLimiter;
pub use replay::StateNip98ReplayGuard;

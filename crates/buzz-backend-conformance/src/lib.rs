//! Behavioral conformance suite for pluggable backends (ADR 0001 Decision 4).
//!
//! Backend equivalence is enforced by behavior, not shared implementation:
//! every `SharedState` and `PubSub` backend runs the same checks from its own
//! test suite. Checks panic with a descriptive message on contract violation
//! (they are meant to run under `#[tokio::test]`).
//!
//! Keys and communities are randomized per call so the suite is safe against
//! shared live backends (a developer's Redis) and repeated runs.
//!
//! Time-dependent behavior (TTL expiry, window elapse) is deliberately NOT
//! part of the shared suite — the in-process backends test it under a paused
//! tokio clock, and live backends would need multi-second sleeps. The shared
//! suite covers the clock-free contract: atomicity, isolation, ordering, and
//! round-trip fidelity.

pub mod pubsub;
pub mod shared_state;

pub use pubsub::check_pubsub_contract;
pub use shared_state::check_shared_state_contract;

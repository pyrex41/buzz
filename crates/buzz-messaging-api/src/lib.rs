//! Transport-neutral pub/sub contract for Buzz relays.
//!
//! This crate defines the [`PubSub`] trait plus the message DTOs that ride
//! it: scoped event topics ([`EventTopic`], [`EventTopicKey`]), the local
//! fan-out payload ([`ChannelEvent`]), and the cross-pod control plane
//! ([`CacheInvalidation`], [`ConnControl`]). Concrete transports (Redis
//! today; in-process and ZeroMQ per the Hive implementation plan) implement
//! the trait in their own crates — the relay imports only this crate.
//!
//! Delivery semantics are deliberately ephemeral (at-most-once): events are
//! durable in the event store, and pub/sub is a fan-out accelerator, never
//! the source of truth. Topics are a routing/performance boundary, not an
//! authorization boundary — the relay re-checks access before local fan-out.

pub mod control;
pub mod pubsub;
pub mod topic;

pub use control::{CacheInvalidation, ConnControl, ScopedCacheInvalidation, ScopedConnControl};
pub use pubsub::{ChannelEvent, MessagingError, PubSub};
pub use topic::{channel_key, global_key, EventTopic, EventTopicKey, TopicError, BUZZ_PREFIX};

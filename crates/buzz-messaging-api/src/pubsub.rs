//! The transport-neutral [`PubSub`] trait.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use buzz_core::{CommunityId, TenantContext};
use thiserror::Error;
use tokio::sync::broadcast;

use crate::control::{CacheInvalidation, ConnControl};
use crate::control::{ScopedCacheInvalidation, ScopedConnControl};
use crate::topic::{EventTopic, TopicError};

/// Boxed future used to keep the trait dyn-compatible (same pattern as
/// `buzz_auth::Nip98ReplayGuard`).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Transport-neutral pub/sub error.
///
/// Backend-specific failures (Redis pool exhaustion, ZMQ socket errors, …)
/// are flattened into [`MessagingError::Backend`]; the relay treats them all
/// the same way — log, count, and rely on the event store for durability.
#[derive(Debug, Error)]
pub enum MessagingError {
    /// The underlying transport failed.
    #[error("messaging backend error: {0}")]
    Backend(String),
    /// A payload failed to serialize or deserialize.
    #[error("messaging serialization error: {0}")]
    Serialization(String),
    /// A wire topic name was malformed.
    #[error(transparent)]
    InvalidTopic(#[from] TopicError),
    /// A local broadcast receiver lagged and dropped messages.
    #[error("local subscriber lagged, dropped {0} messages")]
    Lagged(u64),
    /// The background subscriber task stopped.
    #[error("subscriber stopped")]
    SubscriberStopped,
}

/// A Nostr event received on a scoped event topic, broadcast to local
/// subscribers.
#[derive(Debug, Clone)]
pub struct ChannelEvent {
    /// Server-resolved community that scoped the topic.
    pub community_id: CommunityId,
    /// Tenant-local routing scope for this event.
    pub topic: EventTopic,
    /// The Nostr event payload.
    pub event: nostr::Event,
}

/// Community-scoped event fan-out plus the cross-pod control plane.
///
/// Implementations are ephemeral, at-most-once transports: events are durable
/// in the event store, and a lost pub/sub message is recovered by client
/// re-query, never by the transport. Local delivery uses
/// [`tokio::sync::broadcast`] streams — the consumer loop handles `Lagged`.
///
/// Backends: Redis (`buzz-pubsub`, production today), in-process (single
/// node, zero external services), ZeroMQ (small static-mesh clusters). All
/// share the wire topic names in [`crate::topic`].
pub trait PubSub: Send + Sync {
    /// Fan out an event to a community-scoped topic. Fire-and-forget:
    /// durability lives in the event store.
    fn publish_event<'a>(
        &'a self,
        ctx: &'a TenantContext,
        topic: EventTopic,
        event: &'a nostr::Event,
    ) -> BoxFuture<'a, Result<(), MessagingError>>;

    /// Local delivery stream of events arriving on any retained topic.
    fn subscribe_local(&self) -> broadcast::Receiver<ChannelEvent>;

    /// Record local interest in a topic (refcounted). The first retain
    /// subscribes the transport; implementations may debounce unsubscribe.
    fn retain_topic<'a>(
        &'a self,
        ctx: &'a TenantContext,
        topic: EventTopic,
    ) -> BoxFuture<'a, Result<(), MessagingError>>;

    /// Release local interest in a topic previously retained.
    fn release_topic<'a>(
        &'a self,
        ctx: &'a TenantContext,
        topic: EventTopic,
    ) -> BoxFuture<'a, Result<(), MessagingError>>;

    /// Broadcast a cache invalidation to every pod (including this one).
    fn publish_cache_invalidation<'a>(
        &'a self,
        ctx: &'a TenantContext,
        invalidation: &'a CacheInvalidation,
    ) -> BoxFuture<'a, Result<(), MessagingError>>;

    /// Local delivery stream of cross-pod cache invalidations.
    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<ScopedCacheInvalidation>;

    /// Broadcast a connection-control command to every pod (including this one).
    fn publish_conn_control<'a>(
        &'a self,
        ctx: &'a TenantContext,
        command: &'a ConnControl,
    ) -> BoxFuture<'a, Result<(), MessagingError>>;

    /// Local delivery stream of cross-pod connection-control commands.
    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl>;

    /// Background driver: socket pumps, reconnect loops, debounced
    /// unsubscribes. Runs for the lifetime of the relay; implementations
    /// with no background work (in-process) may pend forever.
    fn run(self: Arc<Self>) -> BoxFuture<'static, Result<(), MessagingError>>;
}

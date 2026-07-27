//! In-process [`PubSub`] backend.
//!
//! Single-node transport: publishes loop straight back into the local
//! broadcast streams, mirroring the Redis behavior where a pod receives its
//! own publishes (`SUBSCRIBE`/`PSUBSCRIBE` deliver self-published messages).
//! The relay's fan-out consumer and its access filters are unchanged; the
//! only difference is that no bytes leave the process.
//!
//! Topic retention is a no-op: with one node there is no remote subscription
//! to manage, and local delivery of un-retained topics is dropped by the
//! relay's subscription routing exactly as a lagging Redis topic would be.

use std::sync::Arc;

use buzz_core::TenantContext;
use tokio::sync::broadcast;

use buzz_messaging_api::pubsub::BoxFuture;
use buzz_messaging_api::{
    CacheInvalidation, ChannelEvent, ConnControl, EventTopic, MessagingError, PubSub,
    ScopedCacheInvalidation, ScopedConnControl,
};

/// Default capacity of each local broadcast channel.
const DEFAULT_CAPACITY: usize = 1024;

/// In-process implementation of [`PubSub`].
pub struct InProcessPubSub {
    events: broadcast::Sender<ChannelEvent>,
    cache_invalidations: broadcast::Sender<ScopedCacheInvalidation>,
    conn_control: broadcast::Sender<ScopedConnControl>,
}

impl Default for InProcessPubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl InProcessPubSub {
    /// Create an in-process pub/sub with the default channel capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create an in-process pub/sub with an explicit channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity);
        let (cache_invalidations, _) = broadcast::channel(capacity);
        let (conn_control, _) = broadcast::channel(capacity);
        Self {
            events,
            cache_invalidations,
            conn_control,
        }
    }
}

impl PubSub for InProcessPubSub {
    fn publish_event<'a>(
        &'a self,
        ctx: &'a TenantContext,
        topic: EventTopic,
        event: &'a nostr::Event,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move {
            // A send error only means "no local receivers right now" — same
            // as publishing to a Redis topic with zero subscribers.
            let _ = self.events.send(ChannelEvent {
                community_id: ctx.community(),
                topic,
                event: event.clone(),
            });
            Ok(())
        })
    }

    fn subscribe_local(&self) -> broadcast::Receiver<ChannelEvent> {
        self.events.subscribe()
    }

    fn retain_topic<'a>(
        &'a self,
        _ctx: &'a TenantContext,
        _topic: EventTopic,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move { Ok(()) })
    }

    fn release_topic<'a>(
        &'a self,
        _ctx: &'a TenantContext,
        _topic: EventTopic,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move { Ok(()) })
    }

    fn publish_cache_invalidation<'a>(
        &'a self,
        ctx: &'a TenantContext,
        invalidation: &'a CacheInvalidation,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move {
            let _ = self.cache_invalidations.send(ScopedCacheInvalidation {
                community_id: ctx.community(),
                invalidation: invalidation.clone(),
            });
            Ok(())
        })
    }

    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<ScopedCacheInvalidation> {
        self.cache_invalidations.subscribe()
    }

    fn publish_conn_control<'a>(
        &'a self,
        ctx: &'a TenantContext,
        command: &'a ConnControl,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move {
            let _ = self.conn_control.send(ScopedConnControl {
                community_id: ctx.community(),
                command: command.clone(),
            });
            Ok(())
        })
    }

    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl> {
        self.conn_control.subscribe()
    }

    fn run(self: Arc<Self>) -> BoxFuture<'static, Result<(), MessagingError>> {
        // No sockets to pump — pend for the lifetime of the relay.
        Box::pin(std::future::pending())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::CommunityId;
    use nostr::{EventBuilder, Keys, Kind};
    use uuid::Uuid;

    fn ctx(id: u128) -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(id)), "a.example")
    }

    fn sample_event() -> nostr::Event {
        EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    #[tokio::test]
    async fn published_events_loop_back_to_local_subscribers() {
        let pubsub = InProcessPubSub::new();
        let mut rx = PubSub::subscribe_local(&pubsub);
        let ctx = ctx(0xaaaa);
        let channel_id = Uuid::from_u128(0xbbbb);
        let event = sample_event();

        pubsub
            .publish_event(&ctx, EventTopic::Channel(channel_id), &event)
            .await
            .unwrap();

        let delivered = rx.recv().await.unwrap();
        assert_eq!(delivered.community_id, ctx.community());
        assert_eq!(delivered.topic, EventTopic::Channel(channel_id));
        assert_eq!(delivered.event.id, event.id);
    }

    #[tokio::test]
    async fn publish_without_subscribers_is_not_an_error() {
        let pubsub = InProcessPubSub::new();
        let ctx = ctx(0xaaaa);
        pubsub
            .publish_event(&ctx, EventTopic::Global, &sample_event())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn control_plane_messages_loop_back_scoped() {
        let pubsub = InProcessPubSub::new();
        let mut cache_rx = PubSub::subscribe_cache_invalidations(&pubsub);
        let mut conn_rx = PubSub::subscribe_conn_control(&pubsub);
        let ctx = ctx(0xaaaa);

        pubsub
            .publish_cache_invalidation(&ctx, &CacheInvalidation::AccessibleAll)
            .await
            .unwrap();
        let scoped = cache_rx.recv().await.unwrap();
        assert_eq!(scoped.community_id, ctx.community());
        assert_eq!(scoped.invalidation, CacheInvalidation::AccessibleAll);

        pubsub
            .publish_conn_control(&ctx, &ConnControl::DisconnectCommunity)
            .await
            .unwrap();
        let scoped = conn_rx.recv().await.unwrap();
        assert_eq!(scoped.community_id, ctx.community());
        assert_eq!(scoped.command, ConnControl::DisconnectCommunity);
    }

    #[tokio::test]
    async fn retain_release_are_noops() {
        let pubsub = InProcessPubSub::new();
        let ctx = ctx(0xaaaa);
        pubsub.retain_topic(&ctx, EventTopic::Global).await.unwrap();
        pubsub
            .release_topic(&ctx, EventTopic::Global)
            .await
            .unwrap();
    }
}

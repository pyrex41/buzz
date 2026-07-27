//! [`PubSub`] contract checks.
//!
//! These exercise the local-delivery contract every backend shares: a node
//! observes its own publishes on `subscribe_local` and the control-plane
//! streams, scoped to the publishing community. Cross-node wire delivery is
//! backend-specific (peers, drivers, live services) and stays in each
//! backend's own tests.

use std::sync::Arc;
use std::time::Duration;

use buzz_core::{CommunityId, TenantContext};
use buzz_messaging_api::{CacheInvalidation, ConnControl, EventTopic, PubSub};
use uuid::Uuid;

fn fresh_ctx() -> TenantContext {
    TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "conformance.example",
    )
}

fn sample_event() -> nostr::Event {
    use nostr::{EventBuilder, Keys, Kind};
    EventBuilder::new(Kind::TextNote, "conformance")
        .sign_with_keys(&Keys::generate())
        .expect("sign")
}

/// Run every local-delivery `PubSub` contract check against `pubsub`.
///
/// The backend's `run()` driver may or may not be spawned — loopback
/// delivery must work either way.
pub async fn check_pubsub_contract(pubsub: Arc<dyn PubSub>) {
    check_event_loopback(pubsub.as_ref()).await;
    check_publish_without_subscribers_is_ok(pubsub.as_ref()).await;
    check_control_plane_loopback_scoping(pubsub.as_ref()).await;
    check_retain_release_accept(pubsub.as_ref()).await;
}

/// A published event reaches local subscribers with community, topic, and
/// payload intact.
pub async fn check_event_loopback(pubsub: &dyn PubSub) {
    let ctx = fresh_ctx();
    let channel_id = Uuid::new_v4();
    let event = sample_event();
    let mut rx = pubsub.subscribe_local();

    pubsub
        .publish_event(&ctx, EventTopic::Channel(channel_id), &event)
        .await
        .expect("publish");

    let delivered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let candidate = rx.recv().await.expect("recv");
            // Live shared backends may deliver unrelated traffic — filter to
            // our freshly minted community.
            if candidate.community_id == ctx.community() {
                break candidate;
            }
        }
    })
    .await
    .expect("own publish must loop back locally");

    assert_eq!(delivered.topic, EventTopic::Channel(channel_id));
    assert_eq!(delivered.event.id, event.id, "payload must survive intact");
}

/// Publishing with zero subscribers is not an error (Redis semantics).
pub async fn check_publish_without_subscribers_is_ok(pubsub: &dyn PubSub) {
    let ctx = fresh_ctx();
    pubsub
        .publish_event(&ctx, EventTopic::Global, &sample_event())
        .await
        .expect("publish with no receivers must be Ok");
}

/// Control-plane messages loop back carrying the publisher's community.
pub async fn check_control_plane_loopback_scoping(pubsub: &dyn PubSub) {
    let ctx = fresh_ctx();
    let mut cache_rx = pubsub.subscribe_cache_invalidations();
    let mut conn_rx = pubsub.subscribe_conn_control();

    pubsub
        .publish_cache_invalidation(&ctx, &CacheInvalidation::AccessibleAll)
        .await
        .expect("publish invalidation");
    let scoped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let candidate = cache_rx.recv().await.expect("recv");
            if candidate.community_id == ctx.community() {
                break candidate;
            }
        }
    })
    .await
    .expect("invalidation must loop back");
    assert_eq!(scoped.invalidation, CacheInvalidation::AccessibleAll);

    pubsub
        .publish_conn_control(&ctx, &ConnControl::DisconnectCommunity)
        .await
        .expect("publish conn control");
    let scoped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let candidate = conn_rx.recv().await.expect("recv");
            if candidate.community_id == ctx.community() {
                break candidate;
            }
        }
    })
    .await
    .expect("conn control must loop back");
    assert_eq!(scoped.command, ConnControl::DisconnectCommunity);
}

/// Retain/release must accept any topic without error.
pub async fn check_retain_release_accept(pubsub: &dyn PubSub) {
    let ctx = fresh_ctx();
    let topic = EventTopic::Channel(Uuid::new_v4());
    pubsub.retain_topic(&ctx, topic).await.expect("retain");
    pubsub.release_topic(&ctx, topic).await.expect("release");
    pubsub
        .retain_topic(&ctx, EventTopic::Global)
        .await
        .expect("retain global");
    pubsub
        .release_topic(&ctx, EventTopic::Global)
        .await
        .expect("release global");
}

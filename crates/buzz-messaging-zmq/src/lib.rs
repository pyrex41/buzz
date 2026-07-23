//! ZeroMQ [`PubSub`] backend — static full-mesh transport for small clusters.
//!
//! Topology (Hive plan §4.2): every node binds one `PUB` socket at
//! [`ZmqTransportConfig::bind`] and runs one `SUB` socket connected to every
//! peer in [`ZmqTransportConfig::peers`]. A node's own publishes are
//! **looped back in-process** (mirroring Redis delivering self-published
//! messages), so `peers` must list only *other* nodes — never the node
//! itself.
//!
//! Wire format: two ZMQ frames per message. Frame 0 is the topic — the exact
//! Redis-era channel name (`buzz:{community}:channel:{uuid}`,
//! `buzz:{community}:global`, `buzz:{community}:cache-invalidate`,
//! `buzz:{community}:conn-control`) so ZMQ prefix subscription on `buzz:`
//! covers events and the control plane alike. Frame 1 is the JSON payload —
//! identical bytes to what the Redis backend publishes.
//!
//! Delivery semantics: at-most-once, ephemeral — same contract as every
//! other backend. Events are durable in the event store; ZMQ's slow-joiner
//! and disconnect drops are recovered by client re-query, never by the
//! transport.
//!
//! Explicit non-goals (v1): dynamic peer discovery, broker (XPUB/XSUB)
//! topologies, CURVE encryption. Clusters that outgrow a static peer list
//! should run the Redis backend until a discovery helper ships (Phase 5).

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc, Mutex};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

use buzz_core::TenantContext;
use buzz_messaging_api::control::{
    cache_invalidation_channel, conn_control_channel, parse_cache_invalidation_channel,
    parse_conn_control_channel,
};
use buzz_messaging_api::pubsub::BoxFuture;
use buzz_messaging_api::{
    CacheInvalidation, ChannelEvent, ConnControl, EventTopic, EventTopicKey, MessagingError,
    PubSub, ScopedCacheInvalidation, ScopedConnControl, BUZZ_PREFIX,
};
use nostr::JsonUtil;

/// Capacity of the local broadcast channels and the outbound send queue.
const CHANNEL_CAPACITY: usize = 1024;

/// Reconnect backoff bounds for the socket driver loop.
const BACKOFF_INITIAL_SECS: u64 = 1;
const BACKOFF_MAX_SECS: u64 = 30;

/// Static-mesh transport configuration.
#[derive(Debug, Clone)]
pub struct ZmqTransportConfig {
    /// Endpoint this node's `PUB` socket binds (e.g. `tcp://0.0.0.0:5559`
    /// or `ipc:///run/buzz/pubsub.ipc`).
    pub bind: String,
    /// `PUB` endpoints of every *other* node in the mesh. Must not include
    /// this node's own endpoint — own publishes are looped back in-process,
    /// and a self-connection would double-deliver.
    pub peers: Vec<String>,
}

/// An outbound wire message: (topic frame, payload frame).
type Outbound = (String, Vec<u8>);

/// ZeroMQ implementation of [`PubSub`].
pub struct ZmqPubSub {
    config: ZmqTransportConfig,
    events: broadcast::Sender<ChannelEvent>,
    cache_invalidations: broadcast::Sender<ScopedCacheInvalidation>,
    conn_control: broadcast::Sender<ScopedConnControl>,
    outbound_tx: mpsc::Sender<Outbound>,
    /// Taken exactly once by [`PubSub::run`].
    outbound_rx: Mutex<Option<mpsc::Receiver<Outbound>>>,
}

impl ZmqPubSub {
    /// Create a ZMQ pub/sub for the given static mesh. Sockets are not
    /// created until [`PubSub::run`] is spawned.
    pub fn new(config: ZmqTransportConfig) -> Self {
        let (events, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (cache_invalidations, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (conn_control, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = mpsc::channel(CHANNEL_CAPACITY);
        Self {
            config,
            events,
            cache_invalidations,
            conn_control,
            outbound_tx,
            outbound_rx: Mutex::new(Some(outbound_rx)),
        }
    }

    /// Queue a wire message for the PUB socket. Best-effort: a full queue
    /// (driver not running or stalled) drops the message, same as a
    /// disconnected Redis publish — durability lives in the event store.
    fn enqueue(&self, topic: String, payload: Vec<u8>) {
        if let Err(e) = self.outbound_tx.try_send((topic, payload)) {
            tracing::warn!("zmq outbound queue full or closed — dropping message: {e}");
        }
    }

    /// Route one inbound wire message to the local broadcast streams.
    fn route_inbound(&self, topic: &str, payload: &[u8]) {
        if let Some(community_id) = parse_cache_invalidation_channel(topic) {
            match serde_json::from_slice::<CacheInvalidation>(payload) {
                Ok(invalidation) => {
                    let _ = self.cache_invalidations.send(ScopedCacheInvalidation {
                        community_id,
                        invalidation,
                    });
                }
                Err(e) => tracing::warn!("zmq: bad cache-invalidation payload: {e}"),
            }
            return;
        }
        if let Some(community_id) = parse_conn_control_channel(topic) {
            match serde_json::from_slice::<ConnControl>(payload) {
                Ok(command) => {
                    let _ = self.conn_control.send(ScopedConnControl {
                        community_id,
                        command,
                    });
                }
                Err(e) => tracing::warn!("zmq: bad conn-control payload: {e}"),
            }
            return;
        }
        match EventTopicKey::parse_redis_channel(topic) {
            Ok(key) => match nostr::Event::from_json(payload) {
                Ok(event) => {
                    let _ = self.events.send(ChannelEvent {
                        community_id: key.community_id,
                        topic: key.topic,
                        event,
                    });
                }
                Err(e) => tracing::warn!("zmq: bad event payload on {topic}: {e}"),
            },
            Err(_) => tracing::warn!("zmq: message on unexpected topic: {topic}"),
        }
    }

    /// One socket lifetime: bind PUB, connect SUB to all peers, pump both
    /// until an error. Returns the error that ended the session.
    async fn drive_sockets(
        &self,
        outbound_rx: &mut mpsc::Receiver<Outbound>,
    ) -> Result<(), zeromq::ZmqError> {
        let mut pub_socket = zeromq::PubSocket::new();
        pub_socket.bind(&self.config.bind).await?;

        let mut sub_socket = zeromq::SubSocket::new();
        for peer in &self.config.peers {
            sub_socket.connect(peer).await?;
        }
        // One prefix covers event topics AND the control plane — all wire
        // topics start with "buzz:".
        sub_socket.subscribe(&format!("{BUZZ_PREFIX}:")).await?;

        tracing::info!(
            bind = %self.config.bind,
            peers = self.config.peers.len(),
            "zmq transport up"
        );

        loop {
            tokio::select! {
                outbound = outbound_rx.recv() => {
                    let Some((topic, payload)) = outbound else {
                        // All senders dropped — relay shutting down.
                        return Ok(());
                    };
                    let mut msg = ZmqMessage::from(Bytes::from(topic.into_bytes()));
                    msg.push_back(Bytes::from(payload));
                    pub_socket.send(msg).await?;
                }
                inbound = sub_socket.recv() => {
                    let msg = inbound?;
                    let (Some(topic), Some(payload)) = (msg.get(0), msg.get(1)) else {
                        tracing::warn!("zmq: dropping message without topic+payload frames");
                        continue;
                    };
                    match std::str::from_utf8(topic) {
                        Ok(topic) => self.route_inbound(topic, payload),
                        Err(_) => tracing::warn!("zmq: non-UTF-8 topic frame dropped"),
                    }
                }
            }
        }
    }
}

impl PubSub for ZmqPubSub {
    fn publish_event<'a>(
        &'a self,
        ctx: &'a TenantContext,
        topic: EventTopic,
        event: &'a nostr::Event,
    ) -> BoxFuture<'a, Result<(), MessagingError>> {
        Box::pin(async move {
            let wire_topic = EventTopicKey::from_context(ctx, topic).redis_channel();
            let payload = event.as_json().into_bytes();
            // Local loopback first (own publishes are delivered in-process),
            // then the wire for the peers.
            let _ = self.events.send(ChannelEvent {
                community_id: ctx.community(),
                topic,
                event: event.clone(),
            });
            self.enqueue(wire_topic, payload);
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
        // The SUB socket subscribes to the whole "buzz:" prefix: with a
        // static mesh the subscription set is not per-topic, so retention is
        // bookkeeping-free. Local routing drops un-consumed topics.
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
            let payload = serde_json::to_vec(invalidation)
                .map_err(|e| MessagingError::Serialization(e.to_string()))?;
            let _ = self.cache_invalidations.send(ScopedCacheInvalidation {
                community_id: ctx.community(),
                invalidation: invalidation.clone(),
            });
            self.enqueue(cache_invalidation_channel(ctx), payload);
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
            let payload = serde_json::to_vec(command)
                .map_err(|e| MessagingError::Serialization(e.to_string()))?;
            let _ = self.conn_control.send(ScopedConnControl {
                community_id: ctx.community(),
                command: command.clone(),
            });
            self.enqueue(conn_control_channel(ctx), payload);
            Ok(())
        })
    }

    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl> {
        self.conn_control.subscribe()
    }

    fn run(self: Arc<Self>) -> BoxFuture<'static, Result<(), MessagingError>> {
        Box::pin(async move {
            let mut outbound_rx = self
                .outbound_rx
                .lock()
                .await
                .take()
                .ok_or(MessagingError::SubscriberStopped)?;
            let mut backoff_secs = BACKOFF_INITIAL_SECS;
            loop {
                match self.drive_sockets(&mut outbound_rx).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        tracing::error!(
                            "zmq transport error: {e} — rebuilding sockets in {backoff_secs}s"
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(BACKOFF_MAX_SECS);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::CommunityId;
    use nostr::{EventBuilder, Keys, Kind};
    use std::time::Duration;
    use uuid::Uuid;

    fn ctx(id: u128) -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(id)), "a.example")
    }

    fn sample_event() -> nostr::Event {
        EventBuilder::new(Kind::TextNote, "over the wire")
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    /// Two-node mesh on loopback TCP. ZMQ PUB/SUB drops messages published
    /// before the subscription propagates (slow joiner), so the publisher
    /// retries until the receiver observes the message.
    #[tokio::test(flavor = "multi_thread")]
    async fn event_crosses_the_wire_between_two_nodes() {
        let port_a = free_port();
        let port_b = free_port();
        let node_a = Arc::new(ZmqPubSub::new(ZmqTransportConfig {
            bind: format!("tcp://127.0.0.1:{port_a}"),
            peers: vec![format!("tcp://127.0.0.1:{port_b}")],
        }));
        let node_b = Arc::new(ZmqPubSub::new(ZmqTransportConfig {
            bind: format!("tcp://127.0.0.1:{port_b}"),
            peers: vec![format!("tcp://127.0.0.1:{port_a}")],
        }));
        let _driver_a = tokio::spawn(Arc::clone(&node_a).run());
        let _driver_b = tokio::spawn(Arc::clone(&node_b).run());

        let ctx = ctx(0xaaaa);
        let channel_id = Uuid::from_u128(0xbbbb);
        let event = sample_event();
        let mut rx_b = PubSub::subscribe_local(node_b.as_ref());

        let delivered = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                node_a
                    .publish_event(&ctx, EventTopic::Channel(channel_id), &event)
                    .await
                    .unwrap();
                // Drain until we see a wire delivery or need to re-publish.
                match tokio::time::timeout(Duration::from_millis(300), rx_b.recv()).await {
                    Ok(Ok(delivered)) => break delivered,
                    Ok(Err(_)) | Err(_) => continue,
                }
            }
        })
        .await
        .expect("event must cross the wire");

        assert_eq!(delivered.community_id, ctx.community());
        assert_eq!(delivered.topic, EventTopic::Channel(channel_id));
        assert_eq!(delivered.event.id, event.id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn control_plane_crosses_the_wire() {
        let port_a = free_port();
        let port_b = free_port();
        let node_a = Arc::new(ZmqPubSub::new(ZmqTransportConfig {
            bind: format!("tcp://127.0.0.1:{port_a}"),
            peers: vec![format!("tcp://127.0.0.1:{port_b}")],
        }));
        let node_b = Arc::new(ZmqPubSub::new(ZmqTransportConfig {
            bind: format!("tcp://127.0.0.1:{port_b}"),
            peers: vec![format!("tcp://127.0.0.1:{port_a}")],
        }));
        let _driver_a = tokio::spawn(Arc::clone(&node_a).run());
        let _driver_b = tokio::spawn(Arc::clone(&node_b).run());

        let ctx = ctx(0xcccc);
        let mut rx_b = PubSub::subscribe_cache_invalidations(node_b.as_ref());

        let scoped = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                node_a
                    .publish_cache_invalidation(&ctx, &CacheInvalidation::AccessibleAll)
                    .await
                    .unwrap();
                match tokio::time::timeout(Duration::from_millis(300), rx_b.recv()).await {
                    Ok(Ok(scoped)) => break scoped,
                    Ok(Err(_)) | Err(_) => continue,
                }
            }
        })
        .await
        .expect("invalidation must cross the wire");

        assert_eq!(scoped.community_id, ctx.community());
        assert_eq!(scoped.invalidation, CacheInvalidation::AccessibleAll);
    }

    #[tokio::test]
    async fn own_publishes_loop_back_without_the_wire() {
        // No driver spawned — the loopback path must work standalone.
        let node = ZmqPubSub::new(ZmqTransportConfig {
            bind: format!("tcp://127.0.0.1:{}", free_port()),
            peers: vec![],
        });
        let mut rx = PubSub::subscribe_local(&node);
        let ctx = ctx(0xdddd);
        let event = sample_event();

        node.publish_event(&ctx, EventTopic::Global, &event)
            .await
            .unwrap();
        let delivered = rx.recv().await.unwrap();
        assert_eq!(delivered.event.id, event.id);
    }

    /// Grab an OS-assigned free TCP port. Racy in principle, unique enough
    /// in practice for loopback tests.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }
}

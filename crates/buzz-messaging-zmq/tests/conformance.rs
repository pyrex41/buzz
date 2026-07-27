//! ZeroMQ PubSub backend vs the shared conformance suite.
//!
//! Runs without spawning the socket driver: the local-delivery contract must
//! hold standalone (loopback is in-process by design).

use std::sync::Arc;

use buzz_messaging_zmq::{ZmqPubSub, ZmqTransportConfig};

#[tokio::test]
async fn zmq_pubsub_meets_the_contract() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let pubsub = Arc::new(ZmqPubSub::new(ZmqTransportConfig {
        bind: format!("tcp://127.0.0.1:{port}"),
        peers: vec![],
    }));
    buzz_backend_conformance::check_pubsub_contract(pubsub).await;
}

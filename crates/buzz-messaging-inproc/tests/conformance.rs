//! In-process PubSub backend vs the shared conformance suite.

use std::sync::Arc;

use buzz_messaging_inproc::InProcessPubSub;

#[tokio::test]
async fn inproc_pubsub_meets_the_contract() {
    buzz_backend_conformance::check_pubsub_contract(Arc::new(InProcessPubSub::new())).await;
}

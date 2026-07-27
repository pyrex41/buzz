//! Redis SharedState + PubSub backends vs the shared conformance suite.
//!
//! Redis-gated like the other live tests in this crate. Run with:
//! `cargo test -p buzz-pubsub --test conformance -- --ignored`

use std::sync::Arc;

use buzz_pubsub::shared_state::RedisSharedState;
use buzz_pubsub::PubSubManager;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn pool() -> deadpool_redis::Pool {
    deadpool_redis::Config::from_url(redis_url())
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("create pool")
}

#[tokio::test]
#[ignore = "requires Redis"]
async fn redis_shared_state_meets_the_contract() {
    buzz_backend_conformance::check_shared_state_contract(Arc::new(RedisSharedState::new(pool())))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Redis"]
async fn redis_pubsub_meets_the_contract() {
    let pubsub = Arc::new(
        PubSubManager::new(&redis_url(), pool())
            .await
            .expect("pubsub manager"),
    );
    // Loopback via Redis requires the live subscriber driver.
    let _driver = tokio::spawn(Arc::clone(&pubsub).run_subscriber());
    let _cache_driver = tokio::spawn(Arc::clone(&pubsub).run_cache_invalidation_subscriber());
    let _conn_driver = tokio::spawn(Arc::clone(&pubsub).run_conn_control_subscriber());
    // The retained-topic set gates event delivery on the Redis backend; the
    // conformance event check subscribes to a fresh community, so retain its
    // global topic ahead of time is not possible here — the PubSubManager
    // subscribes dynamically on retain, which check_event_loopback does not
    // call. Give the drivers a moment, then run the contract.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    buzz_backend_conformance::check_pubsub_contract(pubsub).await;
}

//! [`SharedState`] contract checks.

use std::sync::Arc;
use std::time::Duration;

use buzz_state_api::SharedState;
use uuid::Uuid;

fn fresh_key(tag: &str) -> String {
    format!("buzz:conformance:{}:{tag}", Uuid::new_v4())
}

/// Run every clock-free `SharedState` contract check against `state`.
pub async fn check_shared_state_contract(state: Arc<dyn SharedState>) {
    check_kv_round_trip(state.as_ref()).await;
    check_set_overwrites(state.as_ref()).await;
    check_set_nx_claims_once(state.as_ref()).await;
    check_get_many_order_and_gaps(state.as_ref()).await;
    check_window_counts_monotonically(state.as_ref()).await;
    check_windows_are_per_key(state.as_ref()).await;
    check_concurrent_set_nx_single_winner(Arc::clone(&state)).await;
}

/// set → get returns the same bytes; delete → get returns None.
pub async fn check_kv_round_trip(state: &dyn SharedState) {
    let key = fresh_key("kv");
    assert!(
        state.get(&key).await.expect("get").is_none(),
        "fresh key must read as absent"
    );
    state
        .set_with_ttl(&key, b"payload-bytes", Duration::from_secs(60))
        .await
        .expect("set");
    assert_eq!(
        state.get(&key).await.expect("get").as_deref(),
        Some(&b"payload-bytes"[..]),
        "get must return the exact bytes written"
    );
    state.delete(&key).await.expect("delete");
    assert!(
        state.get(&key).await.expect("get").is_none(),
        "deleted key must read as absent"
    );
}

/// A second set replaces the value (last write wins on plain set).
pub async fn check_set_overwrites(state: &dyn SharedState) {
    let key = fresh_key("overwrite");
    state
        .set_with_ttl(&key, b"first", Duration::from_secs(60))
        .await
        .expect("set");
    state
        .set_with_ttl(&key, b"second", Duration::from_secs(60))
        .await
        .expect("set");
    assert_eq!(
        state.get(&key).await.expect("get").as_deref(),
        Some(&b"second"[..]),
        "plain set must overwrite"
    );
    state.delete(&key).await.expect("delete");
}

/// set_nx returns true exactly once while the key lives.
pub async fn check_set_nx_claims_once(state: &dyn SharedState) {
    let key = fresh_key("nx");
    assert!(
        state
            .set_nx_with_ttl(&key, b"1", Duration::from_secs(120))
            .await
            .expect("set_nx"),
        "first claim must win"
    );
    assert!(
        !state
            .set_nx_with_ttl(&key, b"1", Duration::from_secs(120))
            .await
            .expect("set_nx"),
        "second claim must lose"
    );
    state.delete(&key).await.expect("delete");
}

/// get_many preserves request order and reports gaps as None.
pub async fn check_get_many_order_and_gaps(state: &dyn SharedState) {
    let k1 = fresh_key("m1");
    let k2 = fresh_key("m2");
    let k3 = fresh_key("m3");
    state
        .set_with_ttl(&k1, b"a", Duration::from_secs(60))
        .await
        .expect("set");
    state
        .set_with_ttl(&k3, b"c", Duration::from_secs(60))
        .await
        .expect("set");
    let values = state
        .get_many(&[k1.clone(), k2, k3.clone()])
        .await
        .expect("get_many");
    assert_eq!(values.len(), 3, "result length must match request length");
    assert_eq!(values[0].as_deref(), Some(&b"a"[..]));
    assert!(values[1].is_none(), "absent key must be None, not skipped");
    assert_eq!(values[2].as_deref(), Some(&b"c"[..]));
    state.delete(&k1).await.expect("delete");
    state.delete(&k3).await.expect("delete");
}

/// Repeated increments in one window count 1, 2, 3… and report a
/// remaining-window duration within the configured bound.
pub async fn check_window_counts_monotonically(state: &dyn SharedState) {
    let key = fresh_key("window");
    let window = Duration::from_secs(60);
    for expect in 1..=3u64 {
        let (count, remaining) = state.incr_window(&key, window).await.expect("incr");
        assert_eq!(count, expect, "window counter must increment by one");
        assert!(
            remaining <= window,
            "remaining window cannot exceed the configured window"
        );
    }
    state.delete(&key).await.ok();
}

/// Two different keys count independently.
pub async fn check_windows_are_per_key(state: &dyn SharedState) {
    let a = fresh_key("wa");
    let b = fresh_key("wb");
    let window = Duration::from_secs(60);
    state.incr_window(&a, window).await.expect("incr");
    state.incr_window(&a, window).await.expect("incr");
    let (count_b, _) = state.incr_window(&b, window).await.expect("incr");
    assert_eq!(count_b, 1, "windows must be independent per key");
}

/// N concurrent set_nx racers on one key admit exactly one winner —
/// the atomicity guarantee the NIP-98 replay guard stands on.
pub async fn check_concurrent_set_nx_single_winner(state: Arc<dyn SharedState>) {
    let key = fresh_key("race");
    let mut handles = Vec::new();
    for _ in 0..16 {
        let state = Arc::clone(&state);
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            state
                .set_nx_with_ttl(&key, b"1", Duration::from_secs(120))
                .await
                .expect("set_nx")
        }));
    }
    let mut winners = 0;
    for handle in handles {
        if handle.await.expect("join") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one concurrent set_nx claim must win");
    state.delete(&key).await.expect("delete");
}

//! In-process [`SharedState`] backend.
//!
//! Single-node only (ADR 0001): the map lives in this process, so nothing is
//! shared with other pods. Expiry uses [`tokio::time::Instant`] so tests can
//! drive the clock with `tokio::time::pause`/`advance`.
//!
//! Atomicity: `set_nx_with_ttl` and `incr_window` go through the DashMap
//! entry API, which holds the shard lock for the read-modify-write — the
//! same guarantee the Redis backend gets from `SET NX` and its Lua script.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::time::Instant;

use buzz_state_api::{BoxFuture, SharedState, StateResult};

/// Sweep the maps for expired entries every this many writes.
const SWEEP_EVERY_WRITES: u64 = 4096;

struct ValueEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

struct WindowEntry {
    count: u64,
    window_ends_at: Instant,
}

/// In-process implementation of [`SharedState`].
#[derive(Default)]
pub struct InProcessSharedState {
    values: DashMap<String, ValueEntry>,
    windows: DashMap<String, WindowEntry>,
    writes: AtomicU64,
}

impl InProcessSharedState {
    /// Create an empty in-process store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Opportunistically drop expired entries. Called every
    /// [`SWEEP_EVERY_WRITES`] writes so an idle key set cannot grow without
    /// bound between reads.
    fn maybe_sweep(&self, now: Instant) {
        if !self
            .writes
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(SWEEP_EVERY_WRITES)
        {
            return;
        }
        self.values.retain(|_, e| e.expires_at > now);
        self.windows.retain(|_, e| e.window_ends_at > now);
    }
}

impl SharedState for InProcessSharedState {
    fn set_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<()>> {
        Box::pin(async move {
            let now = Instant::now();
            self.maybe_sweep(now);
            self.values.insert(
                key.to_string(),
                ValueEntry {
                    value: value.to_vec(),
                    expires_at: now + ttl,
                },
            );
            Ok(())
        })
    }

    fn set_nx_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<bool>> {
        Box::pin(async move {
            let now = Instant::now();
            self.maybe_sweep(now);
            match self.values.entry(key.to_string()) {
                Entry::Occupied(mut occupied) => {
                    if occupied.get().expires_at > now {
                        Ok(false)
                    } else {
                        // Expired entry — claimable again, same as Redis
                        // after the TTL elapses.
                        occupied.insert(ValueEntry {
                            value: value.to_vec(),
                            expires_at: now + ttl,
                        });
                        Ok(true)
                    }
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(ValueEntry {
                        value: value.to_vec(),
                        expires_at: now + ttl,
                    });
                    Ok(true)
                }
            }
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<Option<Vec<u8>>>> {
        Box::pin(async move {
            let now = Instant::now();
            let value = self
                .values
                .get(key)
                .filter(|e| e.expires_at > now)
                .map(|e| e.value.clone());
            if value.is_none() {
                // Lazily drop an expired entry (guard ref released above).
                self.values.remove_if(key, |_, e| e.expires_at <= now);
            }
            Ok(value)
        })
    }

    fn get_many<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, StateResult<Vec<Option<Vec<u8>>>>> {
        Box::pin(async move {
            let now = Instant::now();
            Ok(keys
                .iter()
                .map(|key| {
                    self.values
                        .get(key)
                        .filter(|e| e.expires_at > now)
                        .map(|e| e.value.clone())
                })
                .collect())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<()>> {
        Box::pin(async move {
            self.values.remove(key);
            Ok(())
        })
    }

    fn incr_window<'a>(
        &'a self,
        key: &'a str,
        window: Duration,
    ) -> BoxFuture<'a, StateResult<(u64, Duration)>> {
        Box::pin(async move {
            let now = Instant::now();
            self.maybe_sweep(now);
            let mut entry = self
                .windows
                .entry(key.to_string())
                .or_insert_with(|| WindowEntry {
                    count: 0,
                    window_ends_at: now + window,
                });
            if entry.window_ends_at <= now {
                // Window elapsed — start a fresh one, same as Redis expiring
                // the counter key.
                entry.count = 0;
                entry.window_ends_at = now + window;
            }
            entry.count += 1;
            let remaining = entry.window_ends_at.saturating_duration_since(now);
            Ok((entry.count, remaining))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_delete_round_trip() {
        let state = InProcessSharedState::new();
        assert!(state.get("k").await.unwrap().is_none());
        state
            .set_with_ttl("k", b"online", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            state.get("k").await.unwrap().as_deref(),
            Some(&b"online"[..])
        );
        state.delete("k").await.unwrap();
        assert!(state.get("k").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn values_expire_after_ttl() {
        let state = InProcessSharedState::new();
        state
            .set_with_ttl("k", b"v", Duration::from_secs(90))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(91)).await;
        assert!(state.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_nx_claims_once() {
        let state = InProcessSharedState::new();
        assert!(state
            .set_nx_with_ttl("k", b"1", Duration::from_secs(120))
            .await
            .unwrap());
        assert!(!state
            .set_nx_with_ttl("k", b"1", Duration::from_secs(120))
            .await
            .unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn set_nx_reclaims_after_expiry() {
        let state = InProcessSharedState::new();
        assert!(state
            .set_nx_with_ttl("k", b"1", Duration::from_secs(120))
            .await
            .unwrap());
        tokio::time::advance(Duration::from_secs(121)).await;
        assert!(state
            .set_nx_with_ttl("k", b"1", Duration::from_secs(120))
            .await
            .unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn incr_window_counts_then_resets() {
        let state = InProcessSharedState::new();
        let window = Duration::from_secs(60);

        let (count, remaining) = state.incr_window("k", window).await.unwrap();
        assert_eq!(count, 1);
        assert!(remaining <= window && remaining > Duration::ZERO);

        let (count, _) = state.incr_window("k", window).await.unwrap();
        assert_eq!(count, 2);

        tokio::time::advance(Duration::from_secs(61)).await;
        let (count, _) = state.incr_window("k", window).await.unwrap();
        assert_eq!(count, 1, "fresh window restarts the count");
    }

    #[tokio::test]
    async fn get_many_preserves_order_and_gaps() {
        let state = InProcessSharedState::new();
        state
            .set_with_ttl("a", b"1", Duration::from_secs(60))
            .await
            .unwrap();
        state
            .set_with_ttl("c", b"3", Duration::from_secs(60))
            .await
            .unwrap();
        let values = state
            .get_many(&["a".into(), "b".into(), "c".into()])
            .await
            .unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_deref(), Some(&b"1"[..]));
        assert!(values[1].is_none());
        assert_eq!(values[2].as_deref(), Some(&b"3"[..]));
    }

    #[tokio::test]
    async fn concurrent_set_nx_admits_exactly_one() {
        let state = std::sync::Arc::new(InProcessSharedState::new());
        let mut handles = Vec::new();
        for _ in 0..32 {
            let state = std::sync::Arc::clone(&state);
            handles.push(tokio::spawn(async move {
                state
                    .set_nx_with_ttl("contested", b"1", Duration::from_secs(120))
                    .await
                    .unwrap()
            }));
        }
        let mut winners = 0;
        for handle in handles {
            if handle.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one concurrent claim must win");
    }
}

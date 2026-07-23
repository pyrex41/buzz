//! Redis implementation of the [`SharedState`] contract.
//!
//! Command mapping (kept byte-for-byte compatible with the pre-trait code
//! paths so running deployments see identical Redis traffic):
//!
//! - `set_with_ttl`    → `SET key value EX <secs>`
//! - `set_nx_with_ttl` → `SET key value NX EX <secs>` (atomic set-if-absent)
//! - `get` / `get_many` / `delete` → `GET` / `MGET` / `DEL`
//! - `incr_window`     → the atomic Lua `INCR` + conditional `EXPIRE` script
//!   (plus the TTL-repair path for keys orphaned without expiry by a crash
//!   in a pre-script era)

use std::time::Duration;

use buzz_state_api::{BoxFuture, SharedState, StateError, StateResult};
use redis::Script;

/// Atomically INCR the key, set EXPIRE on first call, and return (count, ttl).
///
/// Using a Lua script ensures INCR and EXPIRE are executed atomically —
/// a crash between them can no longer leave a key without a TTL.
const INCR_WINDOW_SCRIPT: &str = r#"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('TTL', KEYS[1])
return {count, ttl}
"#;

/// Clamp a [`Duration`] to whole seconds for Redis `EX`, flooring at 1s so a
/// sub-second TTL cannot silently become "no expiry".
fn ttl_secs(ttl: Duration) -> u64 {
    ttl.as_secs().max(1)
}

fn pool_err(e: deadpool_redis::PoolError) -> StateError {
    StateError::Backend(format!("Redis pool: {e}"))
}

fn redis_err(e: redis::RedisError) -> StateError {
    StateError::Backend(format!("Redis: {e}"))
}

/// Redis-backed [`SharedState`] over a deadpool connection pool.
pub struct RedisSharedState {
    pool: deadpool_redis::Pool,
}

impl RedisSharedState {
    /// Create a shared-state store backed by the given Redis connection pool.
    pub fn new(pool: deadpool_redis::Pool) -> Self {
        Self { pool }
    }
}

impl SharedState for RedisSharedState {
    fn set_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<()>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await.map_err(pool_err)?;
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("EX")
                .arg(ttl_secs(ttl))
                .query_async::<()>(&mut conn)
                .await
                .map_err(redis_err)
        })
    }

    fn set_nx_with_ttl<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, StateResult<bool>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await.map_err(pool_err)?;
            // SET key value NX EX <ttl>. Typed reply: Some("OK") on first
            // claim, None on existing key. Any other value would be a
            // Redis-side bug; surface it as a backend error (callers on
            // correctness fences fail closed).
            let result: Option<String> = redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .arg("EX")
                .arg(ttl_secs(ttl))
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
            match result.as_deref() {
                Some("OK") => Ok(true),
                None => Ok(false),
                Some(other) => Err(StateError::InvalidValue(format!(
                    "unexpected SET NX EX reply: {other}"
                ))),
            }
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await.map_err(pool_err)?;
            redis::cmd("GET")
                .arg(key)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)
        })
    }

    fn get_many<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, StateResult<Vec<Option<Vec<u8>>>>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let mut conn = self.pool.get().await.map_err(pool_err)?;
            redis::cmd("MGET")
                .arg(keys)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, StateResult<()>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await.map_err(pool_err)?;
            redis::cmd("DEL")
                .arg(key)
                .query_async::<()>(&mut conn)
                .await
                .map_err(redis_err)
        })
    }

    fn incr_window<'a>(
        &'a self,
        key: &'a str,
        window: Duration,
    ) -> BoxFuture<'a, StateResult<(u64, Duration)>> {
        Box::pin(async move {
            let window_secs = ttl_secs(window);
            let mut conn = self.pool.get().await.map_err(pool_err)?;

            let script = Script::new(INCR_WINDOW_SCRIPT);
            let (count, ttl): (u64, i64) = script
                .key(key)
                .arg(window_secs as i64)
                .invoke_async(&mut *conn)
                .await
                .map_err(redis_err)?;

            // ttl == -1 means the key exists but has no expiry — broken state
            // from a crash between INCR and EXPIRE in a pre-script era.
            // Repair it now.
            let reset_in_secs = if ttl < 0 {
                tracing::warn!(key = %key, "window counter key has no TTL — repairing");
                redis::cmd("EXPIRE")
                    .arg(key)
                    .arg(window_secs as i64)
                    .query_async::<()>(&mut *conn)
                    .await
                    .map_err(redis_err)?;
                // After repair, the window resets to the full duration.
                window_secs
            } else {
                ttl.max(0) as u64
            };

            Ok((count, Duration::from_secs(reset_in_secs)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::make_test_pool;
    use uuid::Uuid;

    fn fresh_key(suffix: &str) -> String {
        format!("buzz:test:{}:{suffix}", Uuid::new_v4())
    }

    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn set_get_delete_round_trip() {
        let state = RedisSharedState::new(make_test_pool());
        let key = fresh_key("kv");

        assert!(state.get(&key).await.unwrap().is_none());
        state
            .set_with_ttl(&key, b"online", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            state.get(&key).await.unwrap().as_deref(),
            Some(&b"online"[..])
        );
        state.delete(&key).await.unwrap();
        assert!(state.get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn set_nx_claims_once() {
        let state = RedisSharedState::new(make_test_pool());
        let key = fresh_key("nx");

        assert!(state
            .set_nx_with_ttl(&key, b"1", Duration::from_secs(120))
            .await
            .unwrap());
        assert!(!state
            .set_nx_with_ttl(&key, b"1", Duration::from_secs(120))
            .await
            .unwrap());
        state.delete(&key).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn incr_window_counts_and_reports_reset() {
        let state = RedisSharedState::new(make_test_pool());
        let key = fresh_key("window");

        let (count, reset) = state
            .incr_window(&key, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert!(reset <= Duration::from_secs(60) && reset > Duration::ZERO);

        let (count, _) = state
            .incr_window(&key, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(count, 2);
        state.delete(&key).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn get_many_preserves_order_and_gaps() {
        let state = RedisSharedState::new(make_test_pool());
        let k1 = fresh_key("m1");
        let k2 = fresh_key("m2");
        let k3 = fresh_key("m3");

        state
            .set_with_ttl(&k1, b"a", Duration::from_secs(60))
            .await
            .unwrap();
        state
            .set_with_ttl(&k3, b"c", Duration::from_secs(60))
            .await
            .unwrap();

        let values = state
            .get_many(&[k1.clone(), k2.clone(), k3.clone()])
            .await
            .unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_deref(), Some(&b"a"[..]));
        assert!(values[1].is_none());
        assert_eq!(values[2].as_deref(), Some(&b"c"[..]));

        state.delete(&k1).await.unwrap();
        state.delete(&k3).await.unwrap();
    }
}

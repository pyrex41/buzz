//! Fixed-window rate limiter over any [`SharedState`] backend.
//!
//! Key layouts come from `buzz_auth::rate_limit` unchanged
//! (`buzz:{community}:ratelimit:{pubkey_hex}:{suffix}`,
//! `buzz:ratelimit:ip:{ip}:conn`), so the Redis backend stays wire-compatible
//! with the pre-trait `RedisRateLimiter`.
//!
//! ⚠️ Fixed windows allow up to 2× burst at boundaries — same caveat as the
//! Redis Lua implementation this generalizes.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use buzz_auth::{
    error::AuthError,
    rate_limit::{ip_rate_limit_key, rate_limit_key, LimitType, RateLimitResult, RateLimiter},
};
use buzz_core::TenantContext;
use buzz_state_api::SharedState;
use nostr::PublicKey;

/// Rate limiter over any [`SharedState`] backend.
///
/// Callers MUST fail closed on `Err` — the window counter is a correctness
/// fence (trait contract in `buzz_auth::rate_limit`).
#[derive(Clone)]
pub struct StateRateLimiter {
    state: Arc<dyn SharedState>,
}

impl StateRateLimiter {
    /// Create a rate limiter over the given shared-state backend.
    pub fn new(state: Arc<dyn SharedState>) -> Self {
        Self { state }
    }

    async fn run(
        &self,
        key: &str,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let (count, remaining) = self
            .state
            .incr_window(key, Duration::from_secs(window_secs))
            .await
            .map_err(|e| AuthError::Internal(format!("rate limit state: {e}")))?;
        let reset_in_secs = remaining.as_secs();
        if count <= limit {
            Ok(RateLimitResult::allowed(count, limit, reset_in_secs))
        } else {
            Ok(RateLimitResult::denied(count, limit, reset_in_secs))
        }
    }
}

impl RateLimiter for StateRateLimiter {
    async fn check_and_increment(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        limit_type: LimitType,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let key = rate_limit_key(ctx, pubkey, &limit_type);
        self.run(&key, window_secs, limit).await
    }

    async fn check_ip_connection(
        &self,
        ip: &IpAddr,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let key = ip_rate_limit_key(ip);
        self.run(&key, window_secs, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inproc::InProcessSharedState;
    use buzz_core::CommunityId;
    use nostr::Keys;
    use uuid::Uuid;

    fn limiter() -> StateRateLimiter {
        StateRateLimiter::new(Arc::new(InProcessSharedState::new()))
    }

    fn ctx(id: u128) -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(id)), "a.example")
    }

    #[tokio::test]
    async fn allows_up_to_limit_then_denies() {
        let limiter = limiter();
        let ctx = ctx(0xaaaa);
        let pubkey = Keys::generate().public_key();

        for i in 1..=3u64 {
            let result = limiter
                .check_and_increment(&ctx, &pubkey, LimitType::Messages, 60, 3)
                .await
                .unwrap();
            assert!(result.allowed, "request {i} within limit must be allowed");
        }
        let result = limiter
            .check_and_increment(&ctx, &pubkey, LimitType::Messages, 60, 3)
            .await
            .unwrap();
        assert!(!result.allowed, "request over limit must be denied");
    }

    #[tokio::test(start_paused = true)]
    async fn window_elapse_resets_the_quota() {
        let limiter = limiter();
        let ctx = ctx(0xaaaa);
        let pubkey = Keys::generate().public_key();

        for _ in 0..3 {
            limiter
                .check_and_increment(&ctx, &pubkey, LimitType::Messages, 60, 3)
                .await
                .unwrap();
        }
        tokio::time::advance(Duration::from_secs(61)).await;
        let result = limiter
            .check_and_increment(&ctx, &pubkey, LimitType::Messages, 60, 3)
            .await
            .unwrap();
        assert!(result.allowed, "fresh window must reset the quota");
    }

    #[tokio::test]
    async fn communities_have_independent_quotas() {
        let limiter = limiter();
        let pubkey = Keys::generate().public_key();

        for _ in 0..3 {
            limiter
                .check_and_increment(&ctx(0xaaaa), &pubkey, LimitType::Messages, 60, 3)
                .await
                .unwrap();
        }
        let result = limiter
            .check_and_increment(&ctx(0xbbbb), &pubkey, LimitType::Messages, 60, 3)
            .await
            .unwrap();
        assert!(
            result.allowed,
            "same pubkey in another community is a fresh quota"
        );
    }

    #[tokio::test]
    async fn ip_connection_limit_is_operator_global() {
        let limiter = limiter();
        let ip: IpAddr = "203.0.113.7".parse().unwrap();

        for _ in 0..2 {
            assert!(
                limiter
                    .check_ip_connection(&ip, 60, 2)
                    .await
                    .unwrap()
                    .allowed
            );
        }
        assert!(
            !limiter
                .check_ip_connection(&ip, 60, 2)
                .await
                .unwrap()
                .allowed
        );
    }
}

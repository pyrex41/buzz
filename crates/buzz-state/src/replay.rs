//! NIP-98 replay seen-set over any [`SharedState`] backend.
//!
//! Key layout comes from `buzz_auth::nip98_replay` unchanged
//! (`buzz:{scope}:nip98:{event_id_hex}`), so the Redis backend stays
//! wire-compatible with the pre-trait `RedisNip98ReplayGuard`.

use std::sync::Arc;
use std::time::Duration;

use buzz_auth::{
    error::AuthError,
    nip98_replay::{
        nip98_replay_key_for_scope, Nip98ReplayGuard, DEFAULT_REPLAY_TTL_SECS, MAX_REPLAY_TTL_SECS,
    },
};
use buzz_state_api::SharedState;
use nostr::EventId;

/// Replay guard over any [`SharedState`] backend.
///
/// Each claim is one atomic `set_nx_with_ttl`; `Ok(true)` is the first claim,
/// `Ok(false)` is a replay the caller MUST reject, and callers MUST fail
/// closed on `Err` (trait contract in `buzz_auth::nip98_replay`).
#[derive(Clone)]
pub struct StateNip98ReplayGuard {
    state: Arc<dyn SharedState>,
}

impl StateNip98ReplayGuard {
    /// Create a replay guard over the given shared-state backend.
    pub fn new(state: Arc<dyn SharedState>) -> Self {
        Self { state }
    }
}

impl Nip98ReplayGuard for StateNip98ReplayGuard {
    fn try_mark_in_scope<'a>(
        &'a self,
        scope: &'a str,
        event_id: &'a EventId,
        ttl_secs: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, AuthError>> + Send + 'a>>
    {
        Box::pin(async move {
            // §5 gate floor + safety ceiling, identical to the pre-trait Redis
            // guard: sub-floor values are lifted (contract permits clamping);
            // above-ceiling values are pushed down (contract REQUIRES it).
            let ttl = ttl_secs.clamp(DEFAULT_REPLAY_TTL_SECS, MAX_REPLAY_TTL_SECS);
            let key = nip98_replay_key_for_scope(scope, event_id);

            self.state
                .set_nx_with_ttl(&key, b"1", Duration::from_secs(ttl))
                .await
                .map_err(|e| {
                    tracing::warn!(
                        scope = %scope,
                        error = %e,
                        "nip98 replay: state backend failed — caller MUST fail closed"
                    );
                    AuthError::Internal(format!("replay state: {e}"))
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inproc::InProcessSharedState;
    use buzz_core::{CommunityId, TenantContext};
    use nostr::{EventBuilder, Keys, Kind};
    use uuid::Uuid;

    fn guard() -> StateNip98ReplayGuard {
        StateNip98ReplayGuard::new(Arc::new(InProcessSharedState::new()))
    }

    fn fresh_ctx() -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::new_v4()), "test.example")
    }

    fn fresh_event_id() -> EventId {
        EventBuilder::new(Kind::HttpAuth, "")
            .sign_with_keys(&Keys::generate())
            .expect("sign")
            .id
    }

    #[tokio::test]
    async fn first_claim_succeeds_replay_fails() {
        let guard = guard();
        let ctx = fresh_ctx();
        let eid = fresh_event_id();

        assert!(guard
            .try_mark(&ctx, &eid, DEFAULT_REPLAY_TTL_SECS)
            .await
            .unwrap());
        assert!(!guard
            .try_mark(&ctx, &eid, DEFAULT_REPLAY_TTL_SECS)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn isolation_between_communities() {
        let guard = guard();
        let ctx_a = fresh_ctx();
        let ctx_b = fresh_ctx();
        let eid = fresh_event_id();

        assert!(guard
            .try_mark(&ctx_a, &eid, DEFAULT_REPLAY_TTL_SECS)
            .await
            .unwrap());
        assert!(guard
            .try_mark(&ctx_b, &eid, DEFAULT_REPLAY_TTL_SECS)
            .await
            .unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn sub_floor_ttl_is_lifted_to_default() {
        let guard = guard();
        let ctx = fresh_ctx();
        let eid = fresh_event_id();

        assert!(guard.try_mark(&ctx, &eid, 30).await.unwrap());
        // 31s later a 30s TTL would have expired; the floor lift keeps the
        // marker alive, so the replay is still rejected.
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(!guard.try_mark(&ctx, &eid, 30).await.unwrap());
    }

    #[tokio::test]
    async fn above_ceiling_ttl_is_clamped_and_succeeds() {
        let guard = guard();
        let ctx = fresh_ctx();
        let eid = fresh_event_id();

        assert!(guard.try_mark(&ctx, &eid, u64::MAX).await.unwrap());
        assert!(!guard.try_mark(&ctx, &eid, u64::MAX).await.unwrap());
    }
}

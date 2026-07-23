//! Presence tracking — online/away status with TTL, over any [`SharedState`].
//!
//! Key layout is byte-for-byte the pre-trait Redis format
//! (`buzz:{community}:presence:{pubkey_hex}`, `EX 90`), so the Redis backend
//! stays wire-compatible with running deployments.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use buzz_core::TenantContext;
use buzz_state_api::{SharedState, StateError, StateResult};
use nostr::PublicKey;

/// 3x the 30s heartbeat — single missed heartbeat won't cause presence flap.
pub const PRESENCE_TTL_SECS: u64 = 90;

/// Returns the state key for the presence entry of `pubkey` under `ctx`.
pub fn presence_key(ctx: &TenantContext, pubkey: &PublicKey) -> String {
    format!("buzz:{}:presence:{}", ctx.community(), pubkey.to_hex())
}

/// Presence store over any [`SharedState`] backend.
#[derive(Clone)]
pub struct PresenceStore {
    state: Arc<dyn SharedState>,
}

impl PresenceStore {
    /// Create a presence store over the given shared-state backend.
    pub fn new(state: Arc<dyn SharedState>) -> Self {
        Self { state }
    }

    /// Sets presence status for `pubkey` with a [`PRESENCE_TTL_SECS`]-second TTL.
    pub async fn set_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        status: &str,
    ) -> StateResult<()> {
        let key = presence_key(ctx, pubkey);
        self.state
            .set_with_ttl(
                &key,
                status.as_bytes(),
                Duration::from_secs(PRESENCE_TTL_SECS),
            )
            .await
    }

    /// Removes the presence entry for `pubkey`. Call on clean disconnect.
    pub async fn clear_presence(&self, ctx: &TenantContext, pubkey: &PublicKey) -> StateResult<()> {
        self.state.delete(&presence_key(ctx, pubkey)).await
    }

    /// Returns the current presence status for `pubkey`, or `None` if not set
    /// or expired.
    pub async fn get_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> StateResult<Option<String>> {
        let value = self.state.get(&presence_key(ctx, pubkey)).await?;
        value.map(decode_status).transpose()
    }

    /// Returns `pubkey_hex → status` for all currently-set keys.
    pub async fn get_presence_bulk(
        &self,
        ctx: &TenantContext,
        pubkeys: &[PublicKey],
    ) -> StateResult<HashMap<String, String>> {
        if pubkeys.is_empty() {
            return Ok(HashMap::new());
        }
        let keys: Vec<String> = pubkeys
            .iter()
            .map(|pubkey| presence_key(ctx, pubkey))
            .collect();
        let values = self.state.get_many(&keys).await?;
        let mut result = HashMap::new();
        for (pk, value) in pubkeys.iter().zip(values) {
            if let Some(bytes) = value {
                result.insert(pk.to_hex(), decode_status(bytes)?);
            }
        }
        Ok(result)
    }
}

fn decode_status(bytes: Vec<u8>) -> StateResult<String> {
    String::from_utf8(bytes)
        .map_err(|e| StateError::InvalidValue(format!("presence status not UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inproc::InProcessSharedState;
    use buzz_core::CommunityId;
    use nostr::Keys;
    use uuid::Uuid;

    fn ctx(id: u128, host: &str) -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(id)), host)
    }

    fn store() -> PresenceStore {
        PresenceStore::new(Arc::new(InProcessSharedState::new()))
    }

    #[test]
    fn presence_key_matches_redis_layout() {
        let pubkey = Keys::generate().public_key();
        let ctx = ctx(0xaaaa, "a.example");
        assert_eq!(
            presence_key(&ctx, &pubkey),
            format!("buzz:{}:presence:{}", ctx.community(), pubkey.to_hex())
        );
    }

    #[tokio::test]
    async fn set_get_clear_round_trip() {
        let store = store();
        let ctx = ctx(0xaaaa, "a.example");
        let pubkey = Keys::generate().public_key();

        assert!(store.get_presence(&ctx, &pubkey).await.unwrap().is_none());
        store.set_presence(&ctx, &pubkey, "online").await.unwrap();
        assert_eq!(
            store.get_presence(&ctx, &pubkey).await.unwrap().as_deref(),
            Some("online")
        );
        store.clear_presence(&ctx, &pubkey).await.unwrap();
        assert!(store.get_presence(&ctx, &pubkey).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn presence_expires_after_ttl() {
        let store = store();
        let ctx = ctx(0xaaaa, "a.example");
        let pubkey = Keys::generate().public_key();

        store.set_presence(&ctx, &pubkey, "online").await.unwrap();
        tokio::time::advance(Duration::from_secs(PRESENCE_TTL_SECS + 1)).await;
        assert!(store.get_presence(&ctx, &pubkey).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn bulk_skips_absent_and_isolates_communities() {
        let store = store();
        let ctx_a = ctx(0xaaaa, "a.example");
        let ctx_b = ctx(0xbbbb, "b.example");
        let pk1 = Keys::generate().public_key();
        let pk2 = Keys::generate().public_key();

        store.set_presence(&ctx_a, &pk1, "online").await.unwrap();
        store.set_presence(&ctx_b, &pk2, "away").await.unwrap();

        let result = store.get_presence_bulk(&ctx_a, &[pk1, pk2]).await.unwrap();
        assert_eq!(
            result.get(&pk1.to_hex()).map(String::as_str),
            Some("online")
        );
        assert!(
            !result.contains_key(&pk2.to_hex()),
            "pk2's presence lives in community B only"
        );
    }
}

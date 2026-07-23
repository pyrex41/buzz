//! Cross-pod control-plane messages: cache invalidation and connection control.
//!
//! These DTOs ride the same pub/sub transport as event fan-out. The community
//! is carried by the `Scoped*` wrappers on the subscribe side, never by the
//! tenant-local operation itself — publish paths take a `TenantContext`.

use buzz_core::{CommunityId, TenantContext};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::topic::BUZZ_PREFIX;

/// Tenant-local topic suffix for cache-invalidation messages.
pub const CACHE_INVALIDATION_SUFFIX: &str = "cache-invalidate";

/// Pattern used by subscribers to receive cache invalidations for all
/// communities this pod may have cached locally.
pub const CACHE_INVALIDATION_PATTERN: &str = "buzz:*:cache-invalidate";

/// Tenant-local topic suffix for connection-control messages.
pub const CONN_CONTROL_SUFFIX: &str = "conn-control";

/// Pattern subscribers use to receive connection-control messages for every
/// community this pod may hold connections for.
pub const CONN_CONTROL_PATTERN: &str = "buzz:*:conn-control";

/// Wire topic for cache-invalidation messages under `ctx`.
pub fn cache_invalidation_channel(ctx: &TenantContext) -> String {
    format!(
        "{BUZZ_PREFIX}:{}:{CACHE_INVALIDATION_SUFFIX}",
        ctx.community()
    )
}

/// Parse a cache-invalidation wire topic into its scoped community id.
pub fn parse_cache_invalidation_channel(channel: &str) -> Option<CommunityId> {
    parse_control_channel(channel, CACHE_INVALIDATION_SUFFIX)
}

/// Wire topic for connection-control messages under `ctx`.
pub fn conn_control_channel(ctx: &TenantContext) -> String {
    format!("{BUZZ_PREFIX}:{}:{CONN_CONTROL_SUFFIX}", ctx.community())
}

/// Parse a connection-control wire topic into its scoped community id.
pub fn parse_conn_control_channel(channel: &str) -> Option<CommunityId> {
    parse_control_channel(channel, CONN_CONTROL_SUFFIX)
}

fn parse_control_channel(channel: &str, suffix: &str) -> Option<CommunityId> {
    let mut parts = channel.split(':');
    if parts.next()? != BUZZ_PREFIX {
        return None;
    }
    let community_id = Uuid::parse_str(parts.next()?).ok()?;
    if parts.next()? != suffix {
        return None;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(CommunityId::from_uuid(community_id))
}

/// A cache-key drop to apply on every pod. Each variant mirrors exactly one of
/// the relay's local `invalidate_*` operations. The community is carried by
/// [`ScopedCacheInvalidation`], not by the tenant-local operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op")]
pub enum CacheInvalidation {
    /// Drop the `(channel_id, pubkey)` membership entry and the user's
    /// accessible-channels entry. Mirrors `invalidate_membership`.
    Membership {
        /// Channel whose membership changed.
        channel_id: Uuid,
        /// Affected member's pubkey bytes.
        pubkey: Vec<u8>,
    },
    /// Drop every user's accessible-channels entry. Mirrors
    /// `invalidate_all_accessible_channels` (e.g. a new open channel).
    AccessibleAll,
    /// Drop the cached visibility for a single channel. Mirrors
    /// `invalidate_channel_visibility` (e.g. an open→private flip).
    Visibility {
        /// Channel whose visibility changed.
        channel_id: Uuid,
    },
    /// Drop all membership / accessible / visibility caches. Mirrors
    /// `invalidate_channel_deleted`.
    ChannelDeleted,
}

/// A cache invalidation received from a community-scoped topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedCacheInvalidation {
    /// Community whose local cache key should be dropped.
    pub community_id: CommunityId,
    /// Tenant-local cache invalidation operation.
    pub invalidation: CacheInvalidation,
}

/// A connection-control command to apply on every pod.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op")]
pub enum ConnControl {
    /// Disconnect every live socket bound to the carrying community.
    DisconnectCommunity,
    /// Disconnect every live connection authenticated as `pubkey` in the
    /// carrying community — live ban enforcement. `pubkey` is 32 raw bytes.
    /// `event_id` and `reason` reproduce the same NIP-01 `OK` frame the origin
    /// pod sent, so a member disconnected on any pod learns why.
    DisconnectPubkey {
        /// Banned member's pubkey bytes.
        pubkey: Vec<u8>,
        /// Id echoed in the closing `OK` frame (the ban event's id on origin).
        event_id: String,
        /// Human-readable close reason for the `OK` frame.
        reason: String,
    },
}

/// A connection-control command received from a community-scoped topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedConnControl {
    /// Community whose connections the command applies to.
    pub community_id: CommunityId,
    /// The tenant-local connection-control command.
    pub command: ConnControl,
}

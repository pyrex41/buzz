//! Backend-neutral event query DTO.

use buzz_core::CommunityId;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Optional filters for an event-store query.
///
/// Backend-neutral: field semantics are defined here; how each backend pushes
/// them into SQL (indexes, JSONB containment, side tables) is its own
/// business.
#[derive(Debug, Clone)]
pub struct EventQuery {
    /// Server-resolved community scope.
    pub community_id: CommunityId,
    /// Restrict results to this channel.
    pub channel_id: Option<Uuid>,
    /// Restrict results to these kind values (stored as `i32`).
    pub kinds: Option<Vec<i32>>,
    /// Restrict results to events from this pubkey.
    pub pubkey: Option<Vec<u8>>,
    /// Return events created at or after this time.
    pub since: Option<DateTime<Utc>>,
    /// Return events created at or before this time.
    pub until: Option<DateTime<Utc>>,
    /// Maximum number of events to return.
    pub limit: Option<i64>,
    /// Number of events to skip (for pagination).
    pub offset: Option<i64>,
    /// Restrict to events with a `p` tag mentioning this hex pubkey.
    /// Backends serve this from an indexed mentions side table.
    pub p_tag_hex: Option<String>,
    /// Restrict to events with this exact `d_tag` value (NIP-33).
    pub d_tag: Option<String>,
    /// Restrict to events with any of these `d_tag` values (multi-value NIP-33 pushdown).
    /// Used when a filter has multiple `#d` values and targets only NIP-33 kinds.
    pub d_tags: Option<Vec<String>>,
    /// Composite keyset cursor: exclude events at or "after" this (created_at, id) pair.
    /// Used with `until` for stable pagination: events where
    /// `created_at < until OR (created_at = until AND id > before_id)`.
    /// When set, `until` must also be set.
    pub before_id: Option<Vec<u8>>,
    /// When true, restricts results to global events (`channel_id IS NULL`).
    /// Use for endpoints that serve non-channel data (e.g. kind:1 notes) to
    /// defensively prevent leaking channel-scoped events if the ingest
    /// invariant (`is_global_only_kind`) ever changes.
    /// Mutually exclusive with `channel_id`.
    pub global_only: bool,
    /// Restrict results to events from any of these pubkeys (multi-author `IN` pushdown).
    pub authors: Option<Vec<Vec<u8>>>,
    /// Restrict results to events with any of these IDs (multi-id `IN` pushdown).
    pub ids: Option<Vec<Vec<u8>>>,
    /// Restrict results to events with an `e` tag referencing any of these event IDs (hex).
    pub e_tags: Option<Vec<String>>,
    /// Restrict results to events in any of these channels, while retaining
    /// channel-less global events. Applied before SQL `LIMIT` so access-filtered
    /// historical pages have exact exhaustion semantics.
    pub channel_ids: Option<Vec<Uuid>>,
    /// Override the default limit clamp (1000). Used by COUNT fallback path
    /// which needs to fetch all matching events for post-filter counting.
    /// When None, the default clamp of 1000 applies.
    pub max_limit: Option<i64>,
}

impl EventQuery {
    /// Construct an unconstrained query inside a server-resolved community.
    ///
    /// `community_id` has no safe default. This keeps call sites concise while
    /// making tenant provenance explicit at construction.
    #[must_use]
    pub const fn for_community(community_id: CommunityId) -> Self {
        Self {
            community_id,
            channel_id: None,
            kinds: None,
            pubkey: None,
            since: None,
            until: None,
            limit: None,
            offset: None,
            p_tag_hex: None,
            d_tag: None,
            d_tags: None,
            before_id: None,
            global_only: false,
            authors: None,
            ids: None,
            e_tags: None,
            channel_ids: None,
            max_limit: None,
        }
    }
}

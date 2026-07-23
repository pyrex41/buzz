//! SQLite arms for Home Feed queries (WP8 — feed).
//!
//! Ports of `crate::feed` (`query_mentions` / `query_needs_action` /
//! `query_activity`), exposed under their `Db`-facade names
//! (`query_feed_mentions`, …) for name-matched dispatch.
//!
//! Semantics mirror the Postgres arm exactly:
//! - mentions/needs-action join the `event_mentions` side table (populated by
//!   every `super::event` insert path) on the composite tenant/event key;
//! - an **empty** accessible-channel list means "community-global events
//!   only", never "all channels";
//! - every query is capped at [`FEED_MAX_LIMIT`] rows and excludes
//!   soft-deleted events.
//!
//! Type conventions per `super::event`: UUIDs bind as TEXT, timestamps as
//! INTEGER unix seconds, pubkeys hex-encoded in `event_mentions.pubkey_hex`.

// TODO(dispatch): remove once the `Db` facade (lib.rs) wires these arms —
// until then nothing outside this module calls them.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use buzz_core::kind::{
    KIND_FORUM_COMMENT, KIND_FORUM_POST, KIND_JOB_PROGRESS, KIND_JOB_REQUEST, KIND_JOB_RESULT,
    KIND_STREAM_MESSAGE, KIND_STREAM_MESSAGE_V2, KIND_STREAM_REMINDER,
    KIND_WORKFLOW_APPROVAL_REQUESTED,
};
use buzz_core::{CommunityId, StoredEvent};

use crate::error::{DbError, Result};
use crate::feed::FEED_MAX_LIMIT;

use super::event::{community_text, datetime_from_secs, parse_uuid_text, uuid_text};

/// Column list shared by every feed subquery that aliases `events` as `e`.
const EVENT_COLS: &str =
    "e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, e.sig, e.received_at, e.channel_id";

/// Column list for queries selecting directly from `events` (no alias).
const EVENT_COLS_UNALIASED: &str =
    "id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id";

/// Reconstruct a [`StoredEvent`] from a standard event-projection row.
///
/// Local mirror of the (private) `super::event::row_to_stored_event` — the
/// feed queries use the identical projection; rows that fail to round-trip
/// into a `nostr::Event` are logged and skipped (`None`), matching the
/// Postgres arm's lenient conversion.
fn row_to_stored_event(row: &SqliteRow) -> Result<Option<StoredEvent>> {
    let id_bytes: Vec<u8> = row.try_get("id")?;
    let pubkey_bytes: Vec<u8> = row.try_get("pubkey")?;
    let created_at_secs: i64 = row.try_get("created_at")?;
    let kind_i64: i64 = row.try_get("kind")?;
    let tags_text: String = row.try_get("tags")?;
    let content: String = row.try_get("content")?;
    let sig_bytes: Vec<u8> = row.try_get("sig")?;
    let received_at_secs: i64 = row.try_get("received_at")?;
    let channel_id: Option<String> = row.try_get("channel_id")?;
    let channel_id = channel_id.as_deref().map(parse_uuid_text).transpose()?;

    let received_at = datetime_from_secs(received_at_secs)?;
    let kind_u16 = u16::try_from(kind_i64)
        .map_err(|_| DbError::InvalidData(format!("kind out of u16 range: {kind_i64}")))?;
    let tags_json: serde_json::Value = serde_json::from_str(&tags_text)?;

    let event_json = serde_json::json!({
        "id": hex::encode(&id_bytes),
        "pubkey": hex::encode(&pubkey_bytes),
        "created_at": created_at_secs,
        "kind": kind_u16,
        "tags": tags_json,
        "content": content,
        "sig": hex::encode(&sig_bytes),
    });

    let event: nostr::Event = match serde_json::from_value(event_json) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("failed to reconstruct feed event from sqlite row: {e}");
            return Ok(None);
        }
    };

    Ok(Some(StoredEvent::with_received_at(
        event,
        received_at,
        channel_id,
        true,
    )))
}

/// Append channel visibility filtering for feed queries.
///
/// Feed reads may include channel-less community-global events plus events in
/// channels the caller can access. An empty accessible-channel list therefore
/// means "global only", never "all channels".
fn push_visible_channel_filter(qb: &mut QueryBuilder<Sqlite>, col: &str, ids: &[Uuid]) {
    if ids.is_empty() {
        qb.push(format!(" AND {col} IS NULL"));
        return;
    }

    qb.push(format!(" AND ({col} IS NULL OR {col} IN ("));
    let mut sep = qb.separated(", ");
    for id in ids {
        sep.push_bind(uuid_text(*id));
    }
    qb.push("))");
}

/// Convert fetched rows into `Vec<StoredEvent>`, skipping any that fail the
/// lenient event reconstruction.
fn collect_stored_events(rows: Vec<SqliteRow>) -> Result<Vec<StoredEvent>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(ev) = row_to_stored_event(&row)? {
            out.push(ev);
        }
    }
    Ok(out)
}

/// Find events that @mention the given pubkey (`["p", pubkey_hex]` tag),
/// restricted to the message/forum kinds and to visible channels. Indexed
/// via the `event_mentions` join. `limit` is capped at [`FEED_MAX_LIMIT`].
pub(crate) async fn query_feed_mentions(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey_bytes: &[u8],
    accessible_channel_ids: &[Uuid],
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<StoredEvent>> {
    let limit = limit.min(FEED_MAX_LIMIT);
    let pubkey_hex = hex::encode(pubkey_bytes);

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
        "SELECT {EVENT_COLS} FROM events e \
         INNER JOIN event_mentions m ON e.community_id = m.community_id AND e.id = m.event_id \
         WHERE e.community_id = "
    ));
    qb.push_bind(community_text(community));
    qb.push(" AND m.community_id = ")
        .push_bind(community_text(community));
    qb.push(" AND m.pubkey_hex = ").push_bind(pubkey_hex);
    qb.push(" AND e.deleted_at IS NULL");
    qb.push(format!(
        " AND e.kind IN ({KIND_STREAM_MESSAGE}, {KIND_STREAM_MESSAGE_V2}, \
         {KIND_FORUM_POST}, {KIND_FORUM_COMMENT})"
    ));
    push_visible_channel_filter(&mut qb, "e.channel_id", accessible_channel_ids);
    if let Some(s) = since {
        qb.push(" AND m.event_created_at >= ")
            .push_bind(s.timestamp());
    }
    qb.push(" ORDER BY m.event_created_at DESC LIMIT ")
        .push_bind(limit);

    let rows = qb.build().fetch_all(pool).await?;
    collect_stored_events(rows)
}

/// Find events that require action from the given pubkey — workflow approval
/// requests and reminders tagged with the user's pubkey — scoped to visible
/// channels so requests from channels the user was removed from never
/// surface. `limit` is capped at [`FEED_MAX_LIMIT`].
pub(crate) async fn query_feed_needs_action(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey_bytes: &[u8],
    accessible_channel_ids: &[Uuid],
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<StoredEvent>> {
    let limit = limit.min(FEED_MAX_LIMIT);
    let pubkey_hex = hex::encode(pubkey_bytes);

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
        "SELECT {EVENT_COLS} FROM events e \
         INNER JOIN event_mentions m ON e.community_id = m.community_id AND e.id = m.event_id \
         WHERE e.community_id = "
    ));
    qb.push_bind(community_text(community));
    qb.push(" AND m.community_id = ")
        .push_bind(community_text(community));
    qb.push(" AND m.pubkey_hex = ").push_bind(pubkey_hex);
    qb.push(" AND e.deleted_at IS NULL");
    qb.push(format!(
        " AND e.kind IN ({KIND_WORKFLOW_APPROVAL_REQUESTED}, {KIND_STREAM_REMINDER})"
    ));
    push_visible_channel_filter(&mut qb, "e.channel_id", accessible_channel_ids);
    if let Some(s) = since {
        qb.push(" AND m.event_created_at >= ")
            .push_bind(s.timestamp());
    }
    qb.push(" ORDER BY m.event_created_at DESC LIMIT ")
        .push_bind(limit);

    let rows = qb.build().fetch_all(pool).await?;
    collect_stored_events(rows)
}

/// Find recent activity across accessible channels — stream messages, forum
/// posts, and agent job events (workflow execution kinds intentionally
/// excluded). `limit` is capped at [`FEED_MAX_LIMIT`].
pub(crate) async fn query_feed_activity(
    pool: &SqlitePool,
    community: CommunityId,
    accessible_channel_ids: &[Uuid],
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<StoredEvent>> {
    let limit = limit.min(FEED_MAX_LIMIT);

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
        "SELECT {EVENT_COLS_UNALIASED} FROM events WHERE community_id = "
    ));
    qb.push_bind(community_text(community));
    qb.push(" AND deleted_at IS NULL");
    qb.push(format!(
        " AND kind IN ({KIND_STREAM_MESSAGE}, {KIND_STREAM_MESSAGE_V2}, {KIND_FORUM_POST}, \
         {KIND_JOB_REQUEST}, {KIND_JOB_PROGRESS}, {KIND_JOB_RESULT})"
    ));
    push_visible_channel_filter(&mut qb, "channel_id", accessible_channel_ids);
    if let Some(s) = since {
        qb.push(" AND created_at >= ").push_bind(s.timestamp());
    }
    qb.push(" ORDER BY created_at DESC LIMIT ").push_bind(limit);

    let rows = qb.build().fetch_all(pool).await?;
    collect_stored_events(rows)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr as _;

    async fn test_pool() -> SqlitePool {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .expect("options")
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn make_community(pool: &SqlitePool) -> CommunityId {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.to_string())
            .bind(format!("feed-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    /// Insert an event through the WP1 insert path, which also populates the
    /// `event_mentions` side table.
    async fn store_feed_event(
        pool: &SqlitePool,
        community: CommunityId,
        kind: u32,
        content: &str,
        channel_id: Option<Uuid>,
        tags: Vec<Tag>,
    ) -> nostr::Event {
        let keys = Keys::generate();
        let event = EventBuilder::new(Kind::Custom(kind as u16), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .expect("sign event");
        super::super::event::insert_event(pool, community, &event, channel_id)
            .await
            .expect("insert feed event");
        event
    }

    #[tokio::test]
    async fn mentions_are_scoped_to_pubkey_visibility_and_community() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let channel_visible = Uuid::new_v4();
        let channel_hidden = Uuid::new_v4();
        let mentioned_hex = "02".repeat(32);
        let mentioned_bytes = hex::decode(&mentioned_hex).expect("hex pubkey");
        let p_tag = || vec![Tag::parse(["p", mentioned_hex.as_str()]).expect("p tag")];

        let visible = store_feed_event(
            &pool,
            community_a,
            KIND_STREAM_MESSAGE,
            "visible mention",
            Some(channel_visible),
            p_tag(),
        )
        .await;
        let global = store_feed_event(
            &pool,
            community_a,
            KIND_STREAM_MESSAGE,
            "global mention",
            None,
            p_tag(),
        )
        .await;
        let hidden = store_feed_event(
            &pool,
            community_a,
            KIND_STREAM_MESSAGE,
            "hidden mention",
            Some(channel_hidden),
            p_tag(),
        )
        .await;
        let cross_tenant = store_feed_event(
            &pool,
            community_b,
            KIND_STREAM_MESSAGE,
            "other community",
            Some(channel_visible),
            p_tag(),
        )
        .await;
        // A message without the p tag never surfaces as a mention.
        store_feed_event(
            &pool,
            community_a,
            KIND_STREAM_MESSAGE,
            "no mention",
            Some(channel_visible),
            vec![],
        )
        .await;

        let rows = query_feed_mentions(
            &pool,
            community_a,
            &mentioned_bytes,
            &[channel_visible],
            None,
            10,
        )
        .await
        .expect("query mentions");
        let ids: Vec<_> = rows.iter().map(|r| r.event.id).collect();
        assert!(ids.contains(&visible.id));
        assert!(ids.contains(&global.id), "global events are always visible");
        assert!(!ids.contains(&hidden.id), "inaccessible channel excluded");
        assert!(!ids.contains(&cross_tenant.id), "tenant isolation");
        assert_eq!(ids.len(), 2);

        // Empty accessible-channel list means global-only.
        let global_only = query_feed_mentions(&pool, community_a, &mentioned_bytes, &[], None, 10)
            .await
            .expect("global only");
        let ids: Vec<_> = global_only.iter().map(|r| r.event.id).collect();
        assert_eq!(ids, vec![global.id]);
    }

    #[tokio::test]
    async fn needs_action_returns_only_action_kinds_for_the_pubkey() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let channel = Uuid::new_v4();
        let actor_hex = "03".repeat(32);
        let actor_bytes = hex::decode(&actor_hex).expect("hex pubkey");
        let p_tag = || vec![Tag::parse(["p", actor_hex.as_str()]).expect("p tag")];

        let approval = store_feed_event(
            &pool,
            community,
            KIND_WORKFLOW_APPROVAL_REQUESTED,
            "approve me",
            Some(channel),
            p_tag(),
        )
        .await;
        let reminder = store_feed_event(
            &pool,
            community,
            KIND_STREAM_REMINDER,
            "remember",
            Some(channel),
            p_tag(),
        )
        .await;
        // A plain mention is not a needs-action item.
        let chatter = store_feed_event(
            &pool,
            community,
            KIND_STREAM_MESSAGE,
            "just chatting",
            Some(channel),
            p_tag(),
        )
        .await;

        let rows = query_feed_needs_action(&pool, community, &actor_bytes, &[channel], None, 10)
            .await
            .expect("query needs action");
        let ids: Vec<_> = rows.iter().map(|r| r.event.id).collect();
        assert!(ids.contains(&approval.id));
        assert!(ids.contains(&reminder.id));
        assert!(!ids.contains(&chatter.id));
        assert_eq!(ids.len(), 2);
    }

    #[tokio::test]
    async fn activity_is_channel_scoped_and_respects_since() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let channel = Uuid::new_v4();
        let other_channel = Uuid::new_v4();

        let in_channel = store_feed_event(
            &pool,
            community,
            KIND_STREAM_MESSAGE,
            "in channel",
            Some(channel),
            vec![],
        )
        .await;
        let global = store_feed_event(
            &pool,
            community,
            KIND_FORUM_POST,
            "global post",
            None,
            vec![],
        )
        .await;
        let elsewhere = store_feed_event(
            &pool,
            community,
            KIND_STREAM_MESSAGE,
            "elsewhere",
            Some(other_channel),
            vec![],
        )
        .await;

        let rows = query_feed_activity(&pool, community, &[channel], None, 10)
            .await
            .expect("query activity");
        let ids: Vec<_> = rows.iter().map(|r| r.event.id).collect();
        assert!(ids.contains(&in_channel.id));
        assert!(ids.contains(&global.id));
        assert!(!ids.contains(&elsewhere.id));

        // A `since` cursor in the future filters everything out.
        let future = Utc::now() + chrono::Duration::hours(1);
        assert!(
            query_feed_activity(&pool, community, &[channel], Some(future), 10)
                .await
                .expect("future since")
                .is_empty()
        );
    }
}

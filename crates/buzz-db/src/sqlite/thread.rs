//! SQLite arms for thread metadata reads (WP4 — threads).
//!
//! Function-for-function port of the Solo-reachable operations in
//! [`crate::thread`]: `get_thread_replies`, `get_thread_summary`,
//! `get_channel_window`, `get_thread_metadata_by_event`. Semantics match the
//! Postgres arm:
//!
//! - every predicate leads with `community_id` (tenant scoping),
//! - soft-deleted events are invisible (`e.deleted_at IS NULL`),
//! - composite keyset pagination with the event-id tiebreak (same-second
//!   replies must page without gaps or duplicates),
//! - the channel window's `has_more` comes from an internal `limit + 1`
//!   probe; the sentinel row never leaves this module,
//! - rows that fail event reconstruction are skipped, not fatal.
//!
//! The replica-fence routing that wraps these reads in the `Db` facade is
//! Postgres-only plumbing; on SQLite there is a single pool, so each function
//! is one straight query path.
//!
//! **Not ported** (zero production call sites,
//! `docs/phase2/db-callsite-inventory.md` §2): `insert_thread_metadata` and
//! `decrement_reply_count`. The production counter paths live in
//! `crate::sqlite::event` — increments inside
//! `insert_event_with_thread_metadata`, decrements inside
//! `soft_delete_event_and_update_thread` — and are exercised by the tests in
//! this file.
//!
//! Divergences from the Postgres SQL (documented, semantics-preserving):
//! joins between `thread_metadata` and `events` drop the redundant
//! `created_at` leg (the SQLite `events` PK is `(community_id, id)`); row
//! comparisons `(a, b) > (x, y)` are expanded into explicit OR form; the
//! per-root participant 10-cap in `get_channel_window` is applied in Rust
//! instead of a `ROW_NUMBER()` window over grouped aggregates.

// TODO(orchestrator): drop this allow when `Db` dispatch wires these arms.

use chrono::{DateTime, Utc};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use crate::error::{DbError, Result};
use crate::sqlite::channel::opt_datetime_from_unix;
use crate::sqlite::event::{community_text, datetime_from_secs, parse_uuid_text, uuid_text};
use crate::thread::{
    ChannelWindow, ChannelWindowRow, ThreadMetadataRecord, ThreadReply, ThreadSummary,
};

/// Per-thread participant cap (parity with the Postgres arm's `LIMIT 10`).
const PARTICIPANT_CAP: usize = 10;

/// Reconstruct a [`StoredEvent`] from a standard event-projection row
/// (`id, pubkey, created_at, kind, tags, content, sig, received_at,
/// channel_id`). Local mirror of the private helper in
/// `crate::sqlite::event`; rows that fail to round-trip into a
/// `nostr::Event` are logged and skipped (`None`) — same skip-and-continue
/// contract as `crate::event::row_to_stored_event`.
fn row_to_stored_event(row: &sqlx::sqlite::SqliteRow) -> Result<Option<StoredEvent>> {
    let id_bytes: Vec<u8> = row.try_get("id")?;
    let pubkey_bytes: Vec<u8> = row.try_get("pubkey")?;
    let created_at_secs: i64 = row.try_get("created_at")?;
    let kind_i64: i64 = row.try_get("kind")?;
    let tags_text: String = row.try_get("tags")?;
    let content: String = row.try_get("content")?;
    let sig_bytes: Vec<u8> = row.try_get("sig")?;
    let received_at_secs: i64 = row.try_get("received_at")?;
    let channel_raw: Option<String> = row.try_get("channel_id")?;
    let channel_id = channel_raw.as_deref().map(parse_uuid_text).transpose()?;

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
            tracing::warn!("failed to reconstruct event from sqlite row: {e}");
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

/// Fetch all replies under a root event, ordered `(event_created_at ASC,
/// event_id ASC)`.
///
/// - `depth_limit` — if `Some(n)`, only replies at depth <= n.
/// - `cursor` — keyset cursor: 8-byte big-endian i64 seconds followed by the
///   raw event id of the last row already seen (composite tiebreak). A bare
///   8-byte cursor is legacy timestamp-only paging. Wire format identical to
///   the Postgres arm.
/// - `limit` — maximum rows returned.
pub(crate) async fn get_thread_replies(
    pool: &SqlitePool,
    community_id: CommunityId,
    root_event_id: &[u8],
    depth_limit: Option<u32>,
    limit: u32,
    cursor: Option<&[u8]>,
) -> Result<Vec<ThreadReply>> {
    // Decode cursor bytes → (seconds, optional event_id). An out-of-range
    // timestamp invalidates the cursor entirely (parity with the Postgres
    // arm's DateTime::from_timestamp gate).
    let cursor_key: Option<(i64, Option<Vec<u8>>)> = cursor.and_then(|bytes| {
        let secs_bytes: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
        let secs = i64::from_be_bytes(secs_bytes);
        DateTime::from_timestamp(secs, 0)?;
        let id = (bytes.len() > 8).then(|| bytes[8..].to_vec());
        Some((secs, id))
    });

    let mut sql = String::from(
        "SELECT \
            tm.event_id, \
            e.id, \
            tm.parent_event_id, \
            tm.root_event_id, \
            tm.channel_id, \
            e.pubkey, \
            e.created_at, \
            e.tags, \
            e.content, \
            e.kind, \
            e.sig, \
            e.received_at, \
            tm.depth, \
            tm.event_created_at, \
            tm.broadcast \
         FROM thread_metadata tm \
         JOIN events e \
            ON e.community_id = tm.community_id \
           AND e.id           = tm.event_id \
         WHERE tm.community_id = ? \
           AND tm.root_event_id = ? \
           AND e.deleted_at IS NULL",
    );

    if depth_limit.is_some() {
        sql.push_str(" AND tm.depth <= ?");
    }
    match &cursor_key {
        Some((_, Some(_))) => {
            // Composite keyset with the event-id tiebreak, expanded (the
            // Postgres row-value comparison spelled out as OR form).
            sql.push_str(
                " AND (tm.event_created_at > ? \
                   OR (tm.event_created_at = ? AND tm.event_id > ?))",
            );
        }
        Some((_, None)) => {
            // Legacy timestamp-only cursor (no tiebreak).
            sql.push_str(" AND tm.event_created_at > ?");
        }
        None => {}
    }
    sql.push_str(" ORDER BY tm.event_created_at ASC, tm.event_id ASC LIMIT ?");

    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community_id))
        .bind(root_event_id);
    if let Some(dl) = depth_limit {
        q = q.bind(dl as i32);
    }
    match &cursor_key {
        Some((secs, Some(id))) => {
            q = q.bind(*secs).bind(*secs).bind(id.clone());
        }
        Some((secs, None)) => {
            q = q.bind(*secs);
        }
        None => {}
    }
    q = q.bind(limit as i64);

    let rows = q.fetch_all(pool).await?;

    let mut replies = Vec::with_capacity(rows.len());
    for row in rows {
        let event_id: Vec<u8> = row.try_get("event_id")?;
        let parent_event_id: Option<Vec<u8>> = row.try_get("parent_event_id")?;
        let root_event_id_col: Option<Vec<u8>> = row.try_get("root_event_id")?;
        let channel_raw: String = row.try_get("channel_id")?;
        let channel_id: Uuid = parse_uuid_text(&channel_raw)?;
        let pubkey: Vec<u8> = row.try_get("pubkey")?;
        let tags_raw: String = row.try_get("tags")?;
        let tags: serde_json::Value = serde_json::from_str(&tags_raw)?;
        let depth: i32 = row.try_get("depth")?;
        let created_at = datetime_from_secs(row.try_get::<i64, _>("event_created_at")?)?;
        let broadcast: bool = row.try_get("broadcast")?;

        // Skip rows that fail event reconstruction rather than failing the
        // whole thread query.
        let stored_event = match row_to_stored_event(&row)? {
            Some(se) => se,
            None => continue,
        };

        replies.push(ThreadReply {
            event_id,
            parent_event_id,
            root_event_id: root_event_id_col,
            channel_id,
            pubkey,
            tags,
            content: stored_event.event.content.clone(),
            stored_event,
            depth,
            created_at,
            broadcast,
        });
    }

    Ok(replies)
}

/// Fetch aggregated thread stats for a single event, plus up to 10
/// participant pubkeys (most recent first).
pub(crate) async fn get_thread_summary(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
) -> Result<Option<ThreadSummary>> {
    let community = community_text(community_id);
    let row = sqlx::query(
        "SELECT reply_count, descendant_count, last_reply_at \
         FROM thread_metadata \
         WHERE community_id = ?1 AND event_id = ?2 \
         LIMIT 1",
    )
    .bind(&community)
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let row = match row {
        Some(r) => r,
        None => return Ok(None),
    };

    let reply_count: i32 = row.try_get("reply_count")?;
    let descendant_count: i32 = row.try_get("descendant_count")?;
    let last_reply_at = opt_datetime_from_unix(row.try_get("last_reply_at")?)?;

    // Distinct participant pubkeys from the thread, most recent first.
    let participant_rows = sqlx::query(
        "SELECT pubkey FROM ( \
            SELECT e.pubkey AS pubkey, MAX(e.created_at) AS last_seen \
            FROM thread_metadata tm \
            JOIN events e \
               ON e.community_id = tm.community_id \
              AND e.id           = tm.event_id \
            WHERE tm.community_id = ?1 \
              AND tm.root_event_id = ?2 \
              AND e.deleted_at IS NULL \
            GROUP BY e.pubkey \
         ) ORDER BY last_seen DESC LIMIT ?3",
    )
    .bind(&community)
    .bind(event_id)
    .bind(PARTICIPANT_CAP as i64)
    .fetch_all(pool)
    .await?;

    let participants: Vec<Vec<u8>> = participant_rows
        .into_iter()
        .map(|r| r.try_get::<Vec<u8>, _>("pubkey"))
        .collect::<std::result::Result<_, _>>()?;

    Ok(Some(ThreadSummary {
        reply_count,
        descendant_count,
        last_reply_at,
        participants,
    }))
}

/// Fetch one channel window: top-level rows (depth 0, missing metadata, or
/// broadcast depth-1 replies) in `(created_at DESC, id ASC)` keyset order,
/// with thread summaries joined in, plus the server-side `has_more` fact
/// from an internal `limit + 1` probe (the sentinel row is dropped here and
/// never reaches the wire).
///
/// `cursor` is the composite `(created_at, id)` of the last retained row
/// from the previous page; `None` = head of the channel.
pub(crate) async fn get_channel_window(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    limit: u32,
    cursor: Option<(DateTime<Utc>, Vec<u8>)>,
    kind_filter: Option<&[u32]>,
) -> Result<ChannelWindow> {
    let community = community_text(community_id);

    let mut sql = String::from(
        "SELECT \
            e.id, \
            e.pubkey, \
            e.created_at, \
            e.kind, \
            e.tags, \
            e.content, \
            e.sig, \
            e.received_at, \
            e.channel_id, \
            tm.reply_count, \
            tm.descendant_count, \
            tm.last_reply_at \
         FROM events e \
         LEFT JOIN thread_metadata tm \
            ON tm.community_id = e.community_id \
           AND tm.event_id     = e.id \
         WHERE e.community_id = ? \
           AND e.channel_id = ? \
           AND e.deleted_at IS NULL \
           AND ( \
                 tm.depth IS NULL \
              OR tm.depth = 0 \
              OR (tm.depth = 1 AND tm.broadcast = 1) \
           )",
    );

    if cursor.is_some() {
        // Composite keyset: with ORDER BY created_at DESC, id ASC, the page
        // after (ts, id) is created_at < ts OR (created_at = ts AND id > id).
        sql.push_str(" AND (e.created_at < ? OR (e.created_at = ? AND e.id > ?))");
    }

    if let Some(kinds) = kind_filter {
        if !kinds.is_empty() {
            let list = kinds
                .iter()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(&format!(" AND e.kind IN ({list})"));
        }
    }

    sql.push_str(" ORDER BY e.created_at DESC, e.id ASC LIMIT ?");

    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(&community)
        .bind(uuid_text(channel_id));
    if let Some((ts, id)) = &cursor {
        let secs = ts.timestamp();
        q = q.bind(secs).bind(secs).bind(id.clone());
    }
    // The +1 probe row is the server-internal has_more evidence.
    q = q.bind(limit as i64 + 1);

    let mut db_rows = q.fetch_all(pool).await?;

    let has_more = db_rows.len() > limit as usize;
    db_rows.truncate(limit as usize);

    // Scan position of this page: the (created_at, id) of the last retained
    // raw row, captured before reconstruction so skip-and-continue rows
    // cannot stall the cursor. Only meaningful when more rows exist past it.
    let next_cursor = if has_more {
        match db_rows.last() {
            Some(row) => Some((
                datetime_from_secs(row.try_get::<i64, _>("created_at")?)?,
                row.try_get::<Vec<u8>, _>("id")?,
            )),
            None => None,
        }
    } else {
        None
    };

    let mut rows = Vec::with_capacity(db_rows.len());
    for row in db_rows {
        let reply_count: Option<i32> = row.try_get("reply_count")?;
        let descendant_count: Option<i32> = row.try_get("descendant_count")?;
        let last_reply_at = opt_datetime_from_unix(row.try_get("last_reply_at")?)?;

        // Skip rows that fail event reconstruction rather than failing the
        // window (parity with get_thread_replies).
        let stored_event = match row_to_stored_event(&row)? {
            Some(se) => se,
            None => continue,
        };

        let thread_summary = match reply_count {
            Some(rc) if rc > 0 => Some(ThreadSummary {
                reply_count: rc,
                descendant_count: descendant_count.unwrap_or(0),
                last_reply_at,
                participants: Vec::new(), // batch-filled below
            }),
            _ => None,
        };

        rows.push(ChannelWindowRow {
            stored_event,
            thread_summary,
        });
    }

    // Batch participants for every row with thread activity — one query for
    // the whole window. Same shape and 10-cap as get_thread_summary; the
    // per-root cap is applied in Rust while draining the ordered rows
    // (instead of the Postgres ROW_NUMBER() window).
    let roots: Vec<Vec<u8>> = rows
        .iter()
        .filter(|r| r.thread_summary.is_some())
        .map(|r| r.stored_event.event.id.as_bytes().to_vec())
        .collect();
    if !roots.is_empty() {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "SELECT tm.root_event_id AS root_event_id, e.pubkey AS pubkey, \
                    MAX(e.created_at) AS last_seen \
             FROM thread_metadata tm \
             JOIN events e \
                ON e.community_id = tm.community_id \
               AND e.id           = tm.event_id \
             WHERE tm.community_id = ",
        );
        qb.push_bind(&community);
        qb.push(" AND e.deleted_at IS NULL AND tm.root_event_id IN (");
        let mut sep = qb.separated(", ");
        for root in &roots {
            sep.push_bind(root.clone());
        }
        qb.push(") GROUP BY tm.root_event_id, e.pubkey ORDER BY tm.root_event_id, last_seen DESC");

        let participant_rows = qb.build().fetch_all(pool).await?;

        let mut by_root: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
            std::collections::HashMap::new();
        for row in participant_rows {
            let root: Vec<u8> = row.try_get("root_event_id")?;
            let pubkey: Vec<u8> = row.try_get("pubkey")?;
            let entry = by_root.entry(root).or_default();
            if entry.len() < PARTICIPANT_CAP {
                entry.push(pubkey);
            }
        }
        for row in &mut rows {
            if let Some(summary) = &mut row.thread_summary {
                if let Some(p) = by_root.remove(row.stored_event.event.id.as_bytes().as_slice()) {
                    summary.participants = p;
                }
            }
        }
    }

    Ok(ChannelWindow {
        rows,
        has_more,
        next_cursor,
    })
}

/// Look up a single `thread_metadata` row by event id.
///
/// Used when processing soft-deletes to find the parent/root so reply
/// counters can be decremented
/// (`crate::sqlite::event::soft_delete_event_and_update_thread`).
pub(crate) async fn get_thread_metadata_by_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
) -> Result<Option<ThreadMetadataRecord>> {
    let row = sqlx::query(
        "SELECT \
            event_id, \
            event_created_at, \
            channel_id, \
            parent_event_id, \
            root_event_id, \
            depth, \
            reply_count, \
            descendant_count, \
            broadcast \
         FROM thread_metadata \
         WHERE community_id = ?1 AND event_id = ?2 \
         LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let row = match row {
        Some(r) => r,
        None => return Ok(None),
    };

    let channel_raw: String = row.try_get("channel_id")?;
    Ok(Some(ThreadMetadataRecord {
        event_id: row.try_get("event_id")?,
        event_created_at: datetime_from_secs(row.try_get::<i64, _>("event_created_at")?)?,
        channel_id: parse_uuid_text(&channel_raw)?,
        parent_event_id: row.try_get("parent_event_id")?,
        root_event_id: row.try_get("root_event_id")?,
        depth: row.try_get("depth")?,
        reply_count: row.try_get("reply_count")?,
        descendant_count: row.try_get("descendant_count")?,
        broadcast: row.try_get("broadcast")?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ThreadMetadataParams;
    use crate::sqlite::event::{
        insert_event_with_thread_metadata, soft_delete_event_and_update_thread,
    };
    use nostr::{EventBuilder, Keys, Kind, Timestamp};

    async fn setup_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn make_community(pool: &SqlitePool) -> CommunityId {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.to_string())
            .bind(format!("thread-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    async fn make_channel(pool: &SqlitePool, community: CommunityId) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO channels (id, community_id, name, created_by) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(id.to_string())
        .bind(community.as_uuid().to_string())
        .bind(format!("chan-{}", id.simple()))
        .bind(vec![7u8; 32])
        .execute(pool)
        .await
        .expect("insert channel");
        id
    }

    fn make_event(keys: &Keys, content: &str, created_at: u64) -> nostr::Event {
        EventBuilder::new(Kind::Custom(9), content)
            .custom_created_at(Timestamp::from(created_at))
            .sign_with_keys(keys)
            .expect("sign event")
    }

    fn event_ts(event: &nostr::Event) -> DateTime<Utc> {
        datetime_from_secs(event.created_at.as_secs() as i64).expect("valid ts")
    }

    /// Insert a reply through the production path
    /// (insert_event_with_thread_metadata).
    #[allow(clippy::too_many_arguments)]
    async fn insert_reply(
        pool: &SqlitePool,
        community: CommunityId,
        channel: Uuid,
        reply: &nostr::Event,
        parent: &nostr::Event,
        root: &nostr::Event,
        depth: i32,
        broadcast: bool,
    ) {
        insert_event_with_thread_metadata(
            pool,
            community,
            reply,
            Some(channel),
            Some(ThreadMetadataParams {
                event_id: reply.id.as_bytes(),
                event_created_at: event_ts(reply),
                channel_id: channel,
                parent_event_id: Some(parent.id.as_bytes()),
                parent_event_created_at: Some(event_ts(parent)),
                root_event_id: Some(root.id.as_bytes()),
                root_event_created_at: Some(event_ts(root)),
                depth,
                broadcast,
            }),
        )
        .await
        .expect("insert reply");
    }

    /// Counter materialization across the full insert/delete lifecycle:
    /// increments on reply insert (parent reply_count, root
    /// descendant_count), no double-count on duplicate insert, decrements on
    /// soft-delete, floors at zero, and no double-decrement on repeated
    /// delete.
    #[tokio::test]
    async fn thread_counters_increment_and_decrement_across_insert_and_delete() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let channel = make_channel(&pool, community).await;
        let keys = Keys::generate();
        let base = 1_800_000_000u64;

        // Root inserted with no metadata (production shape for a plain
        // top-level message — the stub row is created by the first reply).
        let root = make_event(&keys, "root", base);
        insert_event_with_thread_metadata(&pool, community, &root, Some(channel), None)
            .await
            .expect("insert root");

        // Depth-1 reply (parent == root).
        let child = make_event(&keys, "child", base + 1);
        insert_reply(&pool, community, channel, &child, &root, &root, 1, false).await;

        // Depth-2 reply (parent == child, root == root) — nested branch.
        let grandchild = make_event(&keys, "grandchild", base + 2);
        insert_reply(
            &pool,
            community,
            channel,
            &grandchild,
            &child,
            &root,
            2,
            false,
        )
        .await;

        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("root stub row exists");
        assert_eq!(root_meta.reply_count, 1, "one direct reply to the root");
        assert_eq!(
            root_meta.descendant_count, 2,
            "both replies count as root descendants"
        );

        let child_meta = get_thread_metadata_by_event(&pool, community, child.id.as_bytes())
            .await
            .expect("child meta")
            .expect("child row exists");
        assert_eq!(child_meta.reply_count, 1, "grandchild is child's reply");
        assert_eq!(child_meta.depth, 1);

        // Duplicate insert of the grandchild must not double-count.
        insert_reply(
            &pool,
            community,
            channel,
            &grandchild,
            &child,
            &root,
            2,
            false,
        )
        .await;
        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("row");
        assert_eq!(root_meta.descendant_count, 2, "duplicate must not count");

        // Summary reflects the counters + participants + last_reply_at.
        let summary = get_thread_summary(&pool, community, root.id.as_bytes())
            .await
            .expect("summary")
            .expect("summary exists");
        assert_eq!(summary.reply_count, 1);
        assert_eq!(summary.descendant_count, 2);
        assert!(summary.last_reply_at.is_some());
        assert!(summary
            .participants
            .contains(&keys.public_key().to_bytes().to_vec()));

        // Delete the grandchild: child reply_count 1→0, root descendant 2→1.
        let deleted = soft_delete_event_and_update_thread(
            &pool,
            community,
            grandchild.id.as_bytes(),
            Some(child.id.as_bytes()),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("delete grandchild");
        assert!(deleted);

        let child_meta = get_thread_metadata_by_event(&pool, community, child.id.as_bytes())
            .await
            .expect("child meta")
            .expect("row");
        assert_eq!(child_meta.reply_count, 0);
        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("row");
        assert_eq!(root_meta.descendant_count, 1);
        assert_eq!(root_meta.reply_count, 1, "direct reply still live");

        // Repeating the delete is a no-op (no double-decrement).
        let deleted_again = soft_delete_event_and_update_thread(
            &pool,
            community,
            grandchild.id.as_bytes(),
            Some(child.id.as_bytes()),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("re-delete grandchild");
        assert!(!deleted_again);
        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("row");
        assert_eq!(root_meta.descendant_count, 1, "no double decrement");

        // Delete the child: root reply_count 1→0, descendant 1→0.
        assert!(soft_delete_event_and_update_thread(
            &pool,
            community,
            child.id.as_bytes(),
            Some(root.id.as_bytes()),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("delete child"));
        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("row");
        assert_eq!(root_meta.reply_count, 0);
        assert_eq!(root_meta.descendant_count, 0);

        // Force a decrement past zero via a fresh event deletion pointing at
        // the drained root — counters must floor at 0, never go negative.
        let stray = make_event(&keys, "stray", base + 3);
        insert_event_with_thread_metadata(&pool, community, &stray, Some(channel), None)
            .await
            .expect("insert stray");
        assert!(soft_delete_event_and_update_thread(
            &pool,
            community,
            stray.id.as_bytes(),
            Some(root.id.as_bytes()),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("delete stray"));
        let root_meta = get_thread_metadata_by_event(&pool, community, root.id.as_bytes())
            .await
            .expect("root meta")
            .expect("row");
        assert_eq!(root_meta.reply_count, 0, "floored at zero");
        assert_eq!(root_meta.descendant_count, 0, "floored at zero");
    }

    /// Replies read back in order, deleted replies are invisible, the depth
    /// filter applies, and composite-cursor pagination over same-second ties
    /// loses nothing and duplicates nothing.
    #[tokio::test]
    async fn thread_replies_read_depth_filter_and_tied_cursor_pagination() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let channel = make_channel(&pool, community).await;
        let keys = Keys::generate();
        let base = 1_800_000_100u64;

        let root = make_event(&keys, "root", base);
        insert_event_with_thread_metadata(&pool, community, &root, Some(channel), None)
            .await
            .expect("insert root");

        // Five same-second depth-1 replies — pagination must lean entirely
        // on the event-id tiebreak.
        let mut expected_ids: Vec<Vec<u8>> = Vec::new();
        for i in 0..5 {
            let reply = make_event(&keys, &format!("tie-{i}"), base + 1);
            expected_ids.push(reply.id.as_bytes().to_vec());
            insert_reply(&pool, community, channel, &reply, &root, &root, 1, false).await;
        }
        // One deeper reply, excluded by the depth filter below.
        let deep_parent = expected_ids[0].clone();
        let deep = make_event(&keys, "deep", base + 2);
        insert_event_with_thread_metadata(
            &pool,
            community,
            &deep,
            Some(channel),
            Some(ThreadMetadataParams {
                event_id: deep.id.as_bytes(),
                event_created_at: event_ts(&deep),
                channel_id: channel,
                parent_event_id: Some(&deep_parent),
                parent_event_created_at: Some(datetime_from_secs(base as i64 + 1).expect("ts")),
                root_event_id: Some(root.id.as_bytes()),
                root_event_created_at: Some(event_ts(&root)),
                depth: 2,
                broadcast: false,
            }),
        )
        .await
        .expect("insert deep reply");

        // Depth filter: only the five depth-1 replies.
        let depth1 = get_thread_replies(&pool, community, root.id.as_bytes(), Some(1), 100, None)
            .await
            .expect("depth-1 read");
        assert_eq!(depth1.len(), 5);
        assert!(depth1.iter().all(|r| r.depth == 1));
        assert!(depth1.iter().all(|r| r.channel_id == channel));

        // Full subtree read reaches the depth-2 reply.
        let subtree = get_thread_replies(&pool, community, root.id.as_bytes(), Some(64), 100, None)
            .await
            .expect("subtree read");
        assert_eq!(subtree.len(), 6);
        assert!(subtree.iter().any(|r| r.depth == 2));

        // Composite-cursor paging over the tied second: page size 2.
        let mut collected: Vec<Vec<u8>> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = get_thread_replies(
                &pool,
                community,
                root.id.as_bytes(),
                Some(1),
                2,
                cursor.as_deref(),
            )
            .await
            .expect("page");
            if page.is_empty() {
                break;
            }
            let last = page.last().expect("non-empty page");
            let mut next = last.created_at.timestamp().to_be_bytes().to_vec();
            next.extend_from_slice(&last.event_id);
            cursor = Some(next);
            let full = page.len() == 2;
            for reply in page {
                collected.push(reply.event_id);
            }
            if !full {
                break;
            }
        }
        assert_eq!(collected.len(), 5, "no rows lost across tied pages");
        let mut unique = collected.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 5, "no duplicates across tied pages");
        let mut expected_sorted = expected_ids.clone();
        expected_sorted.sort();
        assert_eq!(unique, expected_sorted);

        // Deleted replies disappear from reads.
        assert!(soft_delete_event_and_update_thread(
            &pool,
            community,
            deep.id.as_bytes(),
            Some(&deep_parent),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("delete deep"));
        let subtree = get_thread_replies(&pool, community, root.id.as_bytes(), Some(64), 100, None)
            .await
            .expect("subtree read after delete");
        assert_eq!(subtree.len(), 5, "deleted reply must be invisible");
    }

    /// Channel window: top-level predicate (roots, metadata-less rows,
    /// broadcast depth-1 replies; ordinary replies never), thread summaries
    /// with batched participants, and the limit+1 has_more probe including
    /// the exact-multiple final page.
    #[tokio::test]
    async fn channel_window_predicate_summaries_and_pagination() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let channel = make_channel(&pool, community).await;
        let author = Keys::generate();
        let replier = Keys::generate();
        let base = 1_800_000_200u64;

        // Root with metadata (depth 0, broadcast).
        let root = make_event(&author, "discussed", base);
        insert_event_with_thread_metadata(
            &pool,
            community,
            &root,
            Some(channel),
            Some(ThreadMetadataParams {
                event_id: root.id.as_bytes(),
                event_created_at: event_ts(&root),
                channel_id: channel,
                parent_event_id: None,
                parent_event_created_at: None,
                root_event_id: None,
                root_event_created_at: None,
                depth: 0,
                broadcast: true,
            }),
        )
        .await
        .expect("insert root");

        // Metadata-less event (legacy shape) — top-level.
        let bare = make_event(&author, "bare", base + 1);
        insert_event_with_thread_metadata(&pool, community, &bare, Some(channel), None)
            .await
            .expect("insert bare");

        // Broadcast depth-1 reply — top-level; quiet reply — never.
        let broadcast_reply = make_event(&replier, "broadcast reply", base + 2);
        insert_reply(
            &pool,
            community,
            channel,
            &broadcast_reply,
            &root,
            &root,
            1,
            true,
        )
        .await;
        let quiet_reply = make_event(&replier, "quiet reply", base + 3);
        insert_reply(
            &pool,
            community,
            channel,
            &quiet_reply,
            &root,
            &root,
            1,
            false,
        )
        .await;

        let window = get_channel_window(&pool, community, channel, 50, None, None)
            .await
            .expect("window");
        let ids: Vec<Vec<u8>> = window
            .rows
            .iter()
            .map(|r| r.stored_event.event.id.as_bytes().to_vec())
            .collect();
        assert!(ids.contains(&root.id.as_bytes().to_vec()), "root is a row");
        assert!(
            ids.contains(&bare.id.as_bytes().to_vec()),
            "metadata-less event is a row"
        );
        assert!(
            ids.contains(&broadcast_reply.id.as_bytes().to_vec()),
            "broadcast depth-1 reply is a row"
        );
        assert!(
            !ids.contains(&quiet_reply.id.as_bytes().to_vec()),
            "ordinary reply must never be a channel row"
        );
        assert!(!window.has_more);
        assert!(window.next_cursor.is_none());

        // Replied root carries a summary with the replier among the batched
        // participants; reply-less row carries none.
        let root_row = window
            .rows
            .iter()
            .find(|r| r.stored_event.event.id == root.id)
            .expect("root row");
        let summary = root_row.thread_summary.as_ref().expect("summary");
        assert_eq!(summary.reply_count, 2);
        assert!(summary
            .participants
            .contains(&replier.public_key().to_bytes().to_vec()));
        let bare_row = window
            .rows
            .iter()
            .find(|r| r.stored_event.event.id == bare.id)
            .expect("bare row");
        assert!(bare_row.thread_summary.is_none());

        // Pagination: 3 top-level rows total, page limit 2 → page 1 has_more
        // with a cursor; the second page is under-full and exhausted. Then
        // an exact-multiple check: limit 3 → single page, no has_more.
        let page1 = get_channel_window(&pool, community, channel, 2, None, None)
            .await
            .expect("page 1");
        assert_eq!(page1.rows.len(), 2);
        assert!(page1.has_more);
        let cursor = page1.next_cursor.clone().expect("cursor");
        let page2 = get_channel_window(&pool, community, channel, 2, Some(cursor), None)
            .await
            .expect("page 2");
        assert_eq!(page2.rows.len(), 1);
        assert!(!page2.has_more);
        assert!(page2.next_cursor.is_none());
        // No row lost or duplicated across pages.
        let mut paged: Vec<Vec<u8>> = page1
            .rows
            .iter()
            .chain(page2.rows.iter())
            .map(|r| r.stored_event.event.id.as_bytes().to_vec())
            .collect();
        paged.sort();
        paged.dedup();
        assert_eq!(paged.len(), 3);

        let exact = get_channel_window(&pool, community, channel, 3, None, None)
            .await
            .expect("exact page");
        assert_eq!(exact.rows.len(), 3);
        assert!(
            !exact.has_more,
            "exact-multiple final page must report exhausted"
        );

        // Kind filter excludes everything (no kind 45001 rows here).
        let filtered = get_channel_window(&pool, community, channel, 10, None, Some(&[45001]))
            .await
            .expect("filtered window");
        assert!(filtered.rows.is_empty());

        // Tenant isolation: another community sees an empty window.
        let other = make_community(&pool).await;
        let none = get_channel_window(&pool, other, channel, 10, None, None)
            .await
            .expect("other tenant window");
        assert!(none.rows.is_empty());
    }
}

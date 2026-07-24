//! SQLite arms for event storage (WP1 — events).
//!
//! Function-for-function ports of `crate::event` plus the event-shaped
//! inline methods of `crate::Db` (`replace_addressable_event`,
//! `replace_parameterized_event`, `publish_nip43_membership_locked`,
//! `soft_delete_discovery_events`). Semantics mirror the Postgres arm:
//!
//! - AUTH (kind 22242) and ephemeral kinds (20000–29999) are never stored.
//! - Idempotency via `ON CONFLICT DO NOTHING` on the `(community_id, id)` PK.
//! - NIP-16/NIP-33 last-write-wins with `created_at DESC, id ASC` dominance.
//! - Soft deletes: visibility predicates always include `deleted_at IS NULL`.
//! - `community_id` leads every predicate (tenant isolation).
//!
//! **Clock/type conventions** (see `docs/phase2/sqlite-schema-notes.md`):
//! timestamps are INTEGER unix seconds converted with chrono at the Rust
//! boundary; UUIDs are lowercase hyphenated TEXT (bound as strings — sqlx
//! would encode a raw `uuid::Uuid` as a 16-byte BLOB on SQLite, which would
//! silently bifurcate the key format); pubkeys/ids/sigs are BLOB; `tags` is
//! JSON TEXT probed with `json_each`; p-tag lookups go through the
//! `event_mentions` side table which every insert path populates.
//!
//! **Locking:** everywhere the Postgres arm serialized writers with
//! `pg_advisory_xact_lock`, this arm uses a plain `BEGIN IMMEDIATE`
//! transaction — SQLite has a single global writer, so taking the write
//! lock up front serializes the whole read-check-write cycle end-to-end.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use buzz_core::kind::{
    event_kind_i32, is_ephemeral, KIND_AUTH, KIND_BOOKMARK_SET, KIND_EVENT_REMINDER,
    KIND_HUDDLE_STARTED, KIND_NIP43_MEMBERSHIP_LIST, KIND_READ_STATE,
};
use buzz_core::{CommunityId, StoredEvent};

use crate::error::{DbError, Result};
use crate::event::{
    extract_d_tag, extract_not_before, DueReminder, ReactionEventInsertOutcome,
    ThreadMetadataParams, D_TAG_MAX_LEN,
};
use crate::EventQuery;

/// Statement used to open every write transaction in this backend.
///
/// `BEGIN IMMEDIATE` takes the SQLite write lock up front, replacing each
/// `pg_advisory_xact_lock` in the Postgres arm (single writer ⇒ the whole
/// transaction is serialized against all other writers).
pub(crate) const BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";

/// Maximum huddle-start content bytes considered by the parent-link lookup
/// (mirrors `crate::event::HUDDLE_LINK_CONTENT_MAX_BYTES`).
const HUDDLE_LINK_CONTENT_MAX_BYTES: i64 = 512;
/// Maximum huddle-link candidate rows inspected after SQL prefiltering.
const HUDDLE_LINK_CANDIDATE_LIMIT: i64 = 32;

// ── Shared boundary helpers (also used by sibling sqlite modules) ───────────

/// Canonical TEXT form for a UUID column value (lowercase hyphenated).
pub(crate) fn uuid_text(id: Uuid) -> String {
    id.to_string()
}

/// Canonical TEXT form for the tenant key.
pub(crate) fn community_text(community_id: CommunityId) -> String {
    uuid_text(*community_id.as_uuid())
}

/// Parse a UUID TEXT column value.
pub(crate) fn parse_uuid_text(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|e| DbError::InvalidData(format!("invalid uuid text {s:?}: {e}")))
}

/// Convert INTEGER unix seconds into a `DateTime<Utc>`.
pub(crate) fn datetime_from_secs(secs: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(secs, 0).ok_or(DbError::InvalidTimestamp(secs))
}

/// Read an optional UUID TEXT column from a row.
fn get_optional_uuid(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<Option<Uuid>> {
    let raw: Option<String> = row.try_get(column)?;
    raw.as_deref().map(parse_uuid_text).transpose()
}

/// Reconstruct a [`StoredEvent`] from a standard event-projection row
/// (`id, pubkey, created_at, kind, tags, content, sig, received_at,
/// channel_id`). Mirrors `crate::event::row_to_stored_event`; rows that fail
/// to round-trip into a `nostr::Event` are logged and skipped (`None`).
fn row_to_stored_event(row: &sqlx::sqlite::SqliteRow) -> Result<Option<StoredEvent>> {
    let id_bytes: Vec<u8> = row.try_get("id")?;
    let pubkey_bytes: Vec<u8> = row.try_get("pubkey")?;
    let created_at_secs: i64 = row.try_get("created_at")?;
    let kind_i64: i64 = row.try_get("kind")?;
    let tags_text: String = row.try_get("tags")?;
    let content: String = row.try_get("content")?;
    let sig_bytes: Vec<u8> = row.try_get("sig")?;
    let received_at_secs: i64 = row.try_get("received_at")?;
    let channel_id = get_optional_uuid(row, "channel_id")?;

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

/// Serialize event tags for the `tags` JSON TEXT column.
fn tags_text(event: &Event) -> Result<String> {
    serde_json::to_string(&event.tags).map_err(DbError::from)
}

// ── Mentions (#p side-table) ────────────────────────────────────────────────

/// Extract p-tag mentions and insert into `event_mentions`
/// (`ON CONFLICT DO NOTHING`). Port of the free `crate::insert_mentions`;
/// called by every event-insert path in this module so the p-tag query
/// pushdown (`EventQuery::p_tag_hex`) and the feed queries stay indexed.
pub(crate) async fn insert_mentions(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
) -> Result<()> {
    let p_tags: Vec<&str> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            if parts.len() >= 2 && parts[0] == "p" {
                Some(parts[1].as_str())
            } else {
                None
            }
        })
        .collect();

    if p_tags.is_empty() {
        return Ok(());
    }

    let valid_pubkeys: Vec<String> = p_tags
        .into_iter()
        .filter(|pk| {
            if pk.len() != 64 || !pk.chars().all(|c| c.is_ascii_hexdigit()) {
                tracing::debug!(
                    event_id = %event.id,
                    invalid_ptag = pk,
                    "skipping malformed p-tag in insert_mentions"
                );
                false
            } else {
                true
            }
        })
        .map(|pk| pk.to_ascii_lowercase())
        .collect();

    if valid_pubkeys.is_empty() {
        return Ok(());
    }

    let event_id_bytes = event.id.as_bytes().to_vec();
    let created_at_secs = event.created_at.as_secs() as i64;
    let kind_i32 = event_kind_i32(event);
    let channel_text = channel_id.map(uuid_text);

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "INSERT INTO event_mentions \
         (community_id, pubkey_hex, event_id, event_created_at, channel_id, event_kind) ",
    );
    qb.push_values(&valid_pubkeys, |mut b, pubkey| {
        b.push_bind(community_text(community_id))
            .push_bind(pubkey.as_str())
            .push_bind(event_id_bytes.clone())
            .push_bind(created_at_secs)
            .push_bind(channel_text.clone())
            .push_bind(kind_i32);
    });
    qb.push(" ON CONFLICT DO NOTHING");

    qb.build().execute(pool).await?;
    Ok(())
}

/// Best-effort mention population after a committed insert — failures are
/// logged, never surfaced (parity with the `Db` facade wrappers).
async fn insert_mentions_best_effort(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
) {
    if let Err(e) = insert_mentions(pool, community_id, event, channel_id).await {
        tracing::warn!(event_id = %event.id, "Failed to insert mentions: {e}");
    }
}

// ── Insert paths ────────────────────────────────────────────────────────────

/// Reject kinds that must never be stored (AUTH + ephemeral).
fn reject_unstorable(event: &Event) -> Result<()> {
    let kind_u16 = event.kind.as_u16();
    let kind_u32 = u32::from(kind_u16);
    if kind_u32 == KIND_AUTH {
        return Err(DbError::AuthEventRejected);
    }
    if is_ephemeral(kind_u32) {
        return Err(DbError::EphemeralEventRejected(kind_u16));
    }
    Ok(())
}

const INSERT_EVENT_SQL: &str = "INSERT INTO events \
     (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id, d_tag, not_before) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
     ON CONFLICT DO NOTHING";

/// Insert a Nostr event. Rejects AUTH and ephemeral kinds.
///
/// Returns `(StoredEvent, was_inserted)` — `was_inserted` is `false` on
/// duplicate. On successful insert the `event_mentions` side table is
/// populated (best-effort, like the Postgres `Db::insert_event` wrapper), so
/// dispatch must NOT call `insert_mentions` again (harmless if it does —
/// `ON CONFLICT DO NOTHING`).
pub(crate) async fn insert_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
) -> Result<(StoredEvent, bool)> {
    reject_unstorable(event)?;

    let received_at = Utc::now();
    let result = sqlx::query(INSERT_EVENT_SQL)
        .bind(community_text(community_id))
        .bind(event.id.as_bytes().as_slice())
        .bind(event.pubkey.to_bytes().as_slice())
        .bind(event.created_at.as_secs() as i64)
        .bind(event_kind_i32(event))
        .bind(tags_text(event)?)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp())
        .bind(channel_id.map(uuid_text))
        .bind(extract_d_tag(event))
        .bind(extract_not_before(event))
        .execute(pool)
        .await?;

    let was_inserted = result.rows_affected() > 0;
    if was_inserted {
        insert_mentions_best_effort(pool, community_id, event, channel_id).await;
    }

    Ok((
        StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
        was_inserted,
    ))
}

// ── Query paths ─────────────────────────────────────────────────────────────

/// Push the shared `EventQuery` filter clauses (everything after the
/// community/deleted/p-tag prelude) onto `qb`. `col_prefix` is `"e."` when
/// the mentions join is active, `""` otherwise.
fn push_common_filters(qb: &mut QueryBuilder<Sqlite>, q: &EventQuery, col_prefix: &str) {
    if let Some(ch) = q.channel_id {
        qb.push(format!(" AND {col_prefix}channel_id = "))
            .push_bind(uuid_text(ch));
    } else if q.global_only {
        qb.push(format!(" AND {col_prefix}channel_id IS NULL"));
    }

    // Multi-channel IN pushdown: accessible channels + global rows.
    // SECURITY: Some(empty vec) means "no channel access" — global only.
    if let Some(ref ch_ids) = q.channel_ids {
        if ch_ids.is_empty() {
            qb.push(format!(" AND {col_prefix}channel_id IS NULL"));
        } else {
            qb.push(format!(
                " AND ({col_prefix}channel_id IS NULL OR {col_prefix}channel_id IN ("
            ));
            let mut sep = qb.separated(", ");
            for ch in ch_ids {
                sep.push_bind(uuid_text(*ch));
            }
            qb.push("))");
        }
    }

    if let Some(ks) = q.kinds.as_deref().filter(|k| !k.is_empty()) {
        qb.push(format!(" AND {col_prefix}kind IN ("));
        let mut sep = qb.separated(", ");
        for k in ks {
            sep.push_bind(*k);
        }
        qb.push(")");
    }

    if let Some(ref pk) = q.pubkey {
        qb.push(format!(" AND {col_prefix}pubkey = "))
            .push_bind(pk.clone());
    }

    if let Some(ref authors) = q.authors {
        if !authors.is_empty() {
            qb.push(format!(" AND {col_prefix}pubkey IN ("));
            let mut sep = qb.separated(", ");
            for a in authors {
                sep.push_bind(a.clone());
            }
            qb.push(")");
        }
    }

    if let Some(ref ids) = q.ids {
        if !ids.is_empty() {
            qb.push(format!(" AND {col_prefix}id IN ("));
            let mut sep = qb.separated(", ");
            for id in ids {
                sep.push_bind(id.clone());
            }
            qb.push(")");
        }
    }

    // e-tag pushdown. The Postgres arm uses jsonb containment
    // (`tags @> '[["e","<hex>"]]'`, GIN-indexed); SQLite probes the JSON TEXT
    // column with json_each. Unindexed per candidate row, which is fine at
    // solo-profile scale — the surrounding community/channel/kind/created_at
    // predicates are index-served first (see sqlite-schema-notes.md).
    if let Some(ref e_tags) = q.e_tags {
        if !e_tags.is_empty() {
            qb.push(" AND (");
            for (i, hex_id) in e_tags.iter().enumerate() {
                if i > 0 {
                    qb.push(" OR ");
                }
                qb.push(format!(
                    "EXISTS (SELECT 1 FROM json_each({col_prefix}tags) AS jt \
                     WHERE jt.value ->> 0 = 'e' AND jt.value ->> 1 = "
                ));
                qb.push_bind(hex_id.clone());
                qb.push(")");
            }
            qb.push(")");
        }
    }

    if let Some(s) = q.since {
        qb.push(format!(" AND {col_prefix}created_at >= "))
            .push_bind(s.timestamp());
    }
}

/// Query events with optional filters, ordered `created_at DESC, id ASC`.
///
/// Full `EventQuery` pushdown parity with `crate::event::query_events`:
/// kinds/authors/ids/channel_ids/since/until/limit/offset/d_tag(s), e-tags via
/// `json_each`, p-tag via the `event_mentions` join, `before_id` keyset
/// cursor, and `global_only`.
pub(crate) async fn query_events(pool: &SqlitePool, q: &EventQuery) -> Result<Vec<StoredEvent>> {
    if q.before_id.is_some() && q.until.is_none() {
        return Err(DbError::InvalidData(
            "before_id requires until to be set".to_string(),
        ));
    }
    if q.global_only && q.channel_id.is_some() {
        return Err(DbError::InvalidData(
            "global_only and channel_id are mutually exclusive".to_string(),
        ));
    }

    // Empty list means "match nothing".
    if q.kinds.as_deref().is_some_and(|k| k.is_empty())
        || q.authors.as_deref().is_some_and(|a| a.is_empty())
        || q.ids.as_deref().is_some_and(|i| i.is_empty())
        || q.e_tags.as_deref().is_some_and(|e| e.is_empty())
    {
        return Ok(vec![]);
    }

    let clamp = q.max_limit.unwrap_or(1000);
    let limit_val = q.limit.unwrap_or(100).min(clamp);
    let offset_val = q.offset.unwrap_or(0);

    let mut qb = new_event_query_builder(
        q,
        "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, \
         e.sig, e.received_at, e.channel_id ",
        "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id ",
    );
    let col_prefix = if q.p_tag_hex.is_some() { "e." } else { "" };

    push_common_filters(&mut qb, q, col_prefix);

    if let Some(u) = q.until {
        if let Some(ref bid) = q.before_id {
            // Composite keyset cursor: with ORDER BY created_at DESC, id ASC
            // the next page is created_at < ts OR (created_at = ts AND id > id).
            qb.push(format!(" AND ({col_prefix}created_at < "));
            qb.push_bind(u.timestamp());
            qb.push(format!(" OR ({col_prefix}created_at = "));
            qb.push_bind(u.timestamp());
            qb.push(format!(" AND {col_prefix}id > "));
            qb.push_bind(bid.clone());
            qb.push("))");
        } else {
            qb.push(format!(" AND {col_prefix}created_at <= "))
                .push_bind(u.timestamp());
        }
    }

    push_d_tag_filters(&mut qb, q, col_prefix);

    qb.push(format!(
        " ORDER BY {col_prefix}created_at DESC, {col_prefix}id ASC LIMIT "
    ));
    qb.push_bind(limit_val);
    qb.push(" OFFSET ").push_bind(offset_val);

    let rows = qb.build().fetch_all(pool).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(ev) = row_to_stored_event(&row)? {
            out.push(ev);
        }
    }
    Ok(out)
}

/// Build the SELECT prelude shared by `query_events`/`count_events`,
/// including the community scope, `deleted_at IS NULL`, and the
/// `event_mentions` join when `p_tag_hex` is set.
fn new_event_query_builder(
    q: &EventQuery,
    joined_select: &str,
    plain_select: &str,
) -> QueryBuilder<Sqlite> {
    if let Some(ref p_hex) = q.p_tag_hex {
        let mut b = QueryBuilder::new(format!(
            "{joined_select}FROM events e \
             INNER JOIN event_mentions m \
                ON e.community_id = m.community_id AND e.id = m.event_id \
             WHERE e.community_id = "
        ));
        b.push_bind(community_text(q.community_id));
        b.push(" AND m.community_id = ");
        b.push_bind(community_text(q.community_id));
        b.push(" AND e.deleted_at IS NULL AND m.pubkey_hex = ");
        b.push_bind(p_hex.to_ascii_lowercase());
        b
    } else {
        let mut b = QueryBuilder::new(format!("{plain_select}FROM events WHERE community_id = "));
        b.push_bind(community_text(q.community_id));
        b.push(" AND deleted_at IS NULL");
        b
    }
}

/// Push the NIP-33 `d_tag` / `d_tags` filters.
fn push_d_tag_filters(qb: &mut QueryBuilder<Sqlite>, q: &EventQuery, col_prefix: &str) {
    if let Some(ref d) = q.d_tag {
        qb.push(format!(" AND {col_prefix}d_tag = "))
            .push_bind(d.clone());
    } else if let Some(ref ds) = q.d_tags {
        if !ds.is_empty() {
            qb.push(format!(" AND {col_prefix}d_tag IN ("));
            let mut sep = qb.separated(", ");
            for d in ds {
                sep.push_bind(d.clone());
            }
            qb.push(")");
        }
    }
}

/// Count events matching the query (NIP-45 COUNT). Same filter logic as
/// [`query_events`], no cursor/order/limit.
pub(crate) async fn count_events(pool: &SqlitePool, q: &EventQuery) -> Result<i64> {
    if q.kinds.as_deref().is_some_and(|k| k.is_empty())
        || q.authors.as_deref().is_some_and(|a| a.is_empty())
        || q.ids.as_deref().is_some_and(|i| i.is_empty())
        || q.e_tags.as_deref().is_some_and(|e| e.is_empty())
    {
        return Ok(0);
    }

    let mut qb = new_event_query_builder(q, "SELECT COUNT(*) as cnt ", "SELECT COUNT(*) as cnt ");
    let col_prefix = if q.p_tag_hex.is_some() { "e." } else { "" };

    push_common_filters(&mut qb, q, col_prefix);

    if let Some(u) = q.until {
        qb.push(format!(" AND {col_prefix}created_at <= "))
            .push_bind(u.timestamp());
    }

    push_d_tag_filters(&mut qb, q, col_prefix);

    let row = qb.build().fetch_one(pool).await?;
    let cnt: i64 = row.try_get("cnt")?;
    Ok(cnt)
}

const EVENT_PROJECTION: &str =
    "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id FROM events ";

/// Fetch a single non-deleted event by its raw 32-byte ID.
pub(crate) async fn get_event_by_id(
    pool: &SqlitePool,
    community_id: CommunityId,
    id_bytes: &[u8],
) -> Result<Option<StoredEvent>> {
    let row = sqlx::query(
        "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
         FROM events WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(id_bytes)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => row_to_stored_event(&r),
        None => Ok(None),
    }
}

/// Fetch a single event by its raw 32-byte ID, **including soft-deleted rows**.
pub(crate) async fn get_event_by_id_including_deleted(
    pool: &SqlitePool,
    community_id: CommunityId,
    id_bytes: &[u8],
) -> Result<Option<StoredEvent>> {
    let row = sqlx::query(
        "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
         FROM events WHERE community_id = ?1 AND id = ?2 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(id_bytes)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => row_to_stored_event(&r),
        None => Ok(None),
    }
}

/// Batch-fetch non-deleted events by raw 32-byte IDs (arbitrary order).
pub(crate) async fn get_events_by_ids(
    pool: &SqlitePool,
    community_id: CommunityId,
    ids: &[&[u8]],
) -> Result<Vec<StoredEvent>> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    debug_assert!(ids.len() <= 500, "batch fetch should be bounded by caller");

    let mut qb: QueryBuilder<Sqlite> =
        QueryBuilder::new(format!("{EVENT_PROJECTION}WHERE community_id = "));
    qb.push_bind(community_text(community_id));
    qb.push(" AND deleted_at IS NULL AND id IN (");
    let mut sep = qb.separated(", ");
    for id in ids {
        sep.push_bind(id.to_vec());
    }
    qb.push(")");

    let rows = qb.build().fetch_all(pool).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(ev) = row_to_stored_event(&row)? {
            out.push(ev);
        }
    }
    Ok(out)
}

/// Fetch the latest global (`channel_id IS NULL`) replaceable event for a
/// `(kind, pubkey)` pair, canonical NIP-16 ordering.
pub(crate) async fn get_latest_global_replaceable(
    pool: &SqlitePool,
    community_id: CommunityId,
    kind: i32,
    pubkey_bytes: &[u8],
) -> Result<Option<StoredEvent>> {
    let row = sqlx::query(
        "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
         FROM events \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 \
           AND channel_id IS NULL AND deleted_at IS NULL \
         ORDER BY created_at DESC, id ASC LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(kind)
    .bind(pubkey_bytes)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => row_to_stored_event(&r),
        None => Ok(None),
    }
}

// ── Soft deletes ────────────────────────────────────────────────────────────

/// Soft-delete an event (`deleted_at = unixepoch()`). Returns `true` if the
/// event was deleted by this call, `false` if already deleted or not found.
pub(crate) async fn soft_delete_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(community_text(community_id))
    .bind(event_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Soft-delete the live row for an addressable coordinate
/// `(kind, pubkey, d_tag)` — the NIP-33 replacement key. `channel_id` is
/// intentionally NOT in the key (replacement is global per spec).
pub(crate) async fn soft_delete_by_coordinate(
    pool: &SqlitePool,
    community_id: CommunityId,
    kind: i32,
    pubkey: &[u8],
    d_tag: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
           AND deleted_at IS NULL",
    )
    .bind(community_text(community_id))
    .bind(kind)
    .bind(pubkey)
    .bind(d_tag)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Atomically soft-delete an event and decrement thread reply counters
/// (floored at zero, `max(x - 1, 0)`), in one transaction.
pub(crate) async fn soft_delete_event_and_update_thread(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
    parent_event_id: Option<&[u8]>,
    root_event_id: Option<&[u8]>,
) -> Result<bool> {
    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;
    let community = community_text(community_id);

    let result = sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(&community)
    .bind(event_id)
    .execute(&mut *tx)
    .await?;

    let deleted = result.rows_affected() > 0;
    if deleted {
        if let Some(pid) = parent_event_id {
            sqlx::query(
                "UPDATE thread_metadata SET reply_count = max(reply_count - 1, 0) \
                 WHERE community_id = ?1 AND event_id = ?2",
            )
            .bind(&community)
            .bind(pid)
            .execute(&mut *tx)
            .await?;

            if let Some(root_id) = root_event_id {
                sqlx::query(
                    "UPDATE thread_metadata SET descendant_count = max(descendant_count - 1, 0) \
                     WHERE community_id = ?1 AND event_id = ?2",
                )
                .bind(&community)
                .bind(root_id)
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    tx.commit().await?;
    Ok(deleted)
}

/// Soft-delete a channel's relay-authored NIP-29 discovery events
/// (kinds 39000–39002). Returns the number of rows retired.
pub(crate) async fn soft_delete_discovery_events(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    relay_pubkey: &[u8],
) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND channel_id = ?2 AND pubkey = ?3 \
           AND deleted_at IS NULL AND kind IN (39000, 39001, 39002)",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(channel_id))
    .bind(relay_pubkey)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ── Channel activity ────────────────────────────────────────────────────────

/// `created_at` of the most recent non-deleted event in a channel.
pub(crate) async fn get_last_message_at(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<Option<DateTime<Utc>>> {
    let secs: Option<i64> = sqlx::query_scalar(
        "SELECT created_at FROM events \
         WHERE community_id = ?1 AND channel_id = ?2 AND deleted_at IS NULL \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(channel_id))
    .fetch_optional(pool)
    .await?;
    secs.map(datetime_from_secs).transpose()
}

/// Bulk map of `channel_id → last_message_at` for the given channels;
/// channels with no events are omitted. One query regardless of input size.
pub(crate) async fn get_last_message_at_bulk(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_ids: &[Uuid],
) -> Result<HashMap<Uuid, DateTime<Utc>>> {
    if channel_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT channel_id, MAX(created_at) as last_at FROM events WHERE community_id = ",
    );
    qb.push_bind(community_text(community_id));
    qb.push(" AND deleted_at IS NULL AND channel_id IN (");
    let mut sep = qb.separated(", ");
    for id in channel_ids {
        sep.push_bind(uuid_text(*id));
    }
    qb.push(") GROUP BY channel_id");

    let rows = qb.build().fetch_all(pool).await?;
    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let id: String = row.try_get("channel_id")?;
        let last_at: i64 = row.try_get("last_at")?;
        map.insert(parse_uuid_text(&id)?, datetime_from_secs(last_at)?);
    }
    Ok(map)
}

// ── Huddle link lookup ──────────────────────────────────────────────────────

/// Return whether `parent_channel_id` has a creator-signed huddle-start event
/// (kind 48100) that links to `ephemeral_channel_id`.
///
/// Postgres `ILIKE`/`octet_length` become `LIKE` (ASCII-case-insensitive by
/// default in SQLite) and `length(CAST(content AS BLOB))` (byte length).
pub(crate) async fn huddle_started_link_exists(
    pool: &SqlitePool,
    community_id: CommunityId,
    parent_channel_id: Uuid,
    ephemeral_channel_id: Uuid,
    creator_pubkey: &[u8],
) -> Result<bool> {
    let uuid_needle = format!("%{ephemeral_channel_id}%");
    let candidates: Vec<String> = sqlx::query_scalar(
        "SELECT content FROM events \
         WHERE deleted_at IS NULL \
           AND community_id = ?1 \
           AND channel_id = ?2 \
           AND kind = ?3 \
           AND pubkey = ?4 \
           AND length(CAST(content AS BLOB)) <= ?5 \
           AND content LIKE ?6 \
         ORDER BY created_at DESC, id ASC \
         LIMIT ?7",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(parent_channel_id))
    .bind(KIND_HUDDLE_STARTED as i32)
    .bind(creator_pubkey)
    .bind(HUDDLE_LINK_CONTENT_MAX_BYTES)
    .bind(uuid_needle)
    .bind(HUDDLE_LINK_CANDIDATE_LIMIT)
    .fetch_all(pool)
    .await?;

    Ok(candidates
        .iter()
        .any(|content| huddle_started_content_links(content, ephemeral_channel_id)))
}

fn huddle_started_content_links(content: &str, ephemeral_channel_id: Uuid) -> bool {
    serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .and_then(|value| {
            value
                .get("ephemeral_channel_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
        })
        .is_some_and(|id| id == ephemeral_channel_id)
}

// ── Thread-coupled inserts ──────────────────────────────────────────────────

async fn insert_event_with_thread_metadata_tx(
    tx: &mut Transaction<'static, Sqlite>,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
    thread_meta: Option<ThreadMetadataParams<'_>>,
) -> Result<(StoredEvent, bool)> {
    reject_unstorable(event)?;

    let community = community_text(community_id);
    let received_at = Utc::now();

    let result = sqlx::query(INSERT_EVENT_SQL)
        .bind(&community)
        .bind(event.id.as_bytes().as_slice())
        .bind(event.pubkey.to_bytes().as_slice())
        .bind(event.created_at.as_secs() as i64)
        .bind(event_kind_i32(event))
        .bind(tags_text(event)?)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp())
        .bind(channel_id.map(uuid_text))
        .bind(extract_d_tag(event))
        .bind(extract_not_before(event))
        .execute(&mut **tx)
        .await?;

    let was_inserted = result.rows_affected() > 0;

    if was_inserted {
        if let Some(ref meta) = thread_meta {
            // Note: the SQLite thread_metadata PK is (community_id, event_id)
            // — stricter than the Postgres partition key, so a same-event
            // stub with a differing timestamp cannot double-count.
            let tm_result = sqlx::query(
                "INSERT INTO thread_metadata \
                 (community_id, event_created_at, event_id, channel_id, \
                  parent_event_id, parent_event_created_at, \
                  root_event_id, root_event_created_at, depth, broadcast) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(&community)
            .bind(meta.event_created_at.timestamp())
            .bind(meta.event_id)
            .bind(uuid_text(meta.channel_id))
            .bind(meta.parent_event_id)
            .bind(meta.parent_event_created_at.map(|t| t.timestamp()))
            .bind(meta.root_event_id)
            .bind(meta.root_event_created_at.map(|t| t.timestamp()))
            .bind(meta.depth)
            .bind(meta.broadcast)
            .execute(&mut **tx)
            .await?;

            // Only bump counters if the metadata row was actually inserted.
            if tm_result.rows_affected() > 0 {
                if let Some(pid) = meta.parent_event_id {
                    // Stub rows so the counter UPDATEs have a target
                    // (roots get no row on first insert).
                    let parent_ts = meta
                        .parent_event_created_at
                        .unwrap_or(meta.event_created_at);
                    insert_thread_stub_row(tx, &community, parent_ts, pid, meta.channel_id).await?;

                    if let Some(root_id) = meta.root_event_id {
                        if root_id != pid {
                            let root_ts =
                                meta.root_event_created_at.unwrap_or(meta.event_created_at);
                            insert_thread_stub_row(
                                tx,
                                &community,
                                root_ts,
                                root_id,
                                meta.channel_id,
                            )
                            .await?;
                        }
                    }

                    sqlx::query(
                        "UPDATE thread_metadata \
                         SET reply_count = reply_count + 1, last_reply_at = unixepoch() \
                         WHERE community_id = ?1 AND event_id = ?2",
                    )
                    .bind(&community)
                    .bind(pid)
                    .execute(&mut **tx)
                    .await?;

                    if let Some(root_id) = meta.root_event_id {
                        sqlx::query(
                            "UPDATE thread_metadata \
                             SET descendant_count = descendant_count + 1 \
                             WHERE community_id = ?1 AND event_id = ?2",
                        )
                        .bind(&community)
                        .bind(root_id)
                        .execute(&mut **tx)
                        .await?;
                    }
                }
            }
        }
    }

    Ok((
        StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
        was_inserted,
    ))
}

async fn insert_thread_stub_row(
    tx: &mut Transaction<'static, Sqlite>,
    community: &str,
    event_created_at: DateTime<Utc>,
    event_id: &[u8],
    channel_id: Uuid,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO thread_metadata \
         (community_id, event_created_at, event_id, channel_id, \
          parent_event_id, parent_event_created_at, \
          root_event_id, root_event_created_at, depth, broadcast) \
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, NULL, 0, 0) \
         ON CONFLICT DO NOTHING",
    )
    .bind(community)
    .bind(event_created_at.timestamp())
    .bind(event_id)
    .bind(uuid_text(channel_id))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Atomically insert an event and its optional thread metadata (reply and
/// descendant counters materialize in the same transaction as the event).
/// Populates `event_mentions` after commit (best-effort).
pub(crate) async fn insert_event_with_thread_metadata(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
    thread_meta: Option<ThreadMetadataParams<'_>>,
) -> Result<(StoredEvent, bool)> {
    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;
    let result =
        insert_event_with_thread_metadata_tx(&mut tx, community_id, event, channel_id, thread_meta)
            .await?;
    tx.commit().await?;
    if result.1 {
        insert_mentions_best_effort(pool, community_id, event, channel_id).await;
    }
    Ok(result)
}

/// Atomically insert a kind:7 reaction event and its reaction row.
///
/// Ordering parity with the Postgres arm: resolve target, upsert/reactivate
/// the reaction row (`ON CONFLICT … DO UPDATE … WHERE removed_at IS NOT
/// NULL`, three-state semantics), short-circuit active duplicates BEFORE the
/// kind:7 event insert, then insert event + thread metadata in the same
/// transaction.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_reaction_event_with_thread_metadata(
    pool: &SqlitePool,
    community_id: CommunityId,
    reaction_event: &Event,
    channel_id: Option<Uuid>,
    thread_meta: Option<ThreadMetadataParams<'_>>,
    target_event_id: &[u8],
    actor_pubkey: &[u8],
    emoji: &str,
) -> Result<ReactionEventInsertOutcome> {
    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;
    let community = community_text(community_id);

    let target_created_at: Option<i64> = sqlx::query_scalar(
        "SELECT created_at FROM events \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&community)
    .bind(target_event_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(target_created_at) = target_created_at else {
        tx.rollback().await?;
        return Ok(ReactionEventInsertOutcome::TargetMissing);
    };

    // Port of reaction::ADD_REACTION_SQL (`NOW()` → `unixepoch()`); preserves
    // the new / re-activate / active-duplicate three-state semantics.
    let reaction_result = sqlx::query(
        "INSERT INTO reactions \
         (community_id, event_created_at, event_id, pubkey, emoji, reaction_event_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT (community_id, event_created_at, event_id, pubkey, emoji) DO UPDATE SET \
             created_at = unixepoch(), \
             removed_at = NULL, \
             reaction_event_id = COALESCE(excluded.reaction_event_id, reactions.reaction_event_id) \
         WHERE reactions.removed_at IS NOT NULL",
    )
    .bind(&community)
    .bind(target_created_at)
    .bind(target_event_id)
    .bind(actor_pubkey)
    .bind(emoji)
    .bind(reaction_event.id.as_bytes().as_slice())
    .execute(&mut *tx)
    .await?;

    if reaction_result.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(ReactionEventInsertOutcome::Duplicate);
    }

    let (stored_event, was_inserted) = insert_event_with_thread_metadata_tx(
        &mut tx,
        community_id,
        reaction_event,
        channel_id,
        thread_meta,
    )
    .await?;

    tx.commit().await?;

    if was_inserted {
        insert_mentions_best_effort(pool, community_id, reaction_event, channel_id).await;
    }

    Ok(ReactionEventInsertOutcome::Inserted {
        stored_event: Box::new(stored_event),
        was_inserted,
    })
}

// ── Replaceable / addressable replacement ───────────────────────────────────

/// Whether `(incoming_ts, incoming_id)` is dominated by `(accepted_ts,
/// accepted_id)` under NIP-16 ordering (`created_at DESC, id ASC` wins).
fn dominated_by(
    incoming_ts: i64,
    incoming_id: &[u8],
    accepted_ts: i64,
    accepted_id: &[u8],
) -> bool {
    incoming_ts < accepted_ts || (incoming_ts == accepted_ts && incoming_id >= accepted_id)
}

/// Atomically replace a replaceable event keyed on `(kind, pubkey,
/// channel_id)` — NIP-16 kinds and relay-signed NIP-29 discovery state.
///
/// Where Postgres serialized writers with `pg_advisory_xact_lock`, this arm
/// relies on the `BEGIN IMMEDIATE` write lock (single writer). Postgres
/// `IS NOT DISTINCT FROM` is spelled `IS` in SQLite (NULL-safe equality).
pub(crate) async fn replace_addressable_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
) -> Result<(StoredEvent, bool)> {
    let kind_i32 = event_kind_i32(event);
    let pubkey_bytes = event.pubkey.to_bytes();
    let created_at_secs = event.created_at.as_secs() as i64;
    let community = community_text(community_id);
    let channel_text = channel_id.map(uuid_text);

    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;

    // Newest existing head; defensive ORDER BY + LIMIT 1 against historical
    // multi-live-row data.
    let existing: Option<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT created_at, id FROM events \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 \
           AND channel_id IS ?4 AND deleted_at IS NULL \
         ORDER BY created_at DESC, id ASC LIMIT 1",
    )
    .bind(&community)
    .bind(kind_i32)
    .bind(pubkey_bytes.as_slice())
    .bind(channel_text.as_deref())
    .fetch_optional(&mut *tx)
    .await?;

    let incoming_id = event.id.as_bytes().as_slice();
    if let Some((existing_ts, existing_id)) = existing {
        if dominated_by(created_at_secs, incoming_id, existing_ts, &existing_id) {
            tx.rollback().await?;
            let received_at = Utc::now();
            return Ok((
                StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
                false,
            ));
        }
    }

    sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 \
           AND channel_id IS ?4 AND deleted_at IS NULL",
    )
    .bind(&community)
    .bind(kind_i32)
    .bind(pubkey_bytes.as_slice())
    .bind(channel_text.as_deref())
    .execute(&mut *tx)
    .await?;

    let received_at = Utc::now();
    let insert_result = sqlx::query(
        "INSERT INTO events \
         (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id, d_tag) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&community)
    .bind(incoming_id)
    .bind(pubkey_bytes.as_slice())
    .bind(created_at_secs)
    .bind(kind_i32)
    .bind(tags_text(event)?)
    .bind(&event.content)
    .bind(event.sig.serialize().as_slice())
    .bind(received_at.timestamp())
    .bind(channel_text.as_deref())
    .bind(extract_d_tag(event))
    .execute(&mut *tx)
    .await?;

    if insert_result.rows_affected() == 0 {
        // Duplicate event id — roll back so the previous head is not lost.
        tx.rollback().await?;
        return Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
            false,
        ));
    }

    tx.commit().await?;

    // Mentions are a denormalized index — safe outside the transaction.
    insert_mentions_best_effort(pool, community_id, event, channel_id).await;

    Ok((
        StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
        true,
    ))
}

/// Atomically replace a NIP-33 parameterized replaceable event keyed on
/// `(kind, pubkey, d_tag)` (channel_id stored for scoping, not identity).
///
/// The Postgres arm's guard-trigger machinery (migrations 0009–0011 + the
/// `buzz.nip_rs_hard_delete` GUC) is deliberately absent here: it defended
/// against pre-fix relay binaries during rolling deploys, which cannot exist
/// on a single-writer Solo node. The NIP-RS watermark dominance check and
/// the hard-delete of superseded NIP-RS/mesh payloads are enforced in this
/// function's Rust/SQL directly, inside one `BEGIN IMMEDIATE` transaction
/// (replacing the advisory lock).
pub(crate) async fn replace_parameterized_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    d_tag: &str,
    channel_id: Option<Uuid>,
) -> Result<(StoredEvent, bool)> {
    let kind_i32 = event_kind_i32(event);
    let pubkey_bytes = event.pubkey.to_bytes();
    let created_at_secs = event.created_at.as_secs() as i64;
    let community = community_text(community_id);

    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;

    let d_tag_count = event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().is_some_and(|part| part == "d"))
        .count();
    let has_exact_d_tag = event.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.len() >= 2 && parts[0] == "d" && parts[1] == d_tag
    });
    let read_state_t_tag_count = event
        .tags
        .iter()
        .filter(|tag| {
            let parts = tag.as_slice();
            parts.len() == 2 && parts[0] == "t" && parts[1] == "read-state"
        })
        .count();
    let is_nip_rs = kind_i32 == KIND_READ_STATE as i32
        && d_tag_count == 1
        && has_exact_d_tag
        && d_tag.strip_prefix("read-state:").is_some_and(|slot| {
            slot.len() == 32
                && slot
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        && read_state_t_tag_count == 1;
    let is_buzz_mesh_status = kind_i32 == KIND_BOOKMARK_SET as i32
        && d_tag.starts_with("buzz-mesh-member-status:")
        && event.tags.iter().any(|tag| {
            let parts = tag.as_slice();
            parts.len() == 2 && parts[0] == "k" && parts[1] == "buzz-mesh-status"
        });
    let hard_delete_superseded = is_nip_rs || is_buzz_mesh_status;

    // Live head + (for NIP-RS) the compact historical ordering watermark.
    // The watermark survives a NIP-09 coordinate deletion, preventing a
    // previously accepted signed blob from being resurrected.
    let existing: Option<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT created_at, id FROM events \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
           AND deleted_at IS NULL \
         ORDER BY created_at DESC, id ASC LIMIT 1",
    )
    .bind(&community)
    .bind(kind_i32)
    .bind(pubkey_bytes.as_slice())
    .bind(d_tag)
    .fetch_optional(&mut *tx)
    .await?;
    let watermark: Option<(i64, Vec<u8>)> = if is_nip_rs {
        sqlx::query_as(
            "SELECT created_at, event_id FROM parameterized_event_watermarks \
             WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4",
        )
        .bind(&community)
        .bind(kind_i32)
        .bind(pubkey_bytes.as_slice())
        .bind(d_tag)
        .fetch_optional(&mut *tx)
        .await?
    } else {
        None
    };

    let incoming_id = event.id.as_bytes().as_slice();
    let dominated = existing
        .iter()
        .chain(watermark.iter())
        .any(|(accepted_ts, accepted_id)| {
            dominated_by(created_at_secs, incoming_id, *accepted_ts, accepted_id)
        });
    if dominated {
        tx.rollback().await?;
        let received_at = Utc::now();
        return Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
            false,
        ));
    }

    if existing.is_some() {
        // No `set_config('buzz.nip_rs_hard_delete', …)` here: the migration
        // 0011 guard trigger does not exist in the SQLite schema; this
        // function IS the enforcement point (see doc comment).
        let statement = if hard_delete_superseded {
            "DELETE FROM events \
             WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
               AND deleted_at IS NULL"
        } else {
            "UPDATE events SET deleted_at = unixepoch() \
             WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
               AND deleted_at IS NULL"
        };
        sqlx::query(statement)
            .bind(&community)
            .bind(kind_i32)
            .bind(pubkey_bytes.as_slice())
            .bind(d_tag)
            .execute(&mut *tx)
            .await?;

        if hard_delete_superseded {
            if let Some((_, existing_id)) = &existing {
                sqlx::query("DELETE FROM event_mentions WHERE community_id = ?1 AND event_id = ?2")
                    .bind(&community)
                    .bind(existing_id.as_slice())
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }

    let received_at = Utc::now();
    let insert_result = sqlx::query(INSERT_EVENT_SQL)
        .bind(&community)
        .bind(incoming_id)
        .bind(pubkey_bytes.as_slice())
        .bind(created_at_secs)
        .bind(kind_i32)
        .bind(tags_text(event)?)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp())
        .bind(channel_id.map(uuid_text))
        .bind(d_tag)
        .bind(extract_not_before(event))
        .execute(&mut *tx)
        .await?;

    if insert_result.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
            false,
        ));
    }

    if is_nip_rs {
        sqlx::query(
            "INSERT INTO parameterized_event_watermarks \
                 (community_id, kind, pubkey, d_tag, created_at, event_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT (community_id, kind, pubkey, d_tag) DO UPDATE SET \
                 created_at = excluded.created_at, event_id = excluded.event_id",
        )
        .bind(&community)
        .bind(kind_i32)
        .bind(pubkey_bytes.as_slice())
        .bind(d_tag)
        .bind(created_at_secs)
        .bind(incoming_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    insert_mentions_best_effort(pool, community_id, event, channel_id).await;

    Ok((
        StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
        true,
    ))
}

/// Atomically publish a NIP-43 membership snapshot: read members, build and
/// sign the kind:13534 event, retire prior snapshots, insert — all in one
/// `BEGIN IMMEDIATE` transaction (replacing the per-community advisory lock;
/// the write lock serializes the whole read-build-write cycle).
pub(crate) async fn publish_nip43_membership_locked(
    pool: &SqlitePool,
    community_id: CommunityId,
    relay_keypair: &nostr::Keys,
) -> Result<(StoredEvent, bool, usize)> {
    use nostr::{EventBuilder, Kind, Tag};

    let kind_i32 = KIND_NIP43_MEMBERSHIP_LIST as i32;
    let pubkey_bytes = relay_keypair.public_key().to_bytes();
    let community = community_text(community_id);

    let mut tx = pool.begin_with(BEGIN_IMMEDIATE).await?;

    // Read current members inside the write-locked transaction.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT pubkey, role FROM relay_members \
         WHERE community_id = ?1 ORDER BY created_at ASC",
    )
    .bind(&community)
    .fetch_all(&mut *tx)
    .await?;

    let member_count = rows.len();

    let mut tags: Vec<Tag> = Vec::with_capacity(member_count + 1);
    // NIP-70 protected-event marker.
    tags.push(
        Tag::parse(["-"])
            .map_err(|e| DbError::InvalidData(format!("failed to build '-' tag: {e}")))?,
    );
    for (pubkey, role) in &rows {
        tags.push(
            Tag::parse(["member", pubkey, role])
                .map_err(|e| DbError::InvalidData(format!("failed to build member tag: {e}")))?,
        );
    }

    let event = EventBuilder::new(Kind::Custom(kind_i32 as u16), "")
        .tags(tags)
        .sign_with_keys(relay_keypair)
        .map_err(|e| DbError::InvalidData(format!("failed to sign kind:13534: {e}")))?;

    let created_at_secs = event.created_at.as_secs() as i64;
    let received_at = Utc::now();

    // Soft-delete prior snapshots — unconditional, the relay is authoritative.
    sqlx::query(
        "UPDATE events SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 \
           AND channel_id IS NULL AND deleted_at IS NULL",
    )
    .bind(&community)
    .bind(kind_i32)
    .bind(pubkey_bytes.as_slice())
    .execute(&mut *tx)
    .await?;

    let insert_result = sqlx::query(
        "INSERT INTO events \
         (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id, d_tag) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&community)
    .bind(event.id.as_bytes().as_slice())
    .bind(pubkey_bytes.as_slice())
    .bind(created_at_secs)
    .bind(kind_i32)
    .bind(tags_text(&event)?)
    .bind(&event.content)
    .bind(event.sig.serialize().as_slice())
    .bind(received_at.timestamp())
    .bind(extract_d_tag(&event))
    .execute(&mut *tx)
    .await?;

    let was_inserted = insert_result.rows_affected() > 0;
    if !was_inserted {
        tx.rollback().await?;
        return Ok((
            StoredEvent::with_received_at(event, received_at, None, false),
            false,
            member_count,
        ));
    }

    tx.commit().await?;

    insert_mentions_best_effort(pool, community_id, &event, None).await;

    Ok((
        StoredEvent::with_received_at(event, received_at, None, true),
        true,
        member_count,
    ))
}

// ── Command events (open-transaction guard) ─────────────────────────────────

/// Result of persisting a command-kind event on the SQLite backend.
///
/// Sibling of [`crate::event::CommandEventPersist`], which wraps a Postgres
/// transaction type and therefore cannot be constructed by this backend. The
/// dispatch seam must reconcile the two (e.g. a backend-neutral enum over
/// [`crate::event::CommandEventTx`] and [`SqliteCommandEventTx`]).
pub enum SqliteCommandEventPersist {
    /// Event already stored (or dominated by a newer NIP-33 write) —
    /// idempotent success, skip domain mutations.
    Duplicate,
    /// Event inserted; the row is held in an open transaction. Commit the
    /// guard after the command's domain mutation succeeds.
    Inserted(SqliteCommandEventTx),
}

/// Deferred command-event insert guarding a validated command event.
///
/// Unlike the Postgres guard (an open transaction serialized by
/// `pg_advisory_xact_lock`), this guard holds NO transaction, NO connection,
/// and NO SQLite lock — it buffers the row and performs the actual
/// check-and-insert atomically inside [`SqliteCommandEventTx::commit`].
///
/// Why: the command executor's contract is persist → run domain mutations →
/// commit, and the domain mutations (e.g. `open_dm`) open their own write
/// transactions. On Postgres those ride a second pool connection; on SQLite
/// an open `BEGIN IMMEDIATE` guard would hold both the single pooled
/// connection and the global write lock across the mutation — a
/// self-deadlock. Deferring the insert preserves the guard's observable
/// semantics: dropping it leaves no trace (the row was never written), and
/// `commit()` lands the event with the same dominance/duplicate checks
/// re-run atomically.
///
/// Trade-off vs Postgres, documented honestly: the advisory lock serializes
/// the entire persist→mutate→commit window per NIP-33 coordinate; here only
/// the commit is serialized, so two racing same-coordinate commands can both
/// run their (idempotent) domain mutations before last-write-wins resolves
/// at commit time. Single-process Solo traffic makes that race window
/// acceptable; the stored-event outcome is identical.
pub struct SqliteCommandEventTx {
    pool: SqlitePool,
    community: String,
    id: Vec<u8>,
    pubkey: Vec<u8>,
    created_at_secs: i64,
    kind_i32: i32,
    tags: String,
    content: String,
    sig: Vec<u8>,
    channel_id: Option<Uuid>,
    d_tag: Option<String>,
}

impl SqliteCommandEventTx {
    /// Atomically land the buffered event insert.
    ///
    /// Re-runs the NIP-33 dominance check and the duplicate-id guard inside
    /// one short `BEGIN IMMEDIATE` transaction. Losing either race is
    /// idempotent success (`Ok`), mirroring the `Duplicate` outcome the
    /// persist step would have reported had the competitor arrived first.
    pub async fn commit(self) -> Result<()> {
        let mut tx = self.pool.begin_with(BEGIN_IMMEDIATE).await?;

        if let Some(ref d_tag) = self.d_tag {
            let existing: Option<(i64, Vec<u8>)> = sqlx::query_as(
                "SELECT created_at, id FROM events \
                 WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
                   AND deleted_at IS NULL \
                 ORDER BY created_at DESC, id ASC LIMIT 1",
            )
            .bind(&self.community)
            .bind(self.kind_i32)
            .bind(self.pubkey.as_slice())
            .bind(d_tag)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((existing_ts, existing_id)) = existing {
                if dominated_by(
                    self.created_at_secs,
                    self.id.as_slice(),
                    existing_ts,
                    &existing_id,
                ) {
                    // A newer head landed while the mutation ran — stale
                    // write, idempotent success (LWW).
                    return Ok(());
                }

                sqlx::query(
                    "UPDATE events SET deleted_at = unixepoch() \
                     WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
                       AND deleted_at IS NULL",
                )
                .bind(&self.community)
                .bind(self.kind_i32)
                .bind(self.pubkey.as_slice())
                .bind(d_tag)
                .execute(&mut *tx)
                .await?;
            }
        }

        sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id, d_tag) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&self.community)
        .bind(self.id.as_slice())
        .bind(self.pubkey.as_slice())
        .bind(self.created_at_secs)
        .bind(self.kind_i32)
        .bind(&self.tags)
        .bind(&self.content)
        .bind(self.sig.as_slice())
        .bind(Utc::now().timestamp())
        .bind(self.channel_id.map(uuid_text))
        .bind(&self.d_tag)
        .execute(&mut *tx)
        .await?;

        tx.commit().await.map_err(DbError::from)
    }
}

/// Validate a command-kind event and prepare its deferred insert.
///
/// Mirrors `crate::event::persist_command_event`'s observable outcomes: for
/// NIP-33 command kinds (those carrying a `d` tag) the current head is
/// checked for dominance — stale writes report
/// [`SqliteCommandEventPersist::Duplicate`] — and exact-id resubmissions are
/// detected up front. No transaction is opened here; the insert (with the
/// checks re-run atomically) happens in [`SqliteCommandEventTx::commit`] so
/// that no SQLite lock or pooled connection is held while the caller runs
/// its domain mutation. See the guard's docs for the full rationale.
pub(crate) async fn persist_command_event(
    pool: &SqlitePool,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
) -> Result<SqliteCommandEventPersist> {
    let community = community_text(community_id);

    let pubkey_bytes = event.pubkey.to_bytes();
    let kind_i32 = event.kind.as_u16() as i32;
    let created_at_secs = event.created_at.as_secs() as i64;

    let d_tag = extract_d_tag(event);
    if let Some(ref d_tag) = d_tag {
        if d_tag.len() > D_TAG_MAX_LEN {
            return Err(DbError::InvalidData(format!(
                "d tag too long ({} bytes, max {})",
                d_tag.len(),
                D_TAG_MAX_LEN,
            )));
        }

        // NIP-33 dominance pre-check: reject stale writes before the caller
        // executes the domain mutation. Re-checked atomically at commit.
        let existing: Option<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT created_at, id FROM events \
             WHERE community_id = ?1 AND kind = ?2 AND pubkey = ?3 AND d_tag = ?4 \
               AND deleted_at IS NULL \
             ORDER BY created_at DESC, id ASC LIMIT 1",
        )
        .bind(&community)
        .bind(kind_i32)
        .bind(pubkey_bytes.as_slice())
        .bind(d_tag)
        .fetch_optional(pool)
        .await?;

        let incoming_id = event.id.as_bytes().as_slice();
        if let Some((existing_ts, existing_id)) = existing {
            if dominated_by(created_at_secs, incoming_id, existing_ts, &existing_id) {
                return Ok(SqliteCommandEventPersist::Duplicate);
            }
        }
    }

    // Exact-id resubmission pre-check (the common duplicate path). Races are
    // caught by commit's ON CONFLICT DO NOTHING.
    let already: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM events WHERE community_id = ?1 AND id = ?2")
            .bind(&community)
            .bind(event.id.as_bytes().as_slice())
            .fetch_optional(pool)
            .await?;
    if already.is_some() {
        return Ok(SqliteCommandEventPersist::Duplicate);
    }

    Ok(SqliteCommandEventPersist::Inserted(SqliteCommandEventTx {
        pool: pool.clone(),
        community,
        id: event.id.as_bytes().to_vec(),
        pubkey: pubkey_bytes.to_vec(),
        created_at_secs,
        kind_i32,
        tags: tags_text(event)?,
        content: event.content.clone(),
        sig: event.sig.serialize().to_vec(),
        channel_id,
        d_tag,
    }))
}

// ── NIP-ER reminders ────────────────────────────────────────────────────────

/// Query due reminders: latest-per-coordinate `kind:30300` rows where
/// `not_before <= now`, not deleted, not delivered, community not archived.
///
/// Postgres used `SELECT DISTINCT ON (community_id, pubkey, d_tag)`; SQLite
/// has no `DISTINCT ON`, so this uses `ROW_NUMBER() OVER (PARTITION BY …
/// ORDER BY created_at DESC, id ASC) = 1` (canonical NIP-16 head pick).
pub(crate) async fn query_due_reminders(
    pool: &SqlitePool,
    now_secs: i64,
    batch_limit: i64,
) -> Result<Vec<DueReminder>> {
    let kind_i32 = KIND_EVENT_REMINDER as i32;
    let rows = sqlx::query(
        "SELECT community_id, host, id, pubkey, created_at, kind, tags, content, sig, channel_id \
         FROM ( \
             SELECT e.community_id, c.host, e.id, e.pubkey, e.created_at, e.kind, \
                    e.tags, e.content, e.sig, e.channel_id, e.d_tag, \
                    ROW_NUMBER() OVER ( \
                        PARTITION BY e.community_id, e.pubkey, e.d_tag \
                        ORDER BY e.created_at DESC, e.id ASC \
                    ) AS rn \
             FROM events AS e \
             JOIN communities AS c ON c.id = e.community_id \
             WHERE e.kind = ?1 \
               AND e.not_before IS NOT NULL \
               AND e.not_before <= ?2 \
               AND e.deleted_at IS NULL \
               AND e.delivered_at IS NULL \
               AND c.archived_at IS NULL \
         ) \
         WHERE rn = 1 \
         ORDER BY community_id, pubkey, d_tag \
         LIMIT ?3",
    )
    .bind(kind_i32)
    .bind(now_secs)
    .bind(batch_limit)
    .fetch_all(pool)
    .await?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let community: String = row.try_get("community_id")?;
        let created_at: i64 = row.try_get("created_at")?;
        let kind_i64: i64 = row.try_get("kind")?;
        let tags_raw: String = row.try_get("tags")?;
        results.push(DueReminder {
            community_id: CommunityId::from_uuid(parse_uuid_text(&community)?),
            host: row.try_get("host")?,
            id: row.try_get("id")?,
            pubkey: row.try_get("pubkey")?,
            created_at: datetime_from_secs(created_at)?,
            kind: i32::try_from(kind_i64)
                .map_err(|_| DbError::InvalidData(format!("kind out of i32 range: {kind_i64}")))?,
            tags: serde_json::from_str(&tags_raw)?,
            content: row.try_get("content")?,
            sig: row.try_get("sig")?,
            channel_id: get_optional_uuid(&row, "channel_id")?,
        });
    }
    Ok(results)
}

/// Atomically claim a due reminder for delivery with a now-timestamp stamp.
pub(crate) async fn claim_due_reminder(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
) -> Result<bool> {
    claim_due_reminder_with_stamp(
        pool,
        community_id,
        event_id,
        event_created_at,
        Utc::now().timestamp(),
    )
    .await
}

/// Atomically claim a due reminder using a caller-supplied delivery stamp
/// (compare-and-set on `delivered_at IS NULL`). Community-scoped: the same
/// event id may exist in multiple communities.
pub(crate) async fn claim_due_reminder_with_stamp(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
    delivery_stamp: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE events SET delivered_at = ?1 \
         WHERE community_id = ?2 AND created_at = ?3 AND id = ?4 AND delivered_at IS NULL",
    )
    .bind(delivery_stamp)
    .bind(community_text(community_id))
    .bind(event_created_at.timestamp())
    .bind(event_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Release a previously claimed reminder when publish fails.
/// Compare-and-clear on the exact `delivery_stamp` written by the claim.
pub(crate) async fn release_due_reminder(
    pool: &SqlitePool,
    community_id: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
    delivery_stamp: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE events SET delivered_at = NULL \
         WHERE community_id = ?1 AND created_at = ?2 AND id = ?3 AND delivered_at = ?4",
    )
    .bind(community_text(community_id))
    .bind(event_created_at.timestamp())
    .bind(event_id)
    .bind(delivery_stamp)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};

    async fn setup_pool() -> SqlitePool {
        // Single connection: sqlite::memory: gives each connection its own DB.
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
            .bind(format!("event-test-{}.example", id.simple()))
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

    fn make_event(kind: u16, content: &str) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    fn make_event_at(kind: u16, content: &str, created_at: u64) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .custom_created_at(Timestamp::from(created_at))
            .sign_with_keys(&Keys::generate())
            .expect("sign timestamped")
    }

    fn make_event_with_tags(kind: u16, content: &str, tags: Vec<Tag>) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .tags(tags)
            .sign_with_keys(&Keys::generate())
            .expect("sign tagged")
    }

    fn keyed_event_at(
        keys: &Keys,
        kind: u16,
        content: &str,
        created_at: u64,
        tags: Vec<Tag>,
    ) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .custom_created_at(Timestamp::from(created_at))
            .tags(tags)
            .sign_with_keys(keys)
            .expect("sign keyed")
    }

    #[tokio::test]
    async fn insert_and_query_round_trip_with_kind_and_channel_filters() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let chan_a = make_channel(&pool, community).await;
        let chan_b = make_channel(&pool, community).await;

        let in_a = make_event(9, "message in channel a");
        let in_b = make_event(9, "message in channel b");
        let other_kind = make_event(45001, "forum post in a");
        let (stored, inserted) = insert_event(&pool, community, &in_a, Some(chan_a))
            .await
            .expect("insert a");
        assert!(inserted);
        assert_eq!(stored.channel_id, Some(chan_a));
        insert_event(&pool, community, &in_b, Some(chan_b))
            .await
            .expect("insert b");
        insert_event(&pool, community, &other_kind, Some(chan_a))
            .await
            .expect("insert other kind");

        // Duplicate insert is idempotent.
        let (_, dup) = insert_event(&pool, community, &in_a, Some(chan_a))
            .await
            .expect("duplicate insert");
        assert!(!dup);

        let events = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                channel_id: Some(chan_a),
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("query");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.id, in_a.id);
        assert_eq!(events[0].event.content, "message in channel a");

        let count = count_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("count");
        assert_eq!(count, 2);

        // Tenant scoping: another community sees nothing.
        let other = make_community(&pool).await;
        let none = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                ..EventQuery::for_community(other)
            },
        )
        .await
        .expect("query other tenant");
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn insert_rejects_auth_and_ephemeral_kinds() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;

        let auth = make_event(22242, "auth");
        assert!(matches!(
            insert_event(&pool, community, &auth, None).await,
            Err(DbError::AuthEventRejected)
        ));

        let ephemeral = make_event(20001, "ephemeral");
        assert!(matches!(
            insert_event(&pool, community, &ephemeral, None).await,
            Err(DbError::EphemeralEventRejected(20001))
        ));
    }

    #[tokio::test]
    async fn addressable_lww_dominance_both_directions() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let keys = Keys::generate();
        let base = 1_800_000_000u64;

        let v1 = keyed_event_at(&keys, 39000, "v1", base, vec![]);
        let v2 = keyed_event_at(&keys, 39000, "v2", base + 10, vec![]);

        let (_, ok) = replace_addressable_event(&pool, community, &v1, None)
            .await
            .expect("insert v1");
        assert!(ok);

        // Newer wins: v2 replaces v1.
        let (_, ok) = replace_addressable_event(&pool, community, &v2, None)
            .await
            .expect("replace with v2");
        assert!(ok);
        assert!(
            get_event_by_id(&pool, community, v1.id.as_bytes())
                .await
                .expect("get v1")
                .is_none(),
            "old head must be soft-deleted"
        );

        // Older loses: replaying v1 must be rejected and v2 stays the head.
        let (_, ok) = replace_addressable_event(&pool, community, &v1, None)
            .await
            .expect("replay v1");
        assert!(!ok, "stale write must be rejected");
        let head =
            get_latest_global_replaceable(&pool, community, 39000, &keys.public_key().to_bytes())
                .await
                .expect("head")
                .expect("head exists");
        assert_eq!(head.event.id, v2.id);

        // Same-second tie: lowest id wins.
        let t1 = keyed_event_at(&keys, 39001, "tie-a", base, vec![]);
        let t2 = keyed_event_at(&keys, 39001, "tie-b", base, vec![]);
        let (lower, higher) = if t1.id.as_bytes() < t2.id.as_bytes() {
            (t1, t2)
        } else {
            (t2, t1)
        };
        let (_, ok) = replace_addressable_event(&pool, community, &higher, None)
            .await
            .expect("insert higher id");
        assert!(ok);
        let (_, ok) = replace_addressable_event(&pool, community, &lower, None)
            .await
            .expect("insert lower id");
        assert!(ok, "lower id must dominate on same-second tie");
        let (_, ok) = replace_addressable_event(&pool, community, &higher, None)
            .await
            .expect("replay higher id");
        assert!(!ok, "higher id must lose on same-second tie");
    }

    #[tokio::test]
    async fn parameterized_lww_and_nip_rs_watermark_guard() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let keys = Keys::generate();
        let base = 1_800_000_000u64;
        let d_tag = format!("read-state:{}", "ab".repeat(16));
        let nip_rs_tags = |d: &str| {
            vec![
                Tag::parse(["d", d]).expect("d tag"),
                Tag::parse(["t", "read-state"]).expect("t tag"),
            ]
        };

        let v1 = keyed_event_at(&keys, 30078, "rs-v1", base, nip_rs_tags(&d_tag));
        let v2 = keyed_event_at(&keys, 30078, "rs-v2", base + 5, nip_rs_tags(&d_tag));

        let (_, ok) = replace_parameterized_event(&pool, community, &v1, &d_tag, None)
            .await
            .expect("insert v1");
        assert!(ok);
        let (_, ok) = replace_parameterized_event(&pool, community, &v2, &d_tag, None)
            .await
            .expect("replace with v2");
        assert!(ok);

        // NIP-RS supersession hard-deletes the old payload entirely.
        assert!(
            get_event_by_id_including_deleted(&pool, community, v1.id.as_bytes())
                .await
                .expect("lookup v1")
                .is_none(),
            "superseded NIP-RS payload must be hard-deleted"
        );

        // Stale write rejected while the head is live.
        let (_, ok) = replace_parameterized_event(&pool, community, &v1, &d_tag, None)
            .await
            .expect("replay v1");
        assert!(!ok);

        // The watermark survives a coordinate deletion: after NIP-09 removes
        // the head, replaying the older payload is STILL rejected.
        assert!(soft_delete_by_coordinate(
            &pool,
            community,
            30078,
            &keys.public_key().to_bytes(),
            &d_tag
        )
        .await
        .expect("coordinate delete"));
        let (_, ok) = replace_parameterized_event(&pool, community, &v1, &d_tag, None)
            .await
            .expect("replay v1 after coordinate delete");
        assert!(!ok, "watermark must block resurrection of an older payload");
    }

    #[tokio::test]
    async fn soft_delete_hides_event_from_reads() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let event = make_event(9, "to be deleted");
        insert_event(&pool, community, &event, None)
            .await
            .expect("insert");

        assert!(soft_delete_event(&pool, community, event.id.as_bytes())
            .await
            .expect("delete"));
        // Second delete is a no-op.
        assert!(!soft_delete_event(&pool, community, event.id.as_bytes())
            .await
            .expect("re-delete"));

        assert!(get_event_by_id(&pool, community, event.id.as_bytes())
            .await
            .expect("get")
            .is_none());
        assert!(
            get_event_by_id_including_deleted(&pool, community, event.id.as_bytes())
                .await
                .expect("get including deleted")
                .is_some()
        );
        let visible = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("query");
        assert!(visible.is_empty());
        assert_eq!(
            count_events(
                &pool,
                &EventQuery {
                    kinds: Some(vec![9]),
                    ..EventQuery::for_community(community)
                }
            )
            .await
            .expect("count"),
            0
        );
    }

    #[tokio::test]
    async fn p_tag_query_uses_mentions_side_table() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let mentioned = Keys::generate().public_key();
        let mentioned_hex = mentioned.to_hex();

        let with_mention = make_event_with_tags(
            9,
            "hey you",
            vec![Tag::parse(["p", &mentioned_hex]).expect("p tag")],
        );
        let without_mention = make_event(9, "no mention here");
        insert_event(&pool, community, &with_mention, None)
            .await
            .expect("insert mention");
        insert_event(&pool, community, &without_mention, None)
            .await
            .expect("insert plain");

        // Mentions row was populated by insert_event.
        let (n,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM event_mentions WHERE community_id = ?1 AND pubkey_hex = ?2",
        )
        .bind(community.as_uuid().to_string())
        .bind(&mentioned_hex)
        .fetch_one(&pool)
        .await
        .expect("count mentions");
        assert_eq!(n, 1);

        let hits = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                p_tag_hex: Some(mentioned_hex.to_ascii_uppercase()), // case-normalized
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("p-tag query");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].event.id, with_mention.id);

        let cnt = count_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                p_tag_hex: Some(mentioned_hex),
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("p-tag count");
        assert_eq!(cnt, 1);
    }

    #[tokio::test]
    async fn e_tag_query_probes_tags_json() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let root = make_event(9, "root message");
        let root_hex = root.id.to_hex();
        let reply = make_event_with_tags(
            9,
            "reply",
            vec![Tag::parse(["e", &root_hex]).expect("e tag")],
        );
        let unrelated = make_event(9, "unrelated");
        insert_event(&pool, community, &root, None)
            .await
            .expect("insert root");
        insert_event(&pool, community, &reply, None)
            .await
            .expect("insert reply");
        insert_event(&pool, community, &unrelated, None)
            .await
            .expect("insert unrelated");

        let hits = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![9]),
                e_tags: Some(vec![root_hex.clone()]),
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("e-tag query");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].event.id, reply.id);

        assert_eq!(
            count_events(
                &pool,
                &EventQuery {
                    kinds: Some(vec![9]),
                    e_tags: Some(vec![root_hex]),
                    ..EventQuery::for_community(community)
                }
            )
            .await
            .expect("e-tag count"),
            1
        );
    }

    #[tokio::test]
    async fn keyset_pagination_orders_created_desc_id_asc() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let base = 1_800_000_000u64;

        // Three events in the same second + one older.
        let mut same_second: Vec<Event> = (0..3)
            .map(|i| make_event_at(9, &format!("same-{i}"), base + 100))
            .collect();
        let older = make_event_at(9, "older", base + 50);
        for e in same_second.iter().chain(std::iter::once(&older)) {
            insert_event(&pool, community, e, None)
                .await
                .expect("insert");
        }
        same_second.sort_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));

        let q = |limit: i64, until: Option<u64>, before_id: Option<Vec<u8>>| EventQuery {
            kinds: Some(vec![9]),
            limit: Some(limit),
            until: until.map(|u| datetime_from_secs(u as i64).expect("until ts")),
            before_id,
            ..EventQuery::for_community(community)
        };

        // First page: created_at DESC, id ASC.
        let page1 = query_events(&pool, &q(2, Some(base + 200), None))
            .await
            .expect("page 1");
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].event.id, same_second[0].id);
        assert_eq!(page1[1].event.id, same_second[1].id);

        // Second page via composite keyset cursor.
        let cursor_ts = page1[1].event.created_at.as_secs();
        let cursor_id = page1[1].event.id.as_bytes().to_vec();
        let page2 = query_events(&pool, &q(2, Some(cursor_ts), Some(cursor_id)))
            .await
            .expect("page 2");
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].event.id, same_second[2].id);
        assert_eq!(page2[1].event.id, older.id);

        // before_id without until is invalid.
        assert!(query_events(&pool, &q(2, None, Some(vec![0u8; 32])))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn thread_metadata_counters_increment_and_decrement() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let channel = make_channel(&pool, community).await;
        let base = 1_800_000_000u64;

        let root = make_event_at(9, "root", base);
        insert_event(&pool, community, &root, Some(channel))
            .await
            .expect("insert root");

        let reply = make_event_at(9, "reply", base + 1);
        let reply_created = datetime_from_secs((base + 1) as i64).expect("ts");
        let root_created = datetime_from_secs(base as i64).expect("ts");
        let (_, inserted) = insert_event_with_thread_metadata(
            &pool,
            community,
            &reply,
            Some(channel),
            Some(ThreadMetadataParams {
                event_id: reply.id.as_bytes(),
                event_created_at: reply_created,
                channel_id: channel,
                parent_event_id: Some(root.id.as_bytes()),
                parent_event_created_at: Some(root_created),
                root_event_id: Some(root.id.as_bytes()),
                root_event_created_at: Some(root_created),
                depth: 1,
                broadcast: false,
            }),
        )
        .await
        .expect("insert reply");
        assert!(inserted);

        let (reply_count, descendant_count): (i64, i64) = sqlx::query_as(
            "SELECT reply_count, descendant_count FROM thread_metadata \
             WHERE community_id = ?1 AND event_id = ?2",
        )
        .bind(community.as_uuid().to_string())
        .bind(root.id.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("root counters");
        assert_eq!(reply_count, 1);
        assert_eq!(descendant_count, 1);

        // Deleting the reply decrements both counters (floored at zero).
        assert!(soft_delete_event_and_update_thread(
            &pool,
            community,
            reply.id.as_bytes(),
            Some(root.id.as_bytes()),
            Some(root.id.as_bytes()),
        )
        .await
        .expect("delete reply"));
        let (reply_count, descendant_count): (i64, i64) = sqlx::query_as(
            "SELECT reply_count, descendant_count FROM thread_metadata \
             WHERE community_id = ?1 AND event_id = ?2",
        )
        .bind(community.as_uuid().to_string())
        .bind(root.id.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("root counters after delete");
        assert_eq!(reply_count, 0);
        assert_eq!(descendant_count, 0);
    }

    #[tokio::test]
    async fn reaction_insert_duplicate_short_circuits_before_event_store() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let target = make_event(9, "reaction target");
        insert_event(&pool, community, &target, None)
            .await
            .expect("insert target");

        let actor = Keys::generate();
        let actor_pubkey = actor.public_key().to_bytes();
        let target_hex = target.id.to_hex();
        let mk_reaction = || {
            EventBuilder::new(Kind::Custom(7), "👍")
                .tags(vec![
                    Tag::parse(["e", &target_hex]).expect("e tag"),
                    Tag::parse(["nonce", &Uuid::new_v4().to_string()]).expect("nonce"),
                ])
                .sign_with_keys(&actor)
                .expect("sign reaction")
        };
        let first = mk_reaction();
        let second = mk_reaction();

        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &first,
            None,
            None,
            target.id.as_bytes(),
            &actor_pubkey,
            "👍",
        )
        .await
        .expect("first reaction");
        assert!(matches!(
            outcome,
            ReactionEventInsertOutcome::Inserted {
                was_inserted: true,
                ..
            }
        ));

        let dup = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &second,
            None,
            None,
            target.id.as_bytes(),
            &actor_pubkey,
            "👍",
        )
        .await
        .expect("duplicate reaction");
        assert!(matches!(dup, ReactionEventInsertOutcome::Duplicate));
        assert!(
            get_event_by_id(&pool, community, second.id.as_bytes())
                .await
                .expect("lookup duplicate kind:7")
                .is_none(),
            "duplicate reaction must not store its kind:7 event"
        );

        // Missing target (other community) commits nothing.
        let other = make_community(&pool).await;
        let cross = insert_reaction_event_with_thread_metadata(
            &pool,
            other,
            &mk_reaction(),
            None,
            None,
            target.id.as_bytes(),
            &actor_pubkey,
            "👍",
        )
        .await
        .expect("cross-community reaction");
        assert!(matches!(cross, ReactionEventInsertOutcome::TargetMissing));
    }

    #[tokio::test]
    async fn persist_command_event_guards_and_replaces_by_coordinate() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let keys = Keys::generate();
        let base = 1_800_000_000u64;
        let d_tags = vec![Tag::parse(["d", "wf-name"]).expect("d tag")];

        let v1 = keyed_event_at(&keys, 31500, "wf-v1", base, d_tags.clone());
        let v2 = keyed_event_at(&keys, 31500, "wf-v2", base + 10, d_tags.clone());

        // First write: Inserted with an open guard; drop without commit rolls back.
        match persist_command_event(&pool, community, &v1, None)
            .await
            .expect("persist v1")
        {
            SqliteCommandEventPersist::Inserted(guard) => drop(guard),
            SqliteCommandEventPersist::Duplicate => panic!("expected Inserted"),
        }
        assert!(
            get_event_by_id(&pool, community, v1.id.as_bytes())
                .await
                .expect("lookup v1")
                .is_none(),
            "dropped guard must roll the insert back"
        );

        // Retry with commit.
        match persist_command_event(&pool, community, &v1, None)
            .await
            .expect("persist v1 again")
        {
            SqliteCommandEventPersist::Inserted(guard) => guard.commit().await.expect("commit"),
            SqliteCommandEventPersist::Duplicate => panic!("expected Inserted"),
        }
        assert!(get_event_by_id(&pool, community, v1.id.as_bytes())
            .await
            .expect("lookup v1 committed")
            .is_some());

        // Same event id again → Duplicate.
        assert!(matches!(
            persist_command_event(&pool, community, &v1, None)
                .await
                .expect("re-persist v1"),
            SqliteCommandEventPersist::Duplicate
        ));

        // Newer coordinate write soft-deletes the old head.
        match persist_command_event(&pool, community, &v2, None)
            .await
            .expect("persist v2")
        {
            SqliteCommandEventPersist::Inserted(guard) => guard.commit().await.expect("commit v2"),
            SqliteCommandEventPersist::Duplicate => panic!("expected Inserted for v2"),
        }
        assert!(get_event_by_id(&pool, community, v1.id.as_bytes())
            .await
            .expect("v1 after replace")
            .is_none());

        // Stale coordinate write → Duplicate (v1 dominated by v2).
        let v0 = keyed_event_at(&keys, 31500, "wf-v0", base - 10, d_tags);
        assert!(matches!(
            persist_command_event(&pool, community, &v0, None)
                .await
                .expect("persist stale v0"),
            SqliteCommandEventPersist::Duplicate
        ));
    }

    #[tokio::test]
    async fn last_message_at_single_and_bulk() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let chan_a = make_channel(&pool, community).await;
        let chan_b = make_channel(&pool, community).await;
        let empty = make_channel(&pool, community).await;
        let base = 1_800_000_000u64;

        insert_event(
            &pool,
            community,
            &make_event_at(9, "a1", base),
            Some(chan_a),
        )
        .await
        .expect("a1");
        insert_event(
            &pool,
            community,
            &make_event_at(9, "a2", base + 30),
            Some(chan_a),
        )
        .await
        .expect("a2");
        insert_event(
            &pool,
            community,
            &make_event_at(9, "b1", base + 20),
            Some(chan_b),
        )
        .await
        .expect("b1");

        let last_a = get_last_message_at(&pool, community, chan_a)
            .await
            .expect("last a")
            .expect("some");
        assert_eq!(last_a.timestamp(), (base + 30) as i64);
        assert!(get_last_message_at(&pool, community, empty)
            .await
            .expect("last empty")
            .is_none());

        let map = get_last_message_at_bulk(&pool, community, &[chan_a, chan_b, empty])
            .await
            .expect("bulk");
        assert_eq!(map.len(), 2);
        assert_eq!(map[&chan_a].timestamp(), (base + 30) as i64);
        assert_eq!(map[&chan_b].timestamp(), (base + 20) as i64);
        assert!(!map.contains_key(&empty));
    }

    #[tokio::test]
    async fn get_events_by_ids_skips_deleted_and_scopes_by_community() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let e1 = make_event(9, "one");
        let e2 = make_event(9, "two");
        insert_event(&pool, community, &e1, None).await.expect("e1");
        insert_event(&pool, community, &e2, None).await.expect("e2");
        soft_delete_event(&pool, community, e2.id.as_bytes())
            .await
            .expect("delete e2");

        let got = get_events_by_ids(
            &pool,
            community,
            &[e1.id.as_bytes().as_slice(), e2.id.as_bytes().as_slice()],
        )
        .await
        .expect("bulk get");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event.id, e1.id);
    }

    #[tokio::test]
    async fn due_reminders_pick_latest_per_coordinate_and_claim_release() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let keys = Keys::generate();
        let base = 1_800_000_000u64;
        let mk_reminder = |created: u64, not_before: u64| {
            keyed_event_at(
                &keys,
                30300,
                "ciphertext",
                created,
                vec![
                    Tag::parse(["d", "reminder-slot"]).expect("d tag"),
                    Tag::parse(["not_before", &not_before.to_string()]).expect("not_before tag"),
                ],
            )
        };

        let old = mk_reminder(base, base + 60);
        let new = mk_reminder(base + 10, base + 60);
        insert_event(&pool, community, &old, None)
            .await
            .expect("old");
        insert_event(&pool, community, &new, None)
            .await
            .expect("new");

        // Not due yet.
        assert!(query_due_reminders(&pool, (base + 30) as i64, 10)
            .await
            .expect("not due")
            .is_empty());

        // Due: only the newest head per (community, pubkey, d_tag).
        let due = query_due_reminders(&pool, (base + 120) as i64, 10)
            .await
            .expect("due");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, new.id.as_bytes().to_vec());
        assert_eq!(due[0].community_id, community);

        // Claim with stamp; second claim loses; release restores.
        let created = datetime_from_secs((base + 10) as i64).expect("ts");
        assert!(
            claim_due_reminder_with_stamp(&pool, community, new.id.as_bytes(), created, 42)
                .await
                .expect("claim")
        );
        assert!(
            !claim_due_reminder_with_stamp(&pool, community, new.id.as_bytes(), created, 43)
                .await
                .expect("second claim")
        );
        // Wrong stamp cannot release.
        assert!(
            !release_due_reminder(&pool, community, new.id.as_bytes(), created, 43)
                .await
                .expect("wrong-stamp release")
        );
        assert!(
            release_due_reminder(&pool, community, new.id.as_bytes(), created, 42)
                .await
                .expect("release")
        );
        assert_eq!(
            query_due_reminders(&pool, (base + 120) as i64, 10)
                .await
                .expect("due after release")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn huddle_link_lookup_matches_creator_signed_content() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let parent = make_channel(&pool, community).await;
        let ephemeral = Uuid::new_v4();
        let creator = Keys::generate();

        let content = serde_json::json!({ "ephemeral_channel_id": ephemeral.to_string() });
        let start = EventBuilder::new(Kind::Custom(48100), content.to_string())
            .sign_with_keys(&creator)
            .expect("sign huddle start");
        insert_event(&pool, community, &start, Some(parent))
            .await
            .expect("insert start");

        assert!(huddle_started_link_exists(
            &pool,
            community,
            parent,
            ephemeral,
            &creator.public_key().to_bytes(),
        )
        .await
        .expect("link exists"));

        // Different creator does not match.
        assert!(!huddle_started_link_exists(
            &pool,
            community,
            parent,
            ephemeral,
            &Keys::generate().public_key().to_bytes(),
        )
        .await
        .expect("wrong creator"));
    }

    #[tokio::test]
    async fn publish_nip43_snapshot_replaces_prior_and_counts_members() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let relay_keys = Keys::generate();

        for (pk, role) in [("aa".repeat(32), "owner"), ("bb".repeat(32), "member")] {
            sqlx::query(
                "INSERT INTO relay_members (community_id, pubkey, role) VALUES (?1, ?2, ?3)",
            )
            .bind(community.as_uuid().to_string())
            .bind(pk)
            .bind(role)
            .execute(&pool)
            .await
            .expect("insert member");
        }

        let (snap1, inserted, count) =
            publish_nip43_membership_locked(&pool, community, &relay_keys)
                .await
                .expect("publish snapshot");
        assert!(inserted);
        assert_eq!(count, 2);
        assert_eq!(
            snap1
                .event
                .tags
                .iter()
                .filter(|t| t.as_slice().first().map(String::as_str) == Some("member"))
                .count(),
            2
        );

        // Membership changes; the next snapshot must replace the prior one.
        // (An identical same-second republish would sign the same event id and
        // be reported as a duplicate — parity with the Postgres arm.)
        sqlx::query(
            "INSERT INTO relay_members (community_id, pubkey, role) VALUES (?1, ?2, 'member')",
        )
        .bind(community.as_uuid().to_string())
        .bind("cc".repeat(32))
        .execute(&pool)
        .await
        .expect("insert third member");
        let (_snap2, inserted2, count2) =
            publish_nip43_membership_locked(&pool, community, &relay_keys)
                .await
                .expect("publish second snapshot");
        assert!(inserted2);
        assert_eq!(count2, 3);
        // Prior snapshot retired.
        assert!(get_event_by_id(&pool, community, snap1.event.id.as_bytes())
            .await
            .expect("prior snapshot")
            .is_none());
        let heads = query_events(
            &pool,
            &EventQuery {
                kinds: Some(vec![KIND_NIP43_MEMBERSHIP_LIST as i32]),
                global_only: true,
                ..EventQuery::for_community(community)
            },
        )
        .await
        .expect("live snapshots");
        assert_eq!(heads.len(), 1);
    }

    #[tokio::test]
    async fn soft_delete_discovery_events_scopes_kind_and_author() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let channel = make_channel(&pool, community).await;
        let relay = Keys::generate();

        let d39000 = EventBuilder::new(Kind::Custom(39000), "meta")
            .sign_with_keys(&relay)
            .expect("sign");
        let plain = EventBuilder::new(Kind::Custom(9), "chat")
            .sign_with_keys(&relay)
            .expect("sign");
        insert_event(&pool, community, &d39000, Some(channel))
            .await
            .expect("insert 39000");
        insert_event(&pool, community, &plain, Some(channel))
            .await
            .expect("insert 9");

        let n =
            soft_delete_discovery_events(&pool, community, channel, &relay.public_key().to_bytes())
                .await
                .expect("delete discovery");
        assert_eq!(n, 1);
        assert!(get_event_by_id(&pool, community, plain.id.as_bytes())
            .await
            .expect("plain")
            .is_some());
    }
}

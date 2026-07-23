//! SQLite arms for channel CRUD and membership (WP2, Phase 2d).
//!
//! Function-for-function port of the Solo-reachable operations in
//! [`crate::channel`]. Semantics match the Postgres arm:
//!
//! - every predicate leads with `community_id` (tenant scoping),
//! - soft-deleted channels are invisible to reads (`deleted_at IS NULL`),
//! - membership upserts re-activate removed rows idempotently,
//! - role grants are enforced inside the same transaction as the write.
//!
//! Postgres-only machinery is translated per `docs/phase2/sqlite-schema-notes.md`:
//! `NOW()` → `unixepoch()` (INTEGER unix seconds, converted to
//! `DateTime<Utc>` at the Rust boundary), UUIDs bind as lowercase hyphenated
//! TEXT (`uuid::fmt::Hyphenated`), enum casts (`::channel_type`) are dropped
//! (TEXT + CHECK columns), and every `pg_advisory_xact_lock` is replaced by a
//! `BEGIN IMMEDIATE` transaction — SQLite has a single writer, so an
//! immediate write transaction provides the same serialization the advisory
//! locks bought on Postgres (see `update_channel` /
//! `reap_expired_ephemeral_channels` for the TTL-trigger interplay).

use chrono::{DateTime, Utc};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::fmt::Hyphenated;
use uuid::Uuid;

use crate::channel::{
    AccessibleChannel, ChannelRecord, ChannelType, ChannelUpdate, ChannelVisibility, MemberRecord,
    MemberRole, ReapedEphemeralChannel, UserRecord,
};
use crate::error::{DbError, Result};
use buzz_core::CommunityId;

/// Convert stored INTEGER unix seconds into the `DateTime<Utc>` the shared
/// record types carry.
pub(crate) fn datetime_from_unix(secs: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(secs, 0).ok_or(DbError::InvalidTimestamp(secs))
}

/// Optional-column variant of [`datetime_from_unix`].
pub(crate) fn opt_datetime_from_unix(secs: Option<i64>) -> Result<Option<DateTime<Utc>>> {
    secs.map(datetime_from_unix).transpose()
}

/// Begin a `BEGIN IMMEDIATE` write transaction.
///
/// This is the SQLite replacement for every Postgres advisory-lock +
/// transaction pattern: the immediate transaction takes the database write
/// lock up front, and SQLite's single-writer model serializes the whole
/// read-check-write sequence end-to-end.
pub(crate) async fn begin_immediate(pool: &SqlitePool) -> Result<Transaction<'static, Sqlite>> {
    Ok(pool.begin_with("BEGIN IMMEDIATE").await?)
}

/// Shared channel column list (no enum casts — TEXT columns are read as-is).
const CHANNEL_COLUMNS: &str = "id, name, channel_type, visibility, description, canvas, \
     created_by, created_at, updated_at, archived_at, deleted_at, \
     nip29_group_id, topic_required, max_members, \
     topic, topic_set_by, topic_set_at, \
     purpose, purpose_set_by, purpose_set_at, \
     ttl_seconds, ttl_deadline";

fn row_to_channel_record(row: sqlx::sqlite::SqliteRow) -> Result<ChannelRecord> {
    let id: Hyphenated = row.try_get("id")?;
    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    Ok(ChannelRecord {
        id: id.into_uuid(),
        name: row.try_get("name")?,
        channel_type: row.try_get("channel_type")?,
        visibility: row.try_get("visibility")?,
        description: row.try_get("description")?,
        canvas: row.try_get("canvas")?,
        created_by: row.try_get("created_by")?,
        created_at: datetime_from_unix(created_at)?,
        updated_at: datetime_from_unix(updated_at)?,
        archived_at: opt_datetime_from_unix(row.try_get("archived_at")?)?,
        deleted_at: opt_datetime_from_unix(row.try_get("deleted_at")?)?,
        nip29_group_id: row.try_get("nip29_group_id")?,
        topic_required: row.try_get("topic_required")?,
        max_members: row.try_get("max_members")?,
        topic: row.try_get("topic")?,
        topic_set_by: row.try_get("topic_set_by")?,
        topic_set_at: opt_datetime_from_unix(row.try_get("topic_set_at")?)?,
        purpose: row.try_get("purpose")?,
        purpose_set_by: row.try_get("purpose_set_by")?,
        purpose_set_at: opt_datetime_from_unix(row.try_get("purpose_set_at")?)?,
        ttl_seconds: row.try_get("ttl_seconds")?,
        ttl_deadline: opt_datetime_from_unix(row.try_get("ttl_deadline")?)?,
    })
}

fn row_to_member_record(row: sqlx::sqlite::SqliteRow) -> Result<MemberRecord> {
    let channel_id: Hyphenated = row.try_get("channel_id")?;
    let joined_at: i64 = row.try_get("joined_at")?;
    Ok(MemberRecord {
        channel_id: channel_id.into_uuid(),
        pubkey: row.try_get("pubkey")?,
        role: row.try_get("role")?,
        joined_at: datetime_from_unix(joined_at)?,
        invited_by: row.try_get("invited_by")?,
        removed_at: opt_datetime_from_unix(row.try_get("removed_at")?)?,
    })
}

fn validate_pubkey(pubkey: &[u8]) -> Result<()> {
    if pubkey.len() != 32 {
        return Err(DbError::InvalidData(format!(
            "pubkey must be 32 bytes, got {}",
            pubkey.len()
        )));
    }
    Ok(())
}

/// Owner-bootstrap / re-activation upsert shared by the create paths and
/// `add_member`. Mirrors the Postgres `ON CONFLICT … DO UPDATE` exactly.
async fn upsert_member_tx(
    tx: &mut Transaction<'_, Sqlite>,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
    role: &str,
    invited_by: Option<&[u8]>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO channel_members (community_id, channel_id, pubkey, role, invited_by) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE SET \
             removed_at = NULL, \
             removed_by = NULL, \
             role = excluded.role",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .bind(role)
    .bind(invited_by)
    .execute(tx.as_mut())
    .await?;
    Ok(())
}

async fn fetch_channel_tx(
    tx: &mut Transaction<'_, Sqlite>,
    community_id: CommunityId,
    channel_id: Uuid,
    include_deleted: bool,
) -> Result<ChannelRecord> {
    let deleted_clause = if include_deleted {
        ""
    } else {
        " AND deleted_at IS NULL"
    };
    let sql = format!(
        "SELECT {CHANNEL_COLUMNS} FROM channels WHERE community_id = ?1 AND id = ?2{deleted_clause}"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_id.as_uuid().hyphenated())
        .bind(channel_id.hyphenated())
        .fetch_optional(tx.as_mut())
        .await?
        .ok_or(DbError::ChannelNotFound(channel_id))?;
    row_to_channel_record(row)
}

/// Transaction-scoped active-role lookup (no channel soft-delete join —
/// callers have already resolved the channel).
async fn get_active_role_tx(
    tx: &mut Transaction<'_, Sqlite>,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT role FROM channel_members \
         WHERE community_id = ?1 AND channel_id = ?2 AND pubkey = ?3 AND removed_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .fetch_optional(tx.as_mut())
    .await?;
    Ok(row.map(|r| r.try_get("role")).transpose()?)
}

/// Creates a new channel, bootstraps the creator as owner, and returns the record.
///
/// TTL deadline is computed as `unixepoch() + ttl_seconds` (the Postgres arm's
/// `NOW() + interval`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    name: &str,
    channel_type: ChannelType,
    visibility: ChannelVisibility,
    description: Option<&str>,
    created_by: &[u8],
    ttl_seconds: Option<i32>,
) -> Result<ChannelRecord> {
    validate_pubkey(created_by)?;
    let id = Uuid::new_v4();
    let mut tx = begin_immediate(pool).await?;

    sqlx::query(
        "INSERT INTO channels \
             (id, community_id, name, channel_type, visibility, description, created_by, ttl_seconds, ttl_deadline) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, \
                 CASE WHEN ?8 IS NOT NULL THEN unixepoch() + ?8 ELSE NULL END)",
    )
    .bind(id.hyphenated())
    .bind(community_id.as_uuid().hyphenated())
    .bind(name)
    .bind(channel_type.as_str())
    .bind(visibility.as_str())
    .bind(description)
    .bind(created_by)
    .bind(ttl_seconds)
    .execute(tx.as_mut())
    .await?;

    upsert_member_tx(
        &mut tx,
        community_id,
        id,
        created_by,
        "owner",
        Some(created_by),
    )
    .await?;

    let record = fetch_channel_tx(&mut tx, community_id, id, true).await?;
    tx.commit().await?;
    Ok(record)
}

/// Creates a channel with a client-supplied UUID (idempotent via
/// `ON CONFLICT DO NOTHING`).
///
/// Returns `(record, true)` if newly created, `(record, false)` if a channel
/// with `channel_id` already exists in this community.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_channel_with_id(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    name: &str,
    channel_type: ChannelType,
    visibility: ChannelVisibility,
    description: Option<&str>,
    created_by: &[u8],
    ttl_seconds: Option<i32>,
) -> Result<(ChannelRecord, bool)> {
    validate_pubkey(created_by)?;
    if channel_id.is_nil() {
        return Err(DbError::InvalidData(
            "channel_id must not be nil (reserved for global fan-out)".into(),
        ));
    }

    let mut tx = begin_immediate(pool).await?;

    let rows_affected = sqlx::query(
        "INSERT INTO channels \
             (id, community_id, name, channel_type, visibility, description, created_by, ttl_seconds, ttl_deadline) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, \
                 CASE WHEN ?8 IS NOT NULL THEN unixepoch() + ?8 ELSE NULL END) \
         ON CONFLICT (community_id, id) DO NOTHING",
    )
    .bind(channel_id.hyphenated())
    .bind(community_id.as_uuid().hyphenated())
    .bind(name)
    .bind(channel_type.as_str())
    .bind(visibility.as_str())
    .bind(description)
    .bind(created_by)
    .bind(ttl_seconds)
    .execute(tx.as_mut())
    .await?
    .rows_affected();

    let was_created = rows_affected > 0;
    if was_created {
        upsert_member_tx(
            &mut tx,
            community_id,
            channel_id,
            created_by,
            "owner",
            Some(created_by),
        )
        .await?;
    }

    let record = fetch_channel_tx(&mut tx, community_id, channel_id, true).await?;
    tx.commit().await?;
    Ok((record, was_created))
}

/// Fetches a channel record by `(community_id, id)`. Returns
/// `ChannelNotFound` if missing or soft-deleted.
pub(crate) async fn get_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<ChannelRecord> {
    let sql = format!(
        "SELECT {CHANNEL_COLUMNS} FROM channels \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_id.as_uuid().hyphenated())
        .bind(channel_id.hyphenated())
        .fetch_optional(pool)
        .await?
        .ok_or(DbError::ChannelNotFound(channel_id))?;
    row_to_channel_record(row)
}

/// Returns the canvas content for a channel, if any.
pub(crate) async fn get_canvas(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT canvas FROM channels WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .fetch_optional(pool)
    .await?
    .ok_or(DbError::ChannelNotFound(channel_id))?;
    Ok(row.try_get("canvas")?)
}

/// Sets or clears the canvas content for a channel.
pub(crate) async fn set_canvas(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    canvas: Option<&str>,
) -> Result<()> {
    let rows = sqlx::query(
        "UPDATE channels SET canvas = ?1 \
         WHERE community_id = ?2 AND id = ?3 AND deleted_at IS NULL",
    )
    .bind(canvas)
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    if rows.rows_affected() == 0 {
        return Err(DbError::ChannelNotFound(channel_id));
    }
    Ok(())
}

/// Add a member to a channel.
///
/// Role enforcement matches the Postgres arm:
/// - Open channels: anyone may join; elevated roles require an owner/admin granter.
/// - Private channels: require an `invited_by` who is an active member
///   (creator self-bootstrap excepted); elevated roles require an elevated inviter.
///
/// The check-then-insert sequence runs inside one immediate transaction so
/// the inviter's role cannot change between the check and the write.
pub(crate) async fn add_member(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
    role: MemberRole,
    invited_by: Option<&[u8]>,
) -> Result<MemberRecord> {
    validate_pubkey(pubkey)?;
    let mut tx = begin_immediate(pool).await?;

    let channel = fetch_channel_tx(&mut tx, community_id, channel_id, false).await?;

    let effective_role = if channel.visibility == "private" {
        let inviter = invited_by.ok_or_else(|| {
            DbError::AccessDenied("private channel requires an invite".to_string())
        })?;

        // Bootstrap: channel creator may add themselves as the first member.
        let is_creator_bootstrap = inviter == pubkey && inviter == channel.created_by.as_slice();

        if !is_creator_bootstrap {
            let inviter_role_str = get_active_role_tx(&mut tx, community_id, channel_id, inviter)
                .await?
                .ok_or_else(|| {
                    DbError::AccessDenied("inviter is not an active member".to_string())
                })?;

            let inviter_role: MemberRole = inviter_role_str.parse().map_err(|_| {
                DbError::InvalidData(format!("invalid role in database: {inviter_role_str}"))
            })?;

            // Any member can invite others, but only owners/admins may grant
            // elevated roles.
            if role.is_elevated() && !inviter_role.is_elevated() {
                return Err(DbError::AccessDenied(
                    "only owners/admins may grant elevated roles".to_string(),
                ));
            }
        }

        role
    } else {
        // Open channel: anyone may join, but only existing owners/admins may
        // grant elevated roles.
        if role.is_elevated() {
            let granter_role = match invited_by {
                Some(inv) => get_active_role_tx(&mut tx, community_id, channel_id, inv).await?,
                None => None,
            };
            match granter_role.as_deref() {
                Some("owner") | Some("admin") => role,
                _ => {
                    return Err(DbError::AccessDenied(
                        "only owners/admins may grant elevated roles".to_string(),
                    ))
                }
            }
        } else {
            role
        }
    };

    upsert_member_tx(
        &mut tx,
        community_id,
        channel_id,
        pubkey,
        effective_role.as_str(),
        invited_by,
    )
    .await?;

    let row = sqlx::query(
        "SELECT channel_id, pubkey, role, joined_at, invited_by, removed_at \
         FROM channel_members WHERE community_id = ?1 AND channel_id = ?2 AND pubkey = ?3",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .fetch_one(tx.as_mut())
    .await?;

    let record = row_to_member_record(row)?;
    tx.commit().await?;
    Ok(record)
}

/// Remove a member from a channel (soft delete).
///
/// `actor_pubkey` must be an active owner/admin, the agent's owner, or the
/// member removing themselves. Refuses to remove the last owner. The role
/// check and the UPDATE run inside one immediate transaction. Unlike the
/// Postgres arm (which reads `is_agent_owner` from the pool because
/// `agent_owner_pubkey` is immutable), the lookup here runs on the same
/// transaction connection — a second pool acquire would deadlock a
/// single-connection SQLite pool, and the read is equivalent either way.
pub(crate) async fn remove_member(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
    actor_pubkey: &[u8],
) -> Result<()> {
    let mut tx = begin_immediate(pool).await?;

    let is_self_remove = pubkey == actor_pubkey;
    if !is_self_remove {
        let actor_role_str = get_active_role_tx(&mut tx, community_id, channel_id, actor_pubkey)
            .await?
            .ok_or_else(|| DbError::AccessDenied("actor is not an active member".to_string()))?;
        let actor_role: MemberRole = actor_role_str.parse().map_err(|_| {
            DbError::InvalidData(format!("invalid role in database: {actor_role_str}"))
        })?;
        if !actor_role.is_elevated()
            && !is_agent_owner_tx(&mut tx, community_id, pubkey, actor_pubkey).await?
        {
            return Err(DbError::AccessDenied(
                "only owners/admins or the agent's owner may remove other members".to_string(),
            ));
        }
    }

    // Defense-in-depth: prevent removing the last owner regardless of caller.
    let target_role = get_active_role_tx(&mut tx, community_id, channel_id, pubkey).await?;
    if target_role.as_deref() == Some("owner") {
        let owner_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM channel_members \
             WHERE community_id = ?1 AND channel_id = ?2 AND role = 'owner' AND removed_at IS NULL",
        )
        .bind(community_id.as_uuid().hyphenated())
        .bind(channel_id.hyphenated())
        .fetch_one(tx.as_mut())
        .await?;
        if owner_count <= 1 {
            return Err(DbError::AccessDenied(
                "cannot remove the last owner — transfer ownership first".to_string(),
            ));
        }
    }

    let result = sqlx::query(
        "UPDATE channel_members \
         SET removed_at = unixepoch(), removed_by = ?1 \
         WHERE community_id = ?2 AND channel_id = ?3 AND pubkey = ?4 AND removed_at IS NULL",
    )
    .bind(actor_pubkey)
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .execute(tx.as_mut())
    .await?;

    if result.rows_affected() == 0 {
        return Err(DbError::MemberNotFound(channel_id));
    }

    tx.commit().await?;
    Ok(())
}

/// SQLite arm of `crate::user::is_agent_owner` — needed by [`remove_member`]
/// until the WP3 users packet lands its own module. Runs on the caller's
/// transaction connection (see [`remove_member`] docs).
async fn is_agent_owner_tx(
    tx: &mut Transaction<'_, Sqlite>,
    community_id: CommunityId,
    target_pubkey: &[u8],
    actor_pubkey: &[u8],
) -> Result<bool> {
    let row = sqlx::query_scalar::<_, bool>(
        "SELECT agent_owner_pubkey = ?3 FROM users \
         WHERE community_id = ?1 AND pubkey = ?2 AND agent_owner_pubkey IS NOT NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(target_pubkey)
    .bind(actor_pubkey)
    .fetch_optional(tx.as_mut())
    .await?;
    Ok(row.unwrap_or(false))
}

/// Returns `true` if the given pubkey is an active member of a non-deleted
/// channel.
pub(crate) async fn is_member(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
) -> Result<bool> {
    let cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM channel_members cm \
         JOIN channels c ON cm.community_id = c.community_id AND cm.channel_id = c.id AND c.deleted_at IS NULL \
         WHERE cm.community_id = ?1 AND cm.channel_id = ?2 AND cm.pubkey = ?3 AND cm.removed_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .fetch_one(pool)
    .await?;
    Ok(cnt > 0)
}

/// Returns all active members of the given channel (empty if the channel is
/// soft-deleted). Ordered by `joined_at`, capped at 1000.
pub(crate) async fn get_members(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<Vec<MemberRecord>> {
    let rows = sqlx::query(
        "SELECT cm.channel_id, cm.pubkey, cm.role, cm.joined_at, cm.invited_by, cm.removed_at \
         FROM channel_members cm \
         JOIN channels c ON cm.community_id = c.community_id AND cm.channel_id = c.id AND c.deleted_at IS NULL \
         WHERE cm.community_id = ?1 AND cm.channel_id = ?2 AND cm.removed_at IS NULL \
         ORDER BY cm.joined_at ASC \
         LIMIT 1000",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_member_record).collect()
}

/// Returns active members for multiple channels in a single query.
///
/// Flat result ordered by `joined_at`; empty input short-circuits without a
/// query. The Postgres `= ANY(uuid[])` bind becomes a built `IN (…)` list.
pub(crate) async fn get_members_bulk(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_ids: &[Uuid],
) -> Result<Vec<MemberRecord>> {
    if channel_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(
        "SELECT cm.channel_id, cm.pubkey, cm.role, cm.joined_at, cm.invited_by, cm.removed_at \
         FROM channel_members cm \
         JOIN channels c ON cm.community_id = c.community_id AND cm.channel_id = c.id AND c.deleted_at IS NULL \
         WHERE cm.community_id = ",
    );
    qb.push_bind(community_id.as_uuid().hyphenated());
    qb.push(" AND cm.removed_at IS NULL AND cm.channel_id IN (");
    let mut sep = qb.separated(", ");
    for id in channel_ids {
        sep.push_bind(id.hyphenated());
    }
    qb.push(") ORDER BY cm.joined_at ASC");

    let rows = qb.build().fetch_all(pool).await?;
    rows.into_iter().map(row_to_member_record).collect()
}

/// Get all channel IDs accessible to a pubkey: active memberships plus all
/// open channels (union), excluding soft-deleted channels.
pub(crate) async fn get_accessible_channel_ids(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(
        "SELECT cm.channel_id \
         FROM channel_members cm \
         JOIN channels c ON cm.community_id = c.community_id AND cm.channel_id = c.id AND c.deleted_at IS NULL \
         WHERE cm.community_id = ?1 AND cm.pubkey = ?2 AND cm.removed_at IS NULL \
         UNION \
         SELECT id AS channel_id \
         FROM channels \
         WHERE community_id = ?1 AND visibility = 'open' AND deleted_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(pubkey)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            let id: Hyphenated = r.try_get("channel_id")?;
            Ok(id.into_uuid())
        })
        .collect()
}

/// Lists channels in a community, optionally filtered by visibility string.
pub(crate) async fn list_channels(
    pool: &SqlitePool,
    community_id: CommunityId,
    visibility: Option<&str>,
) -> Result<Vec<ChannelRecord>> {
    let rows = if let Some(vis) = visibility {
        let sql = format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels \
             WHERE community_id = ?1 AND deleted_at IS NULL AND visibility = ?2 \
             ORDER BY created_at DESC LIMIT 1000"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community_id.as_uuid().hyphenated())
            .bind(vis)
            .fetch_all(pool)
            .await?
    } else {
        let sql = format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels \
             WHERE community_id = ?1 AND deleted_at IS NULL \
             ORDER BY created_at DESC LIMIT 1000"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community_id.as_uuid().hyphenated())
            .fetch_all(pool)
            .await?
    };
    rows.into_iter().map(row_to_channel_record).collect()
}

/// Returns full channel records for all channels a user can access: open
/// channels plus channels where the user is an active member.
///
/// The Postgres `array_position(ARRAY['stream','forum','dm'], …)` ordering is
/// rewritten as a CASE ramp (unknown types sort last, matching PG's
/// NULLS-LAST default).
pub(crate) async fn get_accessible_channels(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
    visibility_filter: Option<&str>,
    member_only: Option<bool>,
) -> Result<Vec<AccessibleChannel>> {
    let membership_clause = if member_only == Some(true) {
        "AND cm.channel_id IS NOT NULL"
    } else {
        "AND (c.visibility = 'open' OR cm.channel_id IS NOT NULL)"
    };

    let visibility_clause = if visibility_filter.is_some() {
        "AND c.visibility = ?3"
    } else {
        ""
    };

    let sql = format!(
        "SELECT c.id, c.name, c.channel_type, c.visibility, c.description, c.canvas, \
                c.created_by, c.created_at, c.updated_at, c.archived_at, c.deleted_at, \
                c.nip29_group_id, c.topic_required, c.max_members, \
                c.topic, c.topic_set_by, c.topic_set_at, \
                c.purpose, c.purpose_set_by, c.purpose_set_at, \
                c.ttl_seconds, c.ttl_deadline, \
                (cm.channel_id IS NOT NULL) AS is_member \
         FROM channels c \
         LEFT JOIN channel_members cm \
             ON c.community_id = cm.community_id AND c.id = cm.channel_id \
                AND cm.pubkey = ?2 AND cm.removed_at IS NULL \
         WHERE c.community_id = ?1 AND c.deleted_at IS NULL \
           {membership_clause} \
           AND (c.channel_type != 'dm' OR cm.hidden_at IS NULL) \
           {visibility_clause} \
         ORDER BY CASE c.channel_type \
                      WHEN 'stream' THEN 1 WHEN 'forum' THEN 2 WHEN 'dm' THEN 3 ELSE 4 \
                  END, c.name \
         LIMIT 1000"
    );

    let query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_id.as_uuid().hyphenated())
        .bind(pubkey);
    let query = if let Some(vis) = visibility_filter {
        query.bind(vis)
    } else {
        query
    };

    let rows = query.fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let is_member: bool = row.try_get("is_member").unwrap_or(false);
            let channel = row_to_channel_record(row)?;
            Ok(AccessibleChannel { channel, is_member })
        })
        .collect()
}

/// Bulk-fetch user records by pubkey inside one community. Empty input
/// short-circuits; ordering is unspecified.
pub(crate) async fn get_users_bulk(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkeys: &[Vec<u8>],
) -> Result<Vec<UserRecord>> {
    if pubkeys.is_empty() {
        return Ok(Vec::new());
    }
    let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(
        "SELECT pubkey, display_name, avatar_url, nip05_handle \
         FROM users WHERE community_id = ",
    );
    qb.push_bind(community_id.as_uuid().hyphenated());
    qb.push(" AND pubkey IN (");
    let mut sep = qb.separated(", ");
    for pk in pubkeys {
        sep.push_bind(pk.as_slice());
    }
    qb.push(")");

    let rows = qb.build().fetch_all(pool).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(UserRecord {
            pubkey: row.try_get("pubkey")?,
            display_name: row.try_get("display_name")?,
            avatar_url: row.try_get("avatar_url")?,
            nip05_handle: row.try_get("nip05_handle")?,
        });
    }
    Ok(out)
}

/// Updates channel metadata dynamically (name / description / visibility /
/// TTL). At least one field must be provided.
///
/// A TTL change resets `ttl_deadline = unixepoch() + ttl_seconds` (or clears
/// it). The Postgres arm serialized TTL transitions against the event-side
/// TTL-refresh trigger with a per-channel advisory lock pair; here the whole
/// UPDATE runs in a `BEGIN IMMEDIATE` transaction, and SQLite's single writer
/// guarantees the in-schema `trg_events_refresh_channel_ttl` trigger either
/// sees the committed TTL or strictly precedes this transition.
pub(crate) async fn update_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    updates: ChannelUpdate,
) -> Result<ChannelRecord> {
    if updates.name.is_none()
        && updates.description.is_none()
        && updates.visibility.is_none()
        && updates.ttl_seconds.is_none()
    {
        return Err(DbError::InvalidData(
            "at least one field must be provided for update".to_string(),
        ));
    }

    let mut set_parts: Vec<String> = Vec::new();
    let mut param_idx: usize = 1;
    if updates.name.is_some() {
        set_parts.push(format!("name = ?{param_idx}"));
        param_idx += 1;
    }
    if updates.description.is_some() {
        set_parts.push(format!("description = ?{param_idx}"));
        param_idx += 1;
    }
    if updates.visibility.is_some() {
        set_parts.push(format!("visibility = ?{param_idx}"));
        param_idx += 1;
    }
    if let Some(ref ttl) = updates.ttl_seconds {
        set_parts.push(format!("ttl_seconds = ?{param_idx}"));
        match ttl {
            Some(_) => set_parts.push(format!("ttl_deadline = unixepoch() + ?{param_idx}")),
            None => set_parts.push("ttl_deadline = NULL".to_string()),
        }
        param_idx += 1;
    }
    let community_param_idx = param_idx;
    let channel_param_idx = param_idx + 1;
    let sql = format!(
        "UPDATE channels SET {}, updated_at = unixepoch() \
         WHERE community_id = ?{community_param_idx} AND id = ?{channel_param_idx} AND deleted_at IS NULL",
        set_parts.join(", ")
    );

    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    if let Some(ref name) = updates.name {
        q = q.bind(name);
    }
    if let Some(ref desc) = updates.description {
        q = q.bind(desc);
    }
    if let Some(ref vis) = updates.visibility {
        q = q.bind(vis);
    }
    if let Some(ref ttl) = updates.ttl_seconds {
        q = q.bind(*ttl);
    }
    q = q.bind(community_id.as_uuid().hyphenated());
    q = q.bind(channel_id.hyphenated());

    if updates.ttl_seconds.is_some() {
        // Immediate transaction replaces the Postgres advisory-lock pair
        // (see fn docs) — the write lock alone serializes against the
        // event-insert TTL trigger.
        let mut tx = begin_immediate(pool).await?;
        let result = q.execute(tx.as_mut()).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::ChannelNotFound(channel_id));
        }
        tx.commit().await?;
    } else {
        let result = q.execute(pool).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::ChannelNotFound(channel_id));
        }
    }

    get_channel(pool, community_id, channel_id).await
}

/// Sets the topic for a channel, recording who set it and when.
pub(crate) async fn set_topic(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    topic: &str,
    set_by: &[u8],
) -> Result<()> {
    let result = sqlx::query(
        "UPDATE channels SET topic = ?1, topic_set_by = ?2, topic_set_at = unixepoch() \
         WHERE community_id = ?3 AND id = ?4 AND deleted_at IS NULL",
    )
    .bind(topic)
    .bind(set_by)
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::ChannelNotFound(channel_id));
    }
    Ok(())
}

/// Sets the purpose for a channel, recording who set it and when.
pub(crate) async fn set_purpose(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    purpose: &str,
    set_by: &[u8],
) -> Result<()> {
    let result = sqlx::query(
        "UPDATE channels SET purpose = ?1, purpose_set_by = ?2, purpose_set_at = unixepoch() \
         WHERE community_id = ?3 AND id = ?4 AND deleted_at IS NULL",
    )
    .bind(purpose)
    .bind(set_by)
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::ChannelNotFound(channel_id));
    }
    Ok(())
}

/// Archives a channel. `AccessDenied` if already archived; `ChannelNotFound`
/// if missing or deleted.
pub(crate) async fn archive_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT archived_at FROM channels \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .fetch_optional(pool)
    .await?;

    match row {
        None => return Err(DbError::ChannelNotFound(channel_id)),
        Some(r) => {
            let archived_at: Option<i64> = r.try_get("archived_at")?;
            if archived_at.is_some() {
                return Err(DbError::AccessDenied(
                    "channel is already archived".to_string(),
                ));
            }
        }
    }

    sqlx::query(
        "UPDATE channels SET archived_at = unixepoch() \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL AND archived_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    Ok(())
}

/// Unarchives a channel, recomputing the TTL deadline for ephemeral channels.
/// `AccessDenied` if not archived; `ChannelNotFound` if missing or deleted.
pub(crate) async fn unarchive_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT archived_at FROM channels \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .fetch_optional(pool)
    .await?;

    match row {
        None => return Err(DbError::ChannelNotFound(channel_id)),
        Some(r) => {
            let archived_at: Option<i64> = r.try_get("archived_at")?;
            if archived_at.is_none() {
                return Err(DbError::AccessDenied("channel is not archived".to_string()));
            }
        }
    }

    sqlx::query(
        "UPDATE channels SET archived_at = NULL, \
             ttl_deadline = CASE \
                 WHEN ttl_seconds IS NOT NULL THEN unixepoch() + ttl_seconds \
                 ELSE ttl_deadline \
             END \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL AND archived_at IS NOT NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    Ok(())
}

/// Soft-delete a channel. Returns `Ok(true)` if deleted, `Ok(false)` if
/// already deleted or not found.
pub(crate) async fn soft_delete_channel(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE channels SET deleted_at = unixepoch() \
         WHERE community_id = ?1 AND id = ?2 AND deleted_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Get the active role of a pubkey in a non-deleted channel, or `None`.
pub(crate) async fn get_member_role(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
    pubkey: &[u8],
) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT cm.role FROM channel_members cm \
         JOIN channels c ON cm.community_id = c.community_id AND cm.channel_id = c.id AND c.deleted_at IS NULL \
         WHERE cm.community_id = ?1 AND cm.channel_id = ?2 AND cm.pubkey = ?3 AND cm.removed_at IS NULL",
    )
    .bind(community_id.as_uuid().hyphenated())
    .bind(channel_id.hyphenated())
    .bind(pubkey)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.try_get("role")).transpose()?)
}

/// Archive ephemeral channels whose TTL deadline has passed, skipping
/// archived communities. Idempotent (`archived_at IS NULL` guard).
///
/// The Postgres arm is a single `UPDATE … FROM communities … RETURNING
/// c.host`; SQLite's RETURNING clause may only reference the updated table,
/// so the host lookup happens as a follow-up read inside the same
/// `BEGIN IMMEDIATE` transaction (equivalent atomicity: single writer).
pub(crate) async fn reap_expired_ephemeral_channels(
    pool: &SqlitePool,
) -> Result<Vec<ReapedEphemeralChannel>> {
    let mut tx = begin_immediate(pool).await?;

    let rows = sqlx::query(
        "UPDATE channels SET archived_at = unixepoch() \
         WHERE ttl_seconds IS NOT NULL \
           AND ttl_deadline < unixepoch() \
           AND archived_at IS NULL \
           AND deleted_at IS NULL \
           AND community_id IN (SELECT id FROM communities WHERE archived_at IS NULL) \
         RETURNING community_id, id",
    )
    .fetch_all(tx.as_mut())
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let community: Hyphenated = row.try_get("community_id")?;
        let channel: Hyphenated = row.try_get("id")?;
        let host: String = sqlx::query_scalar("SELECT host FROM communities WHERE id = ?1")
            .bind(community)
            .fetch_one(tx.as_mut())
            .await?;
        out.push(ReapedEphemeralChannel {
            community_id: CommunityId::from_uuid(community.into_uuid()),
            host,
            channel_id: channel.into_uuid(),
        });
    }

    tx.commit().await?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let host = format!("channel-test-{}.example", id.simple());
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.hyphenated())
            .bind(host)
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    fn pk(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    #[tokio::test]
    async fn create_and_get_round_trip() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let creator = pk(1);

        let record = create_channel(
            &pool,
            community,
            "general",
            ChannelType::Stream,
            ChannelVisibility::Open,
            Some("the general channel"),
            &creator,
            None,
        )
        .await
        .expect("create channel");

        assert_eq!(record.name, "general");
        assert_eq!(record.channel_type, "stream");
        assert_eq!(record.visibility, "open");
        assert_eq!(record.description.as_deref(), Some("the general channel"));
        assert_eq!(record.created_by, creator);
        assert!(record.archived_at.is_none());
        assert!(record.deleted_at.is_none());
        assert!(record.ttl_seconds.is_none());
        assert!(record.ttl_deadline.is_none());

        let fetched = get_channel(&pool, community, record.id)
            .await
            .expect("get channel");
        assert_eq!(fetched.id, record.id);
        assert_eq!(fetched.name, "general");
        assert_eq!(fetched.created_at, record.created_at);

        // Creator was bootstrapped as owner.
        let role = get_member_role(&pool, community, record.id, &creator)
            .await
            .expect("get role");
        assert_eq!(role.as_deref(), Some("owner"));

        // Uuid is stored as lowercase hyphenated TEXT (schema convention).
        let stored: String = sqlx::query_scalar("SELECT id FROM channels WHERE name = 'general'")
            .fetch_one(&pool)
            .await
            .expect("raw id");
        assert_eq!(stored, record.id.hyphenated().to_string());

        // TTL channels get a deadline in the future.
        let ephemeral = create_channel(
            &pool,
            community,
            "huddle",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &creator,
            Some(600),
        )
        .await
        .expect("create ephemeral");
        assert_eq!(ephemeral.ttl_seconds, Some(600));
        let deadline = ephemeral.ttl_deadline.expect("deadline set");
        assert!(deadline > Utc::now());
    }

    #[tokio::test]
    async fn create_channel_with_id_is_idempotent() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let creator = pk(2);
        let id = Uuid::new_v4();

        let (first, created) = create_channel_with_id(
            &pool,
            community,
            id,
            "fixed",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &creator,
            None,
        )
        .await
        .expect("first create");
        assert!(created);
        assert_eq!(first.id, id);

        let (second, created_again) = create_channel_with_id(
            &pool,
            community,
            id,
            "different-name",
            ChannelType::Forum,
            ChannelVisibility::Private,
            None,
            &pk(3),
            None,
        )
        .await
        .expect("second create");
        assert!(!created_again, "duplicate id must not create");
        assert_eq!(second.name, "fixed", "existing record wins");
        assert_eq!(second.channel_type, "stream");

        // Nil channel id is rejected.
        let err = create_channel_with_id(
            &pool,
            community,
            Uuid::nil(),
            "nil",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &creator,
            None,
        )
        .await;
        assert!(matches!(err, Err(DbError::InvalidData(_))));
    }

    #[tokio::test]
    async fn membership_add_remove_idempotence_and_roles() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(10);
        let member = pk(11);

        let channel = create_channel(
            &pool,
            community,
            "open",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("create");

        // Self-join an open channel.
        let rec = add_member(
            &pool,
            community,
            channel.id,
            &member,
            MemberRole::Member,
            None,
        )
        .await
        .expect("self join");
        assert_eq!(rec.role, "member");
        assert!(is_member(&pool, community, channel.id, &member)
            .await
            .expect("is_member"));

        // Adding twice is idempotent — still exactly one active row.
        add_member(
            &pool,
            community,
            channel.id,
            &member,
            MemberRole::Member,
            None,
        )
        .await
        .expect("re-join");
        let members = get_members(&pool, community, channel.id)
            .await
            .expect("members");
        assert_eq!(members.len(), 2, "owner + member, no duplicates");

        // Elevated self-grant on an open channel is denied.
        let stranger = pk(12);
        let err = add_member(
            &pool,
            community,
            channel.id,
            &stranger,
            MemberRole::Admin,
            None,
        )
        .await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));

        // Owner may grant admin.
        let rec = add_member(
            &pool,
            community,
            channel.id,
            &stranger,
            MemberRole::Admin,
            Some(&owner),
        )
        .await
        .expect("owner grants admin");
        assert_eq!(rec.role, "admin");

        // Plain member removing another member is denied.
        let err = remove_member(&pool, community, channel.id, &stranger, &member).await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));

        // Self-removal works; membership flips off.
        remove_member(&pool, community, channel.id, &member, &member)
            .await
            .expect("self remove");
        assert!(!is_member(&pool, community, channel.id, &member)
            .await
            .expect("is_member after removal"));

        // Removing an inactive member reports MemberNotFound.
        let err = remove_member(&pool, community, channel.id, &member, &member).await;
        assert!(matches!(err, Err(DbError::MemberNotFound(_))));

        // Re-adding re-activates the removed row (upsert clears removed_at).
        let rec = add_member(
            &pool,
            community,
            channel.id,
            &member,
            MemberRole::Member,
            None,
        )
        .await
        .expect("re-activate");
        assert!(rec.removed_at.is_none());
        assert!(is_member(&pool, community, channel.id, &member)
            .await
            .expect("is_member re-activated"));

        // The last owner can never be removed, even by themselves.
        let err = remove_member(&pool, community, channel.id, &owner, &owner).await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));
    }

    #[tokio::test]
    async fn private_channel_invite_rules() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let creator = pk(20);
        let invitee = pk(21);
        let outsider = pk(22);

        let channel = create_channel(
            &pool,
            community,
            "secret",
            ChannelType::Stream,
            ChannelVisibility::Private,
            None,
            &creator,
            None,
        )
        .await
        .expect("create private");

        // No invite — denied.
        let err = add_member(
            &pool,
            community,
            channel.id,
            &invitee,
            MemberRole::Member,
            None,
        )
        .await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));

        // Inviter who is not an active member — denied.
        let err = add_member(
            &pool,
            community,
            channel.id,
            &invitee,
            MemberRole::Member,
            Some(&outsider),
        )
        .await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));

        // Owner invites — allowed.
        add_member(
            &pool,
            community,
            channel.id,
            &invitee,
            MemberRole::Member,
            Some(&creator),
        )
        .await
        .expect("owner invites");

        // A plain member may invite, but cannot grant elevated roles.
        let second = pk(23);
        let err = add_member(
            &pool,
            community,
            channel.id,
            &second,
            MemberRole::Admin,
            Some(&invitee),
        )
        .await;
        assert!(matches!(err, Err(DbError::AccessDenied(_))));
        add_member(
            &pool,
            community,
            channel.id,
            &second,
            MemberRole::Member,
            Some(&invitee),
        )
        .await
        .expect("member invites member");
    }

    #[tokio::test]
    async fn agent_owner_may_remove_agent() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(30);
        let human = pk(31);
        let agent = pk(32);

        let channel = create_channel(
            &pool,
            community,
            "bots",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("create");

        for user in [&human, &agent] {
            sqlx::query("INSERT INTO users (community_id, pubkey) VALUES (?1, ?2)")
                .bind(community.as_uuid().hyphenated())
                .bind(user.as_slice())
                .execute(&pool)
                .await
                .expect("insert user");
            add_member(&pool, community, channel.id, user, MemberRole::Member, None)
                .await
                .expect("join");
        }
        sqlx::query(
            "UPDATE users SET agent_owner_pubkey = ?3 WHERE community_id = ?1 AND pubkey = ?2",
        )
        .bind(community.as_uuid().hyphenated())
        .bind(agent.as_slice())
        .bind(human.as_slice())
        .execute(&pool)
        .await
        .expect("set agent owner");

        // The agent's owner (a plain member) may remove the agent.
        remove_member(&pool, community, channel.id, &agent, &human)
            .await
            .expect("agent owner removes agent");
        assert!(!is_member(&pool, community, channel.id, &agent)
            .await
            .expect("agent removed"));
    }

    #[tokio::test]
    async fn accessible_channels_visibility() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(40);
        let insider = pk(41);
        let stranger = pk(42);

        let open = create_channel(
            &pool,
            community,
            "open-ch",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("open");
        let private = create_channel(
            &pool,
            community,
            "private-ch",
            ChannelType::Stream,
            ChannelVisibility::Private,
            None,
            &owner,
            None,
        )
        .await
        .expect("private");
        add_member(
            &pool,
            community,
            private.id,
            &insider,
            MemberRole::Member,
            Some(&owner),
        )
        .await
        .expect("invite insider");

        // Stranger sees only the open channel.
        let visible = get_accessible_channels(&pool, community, &stranger, None, None)
            .await
            .expect("stranger accessible");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].channel.id, open.id);
        assert!(!visible[0].is_member);

        let ids = get_accessible_channel_ids(&pool, community, &stranger)
            .await
            .expect("stranger ids");
        assert_eq!(ids, vec![open.id]);

        // Insider sees both; is_member reflects actual membership.
        let visible = get_accessible_channels(&pool, community, &insider, None, None)
            .await
            .expect("insider accessible");
        assert_eq!(visible.len(), 2);
        let priv_row = visible
            .iter()
            .find(|c| c.channel.id == private.id)
            .expect("private visible");
        assert!(priv_row.is_member);
        let open_row = visible
            .iter()
            .find(|c| c.channel.id == open.id)
            .expect("open visible");
        assert!(!open_row.is_member);

        // member_only restricts to actual memberships.
        let member_only = get_accessible_channels(&pool, community, &insider, None, Some(true))
            .await
            .expect("member only");
        assert_eq!(member_only.len(), 1);
        assert_eq!(member_only[0].channel.id, private.id);

        // Visibility filter.
        let only_open = get_accessible_channels(&pool, community, &insider, Some("open"), None)
            .await
            .expect("open filter");
        assert_eq!(only_open.len(), 1);
        assert_eq!(only_open[0].channel.id, open.id);

        let mut id_set = get_accessible_channel_ids(&pool, community, &insider)
            .await
            .expect("insider ids");
        id_set.sort();
        let mut expected = vec![open.id, private.id];
        expected.sort();
        assert_eq!(id_set, expected);
    }

    #[tokio::test]
    async fn cross_community_isolation_same_channel_uuid() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let owner_a = pk(50);
        let owner_b = pk(51);
        let id = Uuid::new_v4();

        let (chan_a, created_a) = create_channel_with_id(
            &pool,
            community_a,
            id,
            "a-side",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner_a,
            None,
        )
        .await
        .expect("create in A");
        let (chan_b, created_b) = create_channel_with_id(
            &pool,
            community_b,
            id,
            "b-side",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner_b,
            None,
        )
        .await
        .expect("create in B");
        assert!(created_a && created_b, "same UUID allowed per community");
        assert_eq!(chan_a.name, "a-side");
        assert_eq!(chan_b.name, "b-side");

        // Membership does not leak across communities.
        assert!(is_member(&pool, community_a, id, &owner_a)
            .await
            .expect("A member"));
        assert!(!is_member(&pool, community_b, id, &owner_a)
            .await
            .expect("A owner not in B"));

        // Soft-deleting A's channel leaves B's intact.
        assert!(soft_delete_channel(&pool, community_a, id)
            .await
            .expect("delete A"));
        assert!(matches!(
            get_channel(&pool, community_a, id).await,
            Err(DbError::ChannelNotFound(_))
        ));
        let still_b = get_channel(&pool, community_b, id).await.expect("B intact");
        assert_eq!(still_b.name, "b-side");

        // List scoping.
        let list_b = list_channels(&pool, community_b, None)
            .await
            .expect("list B");
        assert_eq!(list_b.len(), 1);
        assert_eq!(list_b[0].name, "b-side");
    }

    #[tokio::test]
    async fn soft_delete_and_archive_visibility() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(60);

        let channel = create_channel(
            &pool,
            community,
            "doomed",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            Some(300),
        )
        .await
        .expect("create");

        // Archive / unarchive state machine.
        archive_channel(&pool, community, channel.id)
            .await
            .expect("archive");
        let archived = get_channel(&pool, community, channel.id)
            .await
            .expect("archived still readable");
        assert!(archived.archived_at.is_some());
        assert!(matches!(
            archive_channel(&pool, community, channel.id).await,
            Err(DbError::AccessDenied(_))
        ));
        unarchive_channel(&pool, community, channel.id)
            .await
            .expect("unarchive");
        let revived = get_channel(&pool, community, channel.id)
            .await
            .expect("revived");
        assert!(revived.archived_at.is_none());
        assert!(
            revived.ttl_deadline.expect("deadline recomputed") > Utc::now(),
            "unarchive recomputes the TTL deadline"
        );
        assert!(matches!(
            unarchive_channel(&pool, community, channel.id).await,
            Err(DbError::AccessDenied(_))
        ));

        // Soft delete hides the channel from every read path.
        assert!(soft_delete_channel(&pool, community, channel.id)
            .await
            .expect("delete"));
        assert!(
            !soft_delete_channel(&pool, community, channel.id)
                .await
                .expect("second delete"),
            "soft delete is idempotent (false on repeat)"
        );
        assert!(matches!(
            get_channel(&pool, community, channel.id).await,
            Err(DbError::ChannelNotFound(_))
        ));
        assert!(!is_member(&pool, community, channel.id, &owner)
            .await
            .expect("membership hidden"));
        assert!(get_members(&pool, community, channel.id)
            .await
            .expect("members hidden")
            .is_empty());
        assert!(get_member_role(&pool, community, channel.id, &owner)
            .await
            .expect("role hidden")
            .is_none());
        assert!(list_channels(&pool, community, None)
            .await
            .expect("list hidden")
            .is_empty());
        assert!(matches!(
            archive_channel(&pool, community, channel.id).await,
            Err(DbError::ChannelNotFound(_))
        ));
    }

    #[tokio::test]
    async fn update_channel_and_metadata_setters() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(70);

        let channel = create_channel(
            &pool,
            community,
            "mutable",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("create");

        // Empty update rejected.
        assert!(matches!(
            update_channel(&pool, community, channel.id, ChannelUpdate::default()).await,
            Err(DbError::InvalidData(_))
        ));

        let updated = update_channel(
            &pool,
            community,
            channel.id,
            ChannelUpdate {
                name: Some("renamed".into()),
                description: Some("new desc".into()),
                visibility: Some("private".into()),
                ttl_seconds: Some(Some(900)),
            },
        )
        .await
        .expect("update");
        assert_eq!(updated.name, "renamed");
        assert_eq!(updated.description.as_deref(), Some("new desc"));
        assert_eq!(updated.visibility, "private");
        assert_eq!(updated.ttl_seconds, Some(900));
        assert!(updated.ttl_deadline.expect("deadline") > Utc::now());

        // Clearing the TTL clears the deadline.
        let cleared = update_channel(
            &pool,
            community,
            channel.id,
            ChannelUpdate {
                ttl_seconds: Some(None),
                ..Default::default()
            },
        )
        .await
        .expect("clear ttl");
        assert!(cleared.ttl_seconds.is_none());
        assert!(cleared.ttl_deadline.is_none());

        // Unknown channel maps to ChannelNotFound.
        assert!(matches!(
            update_channel(
                &pool,
                community,
                Uuid::new_v4(),
                ChannelUpdate {
                    name: Some("nope".into()),
                    ..Default::default()
                },
            )
            .await,
            Err(DbError::ChannelNotFound(_))
        ));

        // Topic / purpose / canvas setters round-trip.
        set_topic(&pool, community, channel.id, "the topic", &owner)
            .await
            .expect("set topic");
        set_purpose(&pool, community, channel.id, "the purpose", &owner)
            .await
            .expect("set purpose");
        set_canvas(&pool, community, channel.id, Some("# canvas"))
            .await
            .expect("set canvas");
        let record = get_channel(&pool, community, channel.id)
            .await
            .expect("get");
        assert_eq!(record.topic.as_deref(), Some("the topic"));
        assert_eq!(record.topic_set_by.as_deref(), Some(owner.as_slice()));
        assert!(record.topic_set_at.is_some());
        assert_eq!(record.purpose.as_deref(), Some("the purpose"));
        assert_eq!(
            get_canvas(&pool, community, channel.id)
                .await
                .expect("get canvas")
                .as_deref(),
            Some("# canvas")
        );
        set_canvas(&pool, community, channel.id, None)
            .await
            .expect("clear canvas");
        assert!(get_canvas(&pool, community, channel.id)
            .await
            .expect("get cleared canvas")
            .is_none());
    }

    #[tokio::test]
    async fn get_members_bulk_and_users_bulk() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(80);

        let a = create_channel(
            &pool,
            community,
            "bulk-a",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("a");
        let b = create_channel(
            &pool,
            community,
            "bulk-b",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("b");
        add_member(&pool, community, b.id, &pk(81), MemberRole::Member, None)
            .await
            .expect("join b");

        assert!(get_members_bulk(&pool, community, &[])
            .await
            .expect("empty input")
            .is_empty());
        let members = get_members_bulk(&pool, community, &[a.id, b.id])
            .await
            .expect("bulk");
        assert_eq!(members.len(), 3);

        // Users bulk is community-scoped.
        sqlx::query(
            "INSERT INTO users (community_id, pubkey, display_name) VALUES (?1, ?2, 'alice')",
        )
        .bind(community.as_uuid().hyphenated())
        .bind(pk(81))
        .execute(&pool)
        .await
        .expect("insert user");
        let other = make_community(&pool).await;
        sqlx::query(
            "INSERT INTO users (community_id, pubkey, display_name) VALUES (?1, ?2, 'other-alice')",
        )
        .bind(other.as_uuid().hyphenated())
        .bind(pk(81))
        .execute(&pool)
        .await
        .expect("insert other user");

        assert!(get_users_bulk(&pool, community, &[])
            .await
            .expect("empty users input")
            .is_empty());
        let users = get_users_bulk(&pool, community, &[pk(81), pk(99)])
            .await
            .expect("users bulk");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].display_name.as_deref(), Some("alice"));
    }

    #[tokio::test]
    async fn ttl_reap_archives_expired_channels() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk(90);

        let expired = create_channel(
            &pool,
            community,
            "expired-huddle",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            Some(60),
        )
        .await
        .expect("expired channel");
        let alive = create_channel(
            &pool,
            community,
            "alive-huddle",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            Some(3600),
        )
        .await
        .expect("alive channel");
        let permanent = create_channel(
            &pool,
            community,
            "permanent",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            None,
        )
        .await
        .expect("permanent channel");

        // Force the first channel past its deadline.
        sqlx::query(
            "UPDATE channels SET ttl_deadline = unixepoch() - 10 WHERE community_id = ?1 AND id = ?2",
        )
        .bind(community.as_uuid().hyphenated())
        .bind(expired.id.hyphenated())
        .execute(&pool)
        .await
        .expect("expire deadline");

        // An expired channel in an archived community must be skipped.
        let archived_comm = make_community(&pool).await;
        let skipped = create_channel(
            &pool,
            archived_comm,
            "skipped",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &owner,
            Some(60),
        )
        .await
        .expect("channel in archived community");
        sqlx::query(
            "UPDATE channels SET ttl_deadline = unixepoch() - 10 WHERE community_id = ?1 AND id = ?2",
        )
        .bind(archived_comm.as_uuid().hyphenated())
        .bind(skipped.id.hyphenated())
        .execute(&pool)
        .await
        .expect("expire skipped deadline");
        sqlx::query("UPDATE communities SET archived_at = unixepoch() WHERE id = ?1")
            .bind(archived_comm.as_uuid().hyphenated())
            .execute(&pool)
            .await
            .expect("archive community");

        let reaped = reap_expired_ephemeral_channels(&pool).await.expect("reap");
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].channel_id, expired.id);
        assert_eq!(reaped[0].community_id, community);
        assert!(reaped[0].host.starts_with("channel-test-"));

        let record = get_channel(&pool, community, expired.id)
            .await
            .expect("reaped record");
        assert!(record.archived_at.is_some());
        assert!(get_channel(&pool, community, alive.id)
            .await
            .expect("alive intact")
            .archived_at
            .is_none());
        assert!(get_channel(&pool, community, permanent.id)
            .await
            .expect("permanent intact")
            .archived_at
            .is_none());
        assert!(get_channel(&pool, archived_comm, skipped.id)
            .await
            .expect("skipped intact")
            .archived_at
            .is_none());

        // Idempotent: a second sweep reaps nothing.
        assert!(reap_expired_ephemeral_channels(&pool)
            .await
            .expect("second reap")
            .is_empty());
    }
}

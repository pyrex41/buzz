//! SQLite arms for community moderation persistence (WP6, Phase 2d).
//!
//! Function-for-function port of the Solo-reachable operations in
//! [`crate::moderation`]: the NIP-56 report queue (`moderation_reports`),
//! ban/timeout state (`community_bans`), and the moderation audit trail
//! (`moderation_actions`). Function names match the `Db` facade methods for
//! name-matched dispatch. `get_moderation_report` and `get_community_ban`
//! have zero production call sites (call-site inventory §2) and are not
//! ported.
//!
//! Semantics match the Postgres arm:
//!
//! - every predicate leads with `community_id` (tenant fence — MOD
//!   invariants),
//! - report insert is idempotent on `(community_id, report_event_id)` via
//!   `ON CONFLICT … DO UPDATE … RETURNING id` (no-op update on re-ingest,
//!   returning the existing row id without reopening a resolved report),
//! - `resolve_moderation_report` is a guarded transition out of `'open'`,
//! - ban/timeout expiry is evaluated in SQL against the DB clock:
//!   `NOW()` → `unixepoch()`, timestamps stored as INTEGER unix seconds
//!   (second granularity — Postgres kept microseconds; sub-second expiry
//!   deadlines truncate to the containing second).
//!
//! No advisory locks or `FOR UPDATE` exist on this path in Postgres, so all
//! writes are single statements on the pool (no `BEGIN IMMEDIATE` needed —
//! each upsert/guarded-update is atomic on its own).

use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use super::channel::{datetime_from_unix, opt_datetime_from_unix};
use super::event::{community_text, parse_uuid_text, uuid_text};
use crate::error::{DbError, Result};
use crate::moderation::{
    ActionRecord, BanRecord, NewAction, NewReport, ReportRecord, ReportTarget, RestrictionState,
};
use crate::CommunityId;

/// Shared report column list.
const REPORT_COLUMNS: &str = "id, report_event_id, reporter_pubkey, target_kind, \
     target_event_id, target_pubkey, target_blob_sha256, channel_id, report_type, note, \
     status, resolved_by, resolved_at, action_id, created_at";

/// Shared ban column list with expiry evaluated in SQL (`banned` accounts for
/// `ban_expires_at`; `muted_until` is returned raw, matching Postgres).
const BAN_COLUMNS: &str = "pubkey, \
     (banned AND (ban_expires_at IS NULL OR ban_expires_at > unixepoch())) AS banned, \
     ban_expires_at, ban_reason, muted_until, mute_reason, actor_pubkey, updated_at";

fn get_optional_uuid(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<Option<Uuid>> {
    let raw: Option<String> = row.try_get(column)?;
    raw.as_deref().map(parse_uuid_text).transpose()
}

fn row_to_report(row: sqlx::sqlite::SqliteRow) -> Result<ReportRecord> {
    let target_kind: String = row.try_get("target_kind")?;
    let target = match target_kind.as_str() {
        "event" => ReportTarget::Event(row.try_get("target_event_id")?),
        "pubkey" => ReportTarget::Pubkey(row.try_get("target_pubkey")?),
        "blob" => ReportTarget::Blob(row.try_get("target_blob_sha256")?),
        other => {
            return Err(DbError::InvalidData(format!(
                "invalid report target_kind: {other}"
            )))
        }
    };

    let id: String = row.try_get("id")?;
    let created_at: i64 = row.try_get("created_at")?;
    Ok(ReportRecord {
        id: parse_uuid_text(&id)?,
        report_event_id: row.try_get("report_event_id")?,
        reporter_pubkey: row.try_get("reporter_pubkey")?,
        target,
        channel_id: get_optional_uuid(&row, "channel_id")?,
        report_type: row.try_get("report_type")?,
        note: row.try_get("note")?,
        status: row.try_get("status")?,
        resolved_by: row.try_get("resolved_by")?,
        resolved_at: opt_datetime_from_unix(row.try_get("resolved_at")?)?,
        action_id: get_optional_uuid(&row, "action_id")?,
        created_at: datetime_from_unix(created_at)?,
    })
}

fn row_to_ban(row: sqlx::sqlite::SqliteRow) -> Result<BanRecord> {
    let updated_at: i64 = row.try_get("updated_at")?;
    Ok(BanRecord {
        pubkey: row.try_get("pubkey")?,
        banned: row.try_get("banned")?,
        ban_expires_at: opt_datetime_from_unix(row.try_get("ban_expires_at")?)?,
        ban_reason: row.try_get("ban_reason")?,
        muted_until: opt_datetime_from_unix(row.try_get("muted_until")?)?,
        mute_reason: row.try_get("mute_reason")?,
        actor_pubkey: row.try_get("actor_pubkey")?,
        updated_at: datetime_from_unix(updated_at)?,
    })
}

fn row_to_action(row: sqlx::sqlite::SqliteRow) -> Result<ActionRecord> {
    let id: String = row.try_get("id")?;
    let created_at: i64 = row.try_get("created_at")?;
    Ok(ActionRecord {
        id: parse_uuid_text(&id)?,
        actor_pubkey: row.try_get("actor_pubkey")?,
        action: row.try_get("action")?,
        target_pubkey: row.try_get("target_pubkey")?,
        target_event_id: row.try_get("target_event_id")?,
        channel_id: get_optional_uuid(&row, "channel_id")?,
        reason_code: row.try_get("reason_code")?,
        public_reason: row.try_get("public_reason")?,
        private_reason: row.try_get("private_reason")?,
        matched_principal: row.try_get("matched_principal")?,
        created_at: datetime_from_unix(created_at)?,
    })
}

/// Insert a new report row. Idempotent on `(community, report_event_id)`:
/// re-ingesting the same signed report is a no-op returning the existing id
/// (status/resolution untouched).
pub(crate) async fn insert_moderation_report(
    pool: &SqlitePool,
    community: CommunityId,
    report: NewReport<'_>,
) -> Result<Uuid> {
    let (target_kind, target_event_id, target_pubkey, target_blob_sha256) = match &report.target {
        ReportTarget::Event(id) => ("event", Some(id.as_slice()), None, None),
        ReportTarget::Pubkey(pubkey) => ("pubkey", None, Some(pubkey.as_slice()), None),
        ReportTarget::Blob(sha256) => ("blob", None, None, Some(sha256.as_slice())),
    };

    let row = sqlx::query(
        "INSERT INTO moderation_reports ( \
             community_id, report_event_id, reporter_pubkey, target_kind, \
             target_event_id, target_pubkey, target_blob_sha256, channel_id, \
             report_type, note \
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
         ON CONFLICT (community_id, report_event_id) DO UPDATE SET \
             report_event_id = excluded.report_event_id \
         RETURNING id",
    )
    .bind(community_text(community))
    .bind(report.report_event_id)
    .bind(report.reporter_pubkey)
    .bind(target_kind)
    .bind(target_event_id)
    .bind(target_pubkey)
    .bind(target_blob_sha256)
    .bind(report.channel_id.map(uuid_text))
    .bind(report.report_type)
    .bind(report.note)
    .fetch_one(pool)
    .await?;

    let id: String = row.try_get("id")?;
    parse_uuid_text(&id)
}

/// List reports for the moderation queue, newest first.
/// `status = None` lists all; `Some("open")` etc. filters.
pub(crate) async fn list_moderation_reports(
    pool: &SqlitePool,
    community: CommunityId,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<ReportRecord>> {
    let sql = format!(
        "SELECT {REPORT_COLUMNS} FROM moderation_reports \
         WHERE community_id = ?1 AND (?2 IS NULL OR status = ?2) \
         ORDER BY created_at DESC \
         LIMIT ?3"
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community))
        .bind(status)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    rows.into_iter().map(row_to_report).collect()
}

/// Fetch one report by signed NIP-56 report event id.
pub(crate) async fn get_moderation_report_by_event(
    pool: &SqlitePool,
    community: CommunityId,
    report_event_id: &[u8],
) -> Result<Option<ReportRecord>> {
    let sql = format!(
        "SELECT {REPORT_COLUMNS} FROM moderation_reports \
         WHERE community_id = ?1 AND report_event_id = ?2"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community))
        .bind(report_event_id)
        .fetch_optional(pool)
        .await?;

    row.map(row_to_report).transpose()
}

/// Mark a report resolved/dismissed/escalated, linking the audit action.
/// Returns `false` if the report was not found or already closed.
pub(crate) async fn resolve_moderation_report(
    pool: &SqlitePool,
    community: CommunityId,
    report_id: Uuid,
    status: &str,
    resolved_by: &[u8],
    action_id: Option<Uuid>,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE moderation_reports \
         SET status = ?3, resolved_by = ?4, resolved_at = unixepoch(), action_id = ?5 \
         WHERE community_id = ?1 AND id = ?2 AND status = 'open'",
    )
    .bind(community_text(community))
    .bind(uuid_text(report_id))
    .bind(status)
    .bind(resolved_by)
    .bind(action_id.map(uuid_text))
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Upsert a ban: sets `banned = true` with optional expiry + reason.
pub(crate) async fn ban_community_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
    actor: &[u8],
    reason: Option<&str>,
    expires_at: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO community_bans ( \
             community_id, pubkey, banned, ban_expires_at, ban_reason, actor_pubkey \
         ) VALUES (?1, ?2, 1, ?3, ?4, ?5) \
         ON CONFLICT (community_id, pubkey) DO UPDATE SET \
             banned = 1, \
             ban_expires_at = excluded.ban_expires_at, \
             ban_reason = excluded.ban_reason, \
             actor_pubkey = excluded.actor_pubkey, \
             updated_at = unixepoch()",
    )
    .bind(community_text(community))
    .bind(pubkey)
    .bind(expires_at.map(|t| t.timestamp()))
    .bind(reason)
    .bind(actor)
    .execute(pool)
    .await?;

    Ok(())
}

/// Lift a ban. Returns `false` if the member was not banned.
pub(crate) async fn unban_community_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
    actor: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE community_bans \
         SET banned = 0, ban_expires_at = NULL, ban_reason = NULL, \
             actor_pubkey = ?3, updated_at = unixepoch() \
         WHERE community_id = ?1 AND pubkey = ?2 AND banned = 1",
    )
    .bind(community_text(community))
    .bind(pubkey)
    .bind(actor)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Upsert a timeout: sets `muted_until` + reason.
pub(crate) async fn timeout_community_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
    actor: &[u8],
    muted_until: DateTime<Utc>,
    reason: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO community_bans ( \
             community_id, pubkey, muted_until, mute_reason, actor_pubkey \
         ) VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT (community_id, pubkey) DO UPDATE SET \
             muted_until = excluded.muted_until, \
             mute_reason = excluded.mute_reason, \
             actor_pubkey = excluded.actor_pubkey, \
             updated_at = unixepoch()",
    )
    .bind(community_text(community))
    .bind(pubkey)
    .bind(muted_until.timestamp())
    .bind(reason)
    .bind(actor)
    .execute(pool)
    .await?;

    Ok(())
}

/// Clear a timeout early. Returns `false` if the member was not timed out
/// (no row, or the timeout already expired).
pub(crate) async fn untimeout_community_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
    actor: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE community_bans \
         SET muted_until = NULL, mute_reason = NULL, \
             actor_pubkey = ?3, updated_at = unixepoch() \
         WHERE community_id = ?1 AND pubkey = ?2 AND muted_until > unixepoch()",
    )
    .bind(community_text(community))
    .bind(pubkey)
    .bind(actor)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Fetch the current restriction state for a pubkey in one community.
/// Missing row ⇒ [`RestrictionState::default()`] (unrestricted). Expiry is
/// evaluated in SQL: `banned` accounts for `ban_expires_at`; `muted_until` is
/// returned only while still in the future.
pub(crate) async fn moderation_restriction_state(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
) -> Result<RestrictionState> {
    let row = sqlx::query(
        "SELECT \
             (banned AND (ban_expires_at IS NULL OR ban_expires_at > unixepoch())) AS banned, \
             CASE WHEN muted_until > unixepoch() THEN muted_until ELSE NULL END AS muted_until \
         FROM community_bans \
         WHERE community_id = ?1 AND pubkey = ?2",
    )
    .bind(community_text(community))
    .bind(pubkey)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => Ok(RestrictionState {
            banned: row.try_get("banned")?,
            muted_until: opt_datetime_from_unix(row.try_get("muted_until")?)?,
        }),
        None => Ok(RestrictionState::default()),
    }
}

/// List currently-restricted members (active ban or timeout) for the queue.
pub(crate) async fn list_community_restrictions(
    pool: &SqlitePool,
    community: CommunityId,
) -> Result<Vec<BanRecord>> {
    let sql = format!(
        "SELECT {BAN_COLUMNS} FROM community_bans \
         WHERE community_id = ?1 \
           AND ( \
               (banned AND (ban_expires_at IS NULL OR ban_expires_at > unixepoch())) \
               OR muted_until > unixepoch() \
           ) \
         ORDER BY updated_at DESC"
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_text(community))
        .fetch_all(pool)
        .await?;

    rows.into_iter().map(row_to_ban).collect()
}

/// Insert a moderation audit row, returning its id.
pub(crate) async fn insert_moderation_action(
    pool: &SqlitePool,
    community: CommunityId,
    action: NewAction<'_>,
) -> Result<Uuid> {
    let row = sqlx::query(
        "INSERT INTO moderation_actions ( \
             community_id, actor_pubkey, action, target_pubkey, target_event_id, \
             channel_id, reason_code, public_reason, private_reason, matched_principal \
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
         RETURNING id",
    )
    .bind(community_text(community))
    .bind(action.actor_pubkey)
    .bind(action.action)
    .bind(action.target_pubkey)
    .bind(action.target_event_id)
    .bind(action.channel_id.map(uuid_text))
    .bind(action.reason_code)
    .bind(action.public_reason)
    .bind(action.private_reason)
    .bind(action.matched_principal)
    .fetch_one(pool)
    .await?;

    let id: String = row.try_get("id")?;
    parse_uuid_text(&id)
}

/// List audit rows, newest first (`buzz moderation audit`).
pub(crate) async fn list_moderation_actions(
    pool: &SqlitePool,
    community: CommunityId,
    limit: i64,
) -> Result<Vec<ActionRecord>> {
    let rows = sqlx::query(
        "SELECT id, actor_pubkey, action, target_pubkey, target_event_id, channel_id, \
                reason_code, public_reason, private_reason, matched_principal, created_at \
         FROM moderation_actions \
         WHERE community_id = ?1 \
         ORDER BY created_at DESC \
         LIMIT ?2",
    )
    .bind(community_text(community))
    .bind(limit)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(row_to_action).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    async fn setup() -> (SqlitePool, CommunityId) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        let ensured = crate::sqlite::community::ensure_configured_community(&pool, "mod.example")
            .await
            .expect("ensure community");
        (pool, ensured.id)
    }

    fn pk(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    fn new_report<'a>(
        report_event_id: &'a [u8],
        reporter: &'a [u8],
        target_event_id: &'a [u8],
        note: Option<&'a str>,
    ) -> NewReport<'a> {
        NewReport {
            report_event_id,
            reporter_pubkey: reporter,
            target: ReportTarget::Event(target_event_id.to_vec()),
            channel_id: None,
            report_type: "spam",
            note,
        }
    }

    #[tokio::test]
    async fn report_lifecycle_insert_resolve_and_idempotent_reingest() {
        let (pool, community) = setup().await;
        let report_event_id = pk(10);
        let reporter = pk(11);
        let target_event_id = pk(12);
        let resolver = pk(13);
        let actor = pk(14);

        let first_id = insert_moderation_report(
            &pool,
            community,
            new_report(&report_event_id, &reporter, &target_event_id, Some("first")),
        )
        .await
        .expect("insert report");

        // Queue read: newest first, status filter works.
        let open = list_moderation_reports(&pool, community, Some("open"), 10)
            .await
            .expect("list open");
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, first_id);
        assert_eq!(open[0].status, "open");
        assert_eq!(open[0].target, ReportTarget::Event(target_event_id.clone()));

        // Resolve with a linked audit action.
        let action_id = insert_moderation_action(
            &pool,
            community,
            NewAction {
                actor_pubkey: &actor,
                action: "dismiss_report",
                target_pubkey: None,
                target_event_id: Some(&target_event_id),
                channel_id: None,
                reason_code: None,
                public_reason: None,
                private_reason: Some("mod-only note"),
                matched_principal: None,
            },
        )
        .await
        .expect("insert action");

        assert!(
            resolve_moderation_report(
                &pool,
                community,
                first_id,
                "resolved",
                &resolver,
                Some(action_id),
            )
            .await
            .expect("first resolve"),
            "first resolve must close the open report"
        );
        assert!(
            !resolve_moderation_report(&pool, community, first_id, "dismissed", &resolver, None)
                .await
                .expect("second resolve"),
            "second resolve must be a no-op on a closed report"
        );

        // Re-ingest of the same signed report: same id, resolution preserved.
        let second_id = insert_moderation_report(
            &pool,
            community,
            new_report(&report_event_id, &reporter, &target_event_id, Some("retry")),
        )
        .await
        .expect("re-ingest report");
        assert_eq!(first_id, second_id, "re-ingest must return the same row id");

        let row = get_moderation_report_by_event(&pool, community, &report_event_id)
            .await
            .expect("get by event")
            .expect("report exists");
        assert_eq!(row.id, first_id);
        assert_eq!(row.status, "resolved", "re-ingest must not reopen");
        assert_eq!(row.resolved_by.as_deref(), Some(resolver.as_slice()));
        assert!(row.resolved_at.is_some());
        assert_eq!(row.action_id, Some(action_id));

        // The audit trail lists the action.
        let actions = list_moderation_actions(&pool, community, 10)
            .await
            .expect("list actions");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].id, action_id);
        assert_eq!(actions[0].action, "dismiss_report");
        assert_eq!(
            actions[0].target_event_id.as_deref(),
            Some(target_event_id.as_slice())
        );
    }

    #[tokio::test]
    async fn ban_blocks_and_unban_restores() {
        let (pool, community) = setup().await;
        let member = pk(1);
        let actor = pk(2);

        // Unrestricted by default.
        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state before ban");
        assert_eq!(state, RestrictionState::default());

        ban_community_member(&pool, community, &member, &actor, Some("spam"), None)
            .await
            .expect("ban");

        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state after ban");
        assert!(state.banned, "permanent ban must be active");

        let restricted = list_community_restrictions(&pool, community)
            .await
            .expect("list restricted");
        assert_eq!(restricted.len(), 1);
        assert_eq!(restricted[0].pubkey, member);
        assert!(restricted[0].banned);
        assert_eq!(restricted[0].ban_reason.as_deref(), Some("spam"));

        assert!(
            unban_community_member(&pool, community, &member, &actor)
                .await
                .expect("unban"),
            "unban of a banned member must report success"
        );
        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state after unban");
        assert!(!state.banned, "unban must restore the member");
        assert!(list_community_restrictions(&pool, community)
            .await
            .expect("list after unban")
            .is_empty());
        assert!(
            !unban_community_member(&pool, community, &member, &actor)
                .await
                .expect("second unban"),
            "unban of a non-banned member must return false"
        );
    }

    #[tokio::test]
    async fn timeout_expiry_is_evaluated_against_the_db_clock() {
        let (pool, community) = setup().await;
        let member = pk(1);
        let actor = pk(2);

        // Active timeout restricts and can be cleared early.
        let until = Utc::now() + Duration::hours(1);
        timeout_community_member(&pool, community, &member, &actor, until, Some("cool off"))
            .await
            .expect("timeout");
        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state during timeout");
        assert!(!state.banned);
        assert_eq!(
            state.muted_until.map(|t| t.timestamp()),
            Some(until.timestamp()),
            "active timeout must surface (second granularity)"
        );
        assert!(
            untimeout_community_member(&pool, community, &member, &actor)
                .await
                .expect("untimeout"),
            "clearing an active timeout must report success"
        );
        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state after untimeout");
        assert_eq!(state, RestrictionState::default());

        // Expired timeout is inert: not surfaced, not clearable.
        let past = Utc::now() - Duration::hours(1);
        timeout_community_member(&pool, community, &member, &actor, past, None)
            .await
            .expect("expired timeout");
        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("state with expired timeout");
        assert!(
            state.muted_until.is_none(),
            "expired timeout must not restrict"
        );
        assert!(
            !untimeout_community_member(&pool, community, &member, &actor)
                .await
                .expect("untimeout expired"),
            "clearing an expired timeout must return false"
        );
        assert!(list_community_restrictions(&pool, community)
            .await
            .expect("list with expired timeout")
            .is_empty());
    }

    #[tokio::test]
    async fn expired_ban_does_not_hide_active_timeout() {
        let (pool, community) = setup().await;
        let member = pk(1);
        let actor = pk(2);

        ban_community_member(
            &pool,
            community,
            &member,
            &actor,
            Some("expired ban"),
            Some(Utc::now() - Duration::hours(1)),
        )
        .await
        .expect("insert expired ban");
        timeout_community_member(
            &pool,
            community,
            &member,
            &actor,
            Utc::now() + Duration::hours(1),
            Some("active timeout"),
        )
        .await
        .expect("insert active timeout");

        let state = moderation_restriction_state(&pool, community, &member)
            .await
            .expect("restriction state");
        assert!(!state.banned, "expired ban must evaluate inactive");
        assert!(
            state.muted_until.is_some(),
            "active timeout must survive an expired ban on the same row"
        );

        let restricted = list_community_restrictions(&pool, community)
            .await
            .expect("list restricted");
        let listed = restricted
            .iter()
            .find(|row| row.pubkey == member)
            .expect("timeout-only row remains listed");
        assert!(!listed.banned);
        assert!(listed.muted_until.is_some());
    }

    /// Restrictions are tenant-fenced: a ban in community A must not restrict
    /// the same pubkey in community B (mirrors the Postgres tenant-fence test).
    #[tokio::test]
    async fn restrictions_are_confined_to_their_community() {
        let (pool, community_a) = setup().await;
        let community_b =
            crate::sqlite::community::ensure_configured_community(&pool, "mod-b.example")
                .await
                .expect("second community")
                .id;
        let member = pk(1);
        let actor = pk(2);

        ban_community_member(&pool, community_a, &member, &actor, None, None)
            .await
            .expect("ban in A");

        let state_b = moderation_restriction_state(&pool, community_b, &member)
            .await
            .expect("state in B");
        assert_eq!(
            state_b,
            RestrictionState::default(),
            "ban in A must not leak into B"
        );
        assert!(list_community_restrictions(&pool, community_b)
            .await
            .expect("list in B")
            .is_empty());
    }
}

//! SQLite arms for workflow persistence (WP7 — workflows + approvals).
//!
//! Function-for-function ports of the Solo-reachable subset of
//! `crate::workflow` (see `docs/phase2/db-callsite-inventory.md` §3):
//! definition upsert/lookup, scheduler scan + scheduled-fire claim, run
//! lifecycle, and approval one-shot transitions. The ~12 workflow CRUD
//! methods with zero production call sites (`create_workflow`,
//! `update_workflow`, `list_workflow_runs`, …) are intentionally omitted.
//!
//! Ported semantics:
//! - `workflows`/`workflow_runs`/`workflow_approvals` are keyed
//!   `(community_id, id)` / `(community_id, token)` — every predicate binds
//!   the server-resolved community first.
//! - Upsert-by-d-tag: `ON CONFLICT (community_id, id) DO UPDATE … WHERE`
//!   owner/channel guard, with Postgres's `IS NOT DISTINCT FROM` rendered as
//!   SQLite's null-safe `IS`.
//! - JSON columns (`definition`, `execution_trace`, `trigger_context`) are
//!   TEXT, serialized/parsed at the Rust boundary.
//! - Timestamps are INTEGER unix seconds (`NOW()` → `unixepoch()`), so
//!   `scheduled_for` claims are second-granularity (cron instants are whole
//!   seconds in practice).
//! - **Scheduled-fire claim**: the Postgres arm never needed `SKIP LOCKED` —
//!   it is a single atomic `INSERT … ON CONFLICT DO NOTHING RETURNING` on the
//!   `(community_id, workflow_id, scheduled_for)` PK, which SQLite ≥ 3.35
//!   supports natively. Under SQLite's single global writer that one
//!   statement is fully serialized, so first-caller-wins exclusivity holds
//!   without any explicit `BEGIN IMMEDIATE` claim transaction.
//! - Approval one-shot: the `status = 'pending'` predicate inside the single
//!   UPDATE makes grant/deny TOCTOU-safe — the second actor's update touches
//!   zero rows and returns `false`.

// TODO(dispatch): remove once the `Db` facade (lib.rs) wires these arms —
// until then nothing outside this module calls them.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use buzz_core::CommunityId;

use crate::error::{DbError, Result};
use crate::workflow::{
    ApprovalRecord, ApprovalStatus, CreateApprovalParams, RunStatus, ScheduledWorkflowFireClaim,
    WorkflowRecord, WorkflowRunRecord, LIST_MAX_LIMIT,
};

use super::event::{community_text, datetime_from_secs, parse_uuid_text, uuid_text};

/// SHA-256 of a raw approval token (mirror of the private
/// `crate::workflow::hash_approval_token` — tokens are stored hashed so a DB
/// read never exposes the raw value).
fn hash_approval_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

// ── Row mappers ─────────────────────────────────────────────────────────────

/// Decode a standard `workflows` projection row into a [`WorkflowRecord`].
fn row_to_workflow_record(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRecord> {
    let id: String = row.try_get("id")?;
    let community_id: String = row.try_get("community_id")?;
    let channel_id: Option<String> = row.try_get("channel_id")?;
    let definition_text: String = row.try_get("definition")?;
    let status_text: String = row.try_get("status")?;
    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;

    Ok(WorkflowRecord {
        id: parse_uuid_text(&id)?,
        community_id: CommunityId::from_uuid(parse_uuid_text(&community_id)?),
        name: row.try_get("name")?,
        owner_pubkey: row.try_get("owner_pubkey")?,
        channel_id: channel_id.as_deref().map(parse_uuid_text).transpose()?,
        definition: serde_json::from_str(&definition_text)?,
        definition_hash: row.try_get("definition_hash")?,
        status: status_text.parse()?,
        enabled: row.try_get("enabled")?,
        created_at: datetime_from_secs(created_at)?,
        updated_at: datetime_from_secs(updated_at)?,
    })
}

/// Decode a standard `workflow_runs` projection row into a [`WorkflowRunRecord`].
fn row_to_run_record(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRunRecord> {
    let id: String = row.try_get("id")?;
    let community_id: String = row.try_get("community_id")?;
    let workflow_id: String = row.try_get("workflow_id")?;
    let status_text: String = row.try_get("status")?;
    let trace_text: String = row.try_get("execution_trace")?;
    let trigger_context_text: Option<String> = row.try_get("trigger_context")?;
    let started_at: Option<i64> = row.try_get("started_at")?;
    let completed_at: Option<i64> = row.try_get("completed_at")?;
    let created_at: i64 = row.try_get("created_at")?;

    Ok(WorkflowRunRecord {
        id: parse_uuid_text(&id)?,
        community_id: CommunityId::from_uuid(parse_uuid_text(&community_id)?),
        workflow_id: parse_uuid_text(&workflow_id)?,
        status: status_text.parse()?,
        trigger_event_id: row.try_get("trigger_event_id")?,
        current_step: row.try_get("current_step")?,
        execution_trace: serde_json::from_str(&trace_text)?,
        trigger_context: trigger_context_text
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        started_at: started_at.map(datetime_from_secs).transpose()?,
        completed_at: completed_at.map(datetime_from_secs).transpose()?,
        error_message: row.try_get("error_message")?,
        created_at: datetime_from_secs(created_at)?,
    })
}

/// Decode a `workflow_approvals` projection row into an [`ApprovalRecord`].
fn row_to_approval_record(row: &sqlx::sqlite::SqliteRow) -> Result<ApprovalRecord> {
    let workflow_id: String = row.try_get("workflow_id")?;
    let run_id: String = row.try_get("run_id")?;
    let status_text: String = row.try_get("status")?;
    let expires_at: i64 = row.try_get("expires_at")?;
    let created_at: i64 = row.try_get("created_at")?;

    Ok(ApprovalRecord {
        token: row.try_get("token")?,
        workflow_id: parse_uuid_text(&workflow_id)?,
        run_id: parse_uuid_text(&run_id)?,
        step_id: row.try_get("step_id")?,
        step_index: row.try_get("step_index")?,
        approver_spec: row.try_get("approver_spec")?,
        status: status_text.parse()?,
        approver_pubkey: row.try_get("approver_pubkey")?,
        note: row.try_get("note")?,
        expires_at: datetime_from_secs(expires_at)?,
        created_at: datetime_from_secs(created_at)?,
    })
}

// ── Workflow definition CRUD ────────────────────────────────────────────────

/// Insert or update a workflow at the caller-supplied NIP-33 `d`-tag UUID.
///
/// Updates are allowed only when the existing row has the same owner and
/// channel (upsert guard in the `DO UPDATE … WHERE` clause; Postgres's
/// `IS NOT DISTINCT FROM` becomes SQLite's null-safe `IS`). A guarded-out
/// update returns no row and surfaces as [`DbError::AccessDenied`], keeping a
/// learned workflow UUID from becoming a cross-user or cross-channel
/// overwrite primitive while leaving retries idempotent.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_workflow(
    pool: &SqlitePool,
    community_id: CommunityId,
    id: Uuid,
    channel_id: Option<Uuid>,
    owner_pubkey: &[u8],
    name: &str,
    definition_json: &str,
    definition_hash: &[u8],
) -> Result<()> {
    let row = sqlx::query(
        "INSERT INTO workflows \
             (community_id, id, name, owner_pubkey, channel_id, definition, definition_hash, \
              status, enabled) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', 1) \
         ON CONFLICT (community_id, id) DO UPDATE \
         SET name = excluded.name, \
             definition = excluded.definition, \
             definition_hash = excluded.definition_hash, \
             updated_at = unixepoch() \
         WHERE workflows.owner_pubkey = excluded.owner_pubkey \
           AND workflows.channel_id IS excluded.channel_id \
         RETURNING id",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .bind(name)
    .bind(owner_pubkey)
    .bind(channel_id.map(uuid_text))
    .bind(definition_json)
    .bind(definition_hash)
    .fetch_optional(pool)
    .await?;

    if row.is_none() {
        return Err(DbError::AccessDenied(format!(
            "workflow {id} belongs to a different owner or channel"
        )));
    }
    Ok(())
}

/// Fetch a single workflow by ID, scoped to its community.
pub(crate) async fn get_workflow(
    pool: &SqlitePool,
    community_id: CommunityId,
    id: Uuid,
) -> Result<WorkflowRecord> {
    let row = sqlx::query(
        "SELECT id, community_id, name, owner_pubkey, channel_id, definition, definition_hash, \
                status, enabled, created_at, updated_at \
         FROM workflows WHERE community_id = ?1 AND id = ?2",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| DbError::NotFound(format!("workflow {id}")))?;

    row_to_workflow_record(&row)
}

/// List active, enabled workflows for a channel (trigger-matching path).
///
/// Bounded to [`LIST_MAX_LIMIT`] rows. Written as `enabled = 1` so the
/// partial index `idx_workflows_enabled` stays usable.
pub(crate) async fn list_enabled_channel_workflows(
    pool: &SqlitePool,
    community_id: CommunityId,
    channel_id: Uuid,
) -> Result<Vec<WorkflowRecord>> {
    let rows = sqlx::query(
        "SELECT id, community_id, name, owner_pubkey, channel_id, definition, definition_hash, \
                status, enabled, created_at, updated_at \
         FROM workflows \
         WHERE community_id = ?1 AND channel_id = ?2 AND status = 'active' AND enabled = 1 \
         ORDER BY created_at DESC LIMIT ?3",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(channel_id))
    .bind(LIST_MAX_LIMIT)
    .fetch_all(pool)
    .await?;

    rows.iter().map(row_to_workflow_record).collect()
}

/// List all active, enabled schedule-triggered workflows across channels
/// (cron scheduler scan; excludes archived communities).
///
/// The Postgres `definition->'trigger'->>'on'` probe becomes
/// `json_extract(definition, '$.trigger.on')`. Bounded to [`LIST_MAX_LIMIT`].
pub(crate) async fn list_all_enabled_workflows(pool: &SqlitePool) -> Result<Vec<WorkflowRecord>> {
    let rows = sqlx::query(
        "SELECT w.id, w.community_id, w.name, w.owner_pubkey, w.channel_id, w.definition, \
                w.definition_hash, w.status, w.enabled, w.created_at, w.updated_at \
         FROM workflows w \
         JOIN communities c ON c.id = w.community_id \
         WHERE w.status = 'active' \
           AND w.enabled = 1 \
           AND json_extract(w.definition, '$.trigger.on') = 'schedule' \
           AND c.archived_at IS NULL \
         ORDER BY w.created_at ASC \
         LIMIT ?1",
    )
    .bind(LIST_MAX_LIMIT)
    .fetch_all(pool)
    .await?;

    rows.iter().map(row_to_workflow_record).collect()
}

/// Delete a workflow only when it belongs to `owner_pubkey` (runs/approvals
/// cascade). Returns the deleted workflow's `channel_id` for trigger-cache
/// invalidation; a missing or differently-owned workflow is
/// [`DbError::NotFound`] (owner predicate inside the DELETE — no
/// check-then-delete race).
pub(crate) async fn delete_workflow_for_owner(
    pool: &SqlitePool,
    community_id: CommunityId,
    id: Uuid,
    owner_pubkey: &[u8],
) -> Result<Option<Uuid>> {
    let row = sqlx::query(
        "DELETE FROM workflows WHERE community_id = ?1 AND id = ?2 AND owner_pubkey = ?3 \
         RETURNING channel_id",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .bind(owner_pubkey)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let channel_id: Option<String> = row.try_get("channel_id")?;
            channel_id.as_deref().map(parse_uuid_text).transpose()
        }
        None => Err(DbError::NotFound(format!("workflow {id}"))),
    }
}

/// Find a workflow by owner pubkey and name within a community (NIP-09
/// a-tag deletion path). Returns the first match, active or not.
pub(crate) async fn find_workflow_by_owner_and_name(
    pool: &SqlitePool,
    community_id: CommunityId,
    owner_pubkey: &[u8],
    name: &str,
) -> Result<Option<WorkflowRecord>> {
    let row = sqlx::query(
        "SELECT id, community_id, name, owner_pubkey, channel_id, definition, definition_hash, \
                status, enabled, created_at, updated_at \
         FROM workflows \
         WHERE community_id = ?1 AND owner_pubkey = ?2 AND name = ?3 LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(owner_pubkey)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(row_to_workflow_record).transpose()
}

// ── Scheduled fires ─────────────────────────────────────────────────────────

/// Claim a scheduled workflow fire for an authoritative schedule instant.
///
/// Returns `Some` only for the first caller to claim `(community_id,
/// workflow_id, scheduled_for)`; every later claim of the same instant gets
/// `None` and must skip creating a run. This is the same atomic
/// `INSERT … ON CONFLICT DO NOTHING RETURNING` the Postgres arm uses (no
/// `SKIP LOCKED` was ever involved) — SQLite's single writer serializes the
/// statement, so the claim row is exclusive even without an explicit
/// `BEGIN IMMEDIATE`. The inner `SELECT … FROM workflows` guard means a
/// claim on a nonexistent workflow inserts nothing and returns `None`.
/// `scheduled_for` is stored as INTEGER unix seconds (second granularity).
pub(crate) async fn claim_scheduled_workflow_fire(
    pool: &SqlitePool,
    community_id: CommunityId,
    workflow_id: Uuid,
    scheduled_for: DateTime<Utc>,
) -> Result<Option<ScheduledWorkflowFireClaim>> {
    let row = sqlx::query(
        "INSERT INTO scheduled_workflow_fires (community_id, workflow_id, scheduled_for) \
         SELECT w.community_id, w.id, ?3 FROM workflows w \
         WHERE w.community_id = ?1 AND w.id = ?2 \
         ON CONFLICT (community_id, workflow_id, scheduled_for) DO NOTHING \
         RETURNING community_id, workflow_id, scheduled_for, claimed_at",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(workflow_id))
    .bind(scheduled_for.timestamp())
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        let community_id: String = row.try_get("community_id")?;
        let workflow_id: String = row.try_get("workflow_id")?;
        let scheduled_for: i64 = row.try_get("scheduled_for")?;
        let claimed_at: i64 = row.try_get("claimed_at")?;
        Ok(ScheduledWorkflowFireClaim {
            community_id: CommunityId::from_uuid(parse_uuid_text(&community_id)?),
            workflow_id: parse_uuid_text(&workflow_id)?,
            scheduled_for: datetime_from_secs(scheduled_for)?,
            claimed_at: datetime_from_secs(claimed_at)?,
        })
    })
    .transpose()
}

/// Fetch the greatest claimed schedule instant for a workflow — the
/// DB-authoritative `last_fired` anchor for interval schedulers. Reads from
/// `scheduled_workflow_fires` (the dedupe source of truth), not
/// `workflow_runs`.
pub(crate) async fn latest_scheduled_workflow_fire(
    pool: &SqlitePool,
    community_id: CommunityId,
    workflow_id: Uuid,
) -> Result<Option<DateTime<Utc>>> {
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(scheduled_for) FROM scheduled_workflow_fires \
         WHERE community_id = ?1 AND workflow_id = ?2",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(workflow_id))
    .fetch_one(pool)
    .await?;

    latest.map(datetime_from_secs).transpose()
}

/// Link a won scheduled-fire claim to the workflow run it created
/// (ops/audit forensics only; the claim row stays the dedupe boundary).
/// Returns `true` only for the first attach — `workflow_run_id IS NULL` in
/// the predicate makes re-attaches a no-op `false`.
pub(crate) async fn attach_scheduled_workflow_run(
    pool: &SqlitePool,
    community_id: CommunityId,
    workflow_id: Uuid,
    scheduled_for: DateTime<Utc>,
    workflow_run_id: Uuid,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE scheduled_workflow_fires SET workflow_run_id = ?4 \
         WHERE community_id = ?1 AND workflow_id = ?2 AND scheduled_for = ?3 \
           AND workflow_run_id IS NULL",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(workflow_id))
    .bind(scheduled_for.timestamp())
    .bind(uuid_text(workflow_run_id))
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

// ── Workflow runs ───────────────────────────────────────────────────────────

/// Insert a new workflow run (`pending`, step 0, empty trace). Returns the
/// new run's UUID. `trigger_context` is stored as JSON TEXT so post-approval
/// resume steps can restore `{{trigger.*}}` template data.
pub(crate) async fn create_workflow_run(
    pool: &SqlitePool,
    community_id: CommunityId,
    workflow_id: Uuid,
    trigger_event_id: Option<&[u8]>,
    trigger_context: Option<&serde_json::Value>,
) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let trigger_context_text = trigger_context.map(serde_json::to_string).transpose()?;

    sqlx::query(
        "INSERT INTO workflow_runs \
             (community_id, id, workflow_id, status, trigger_event_id, current_step, \
              execution_trace, trigger_context) \
         VALUES (?1, ?2, ?3, 'pending', ?4, 0, '[]', ?5)",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .bind(uuid_text(workflow_id))
    .bind(trigger_event_id)
    .bind(trigger_context_text)
    .execute(pool)
    .await?;

    Ok(id)
}

/// Fetch a single workflow run by ID, scoped to its community.
pub(crate) async fn get_workflow_run(
    pool: &SqlitePool,
    community_id: CommunityId,
    id: Uuid,
) -> Result<WorkflowRunRecord> {
    let row = sqlx::query(
        "SELECT community_id, id, workflow_id, status, trigger_event_id, current_step, \
                execution_trace, trigger_context, started_at, completed_at, error_message, \
                created_at \
         FROM workflow_runs WHERE community_id = ?1 AND id = ?2",
    )
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| DbError::NotFound(format!("workflow_run {id}")))?;

    row_to_run_record(&row)
}

/// Update run status, current step, execution trace, and optional error.
///
/// `started_at` is stamped once, on the first transition to `running`
/// (checked against the bind parameter, preserving Postgres fix C3);
/// `completed_at` is stamped when the new status is terminal.
pub(crate) async fn update_workflow_run(
    pool: &SqlitePool,
    community_id: CommunityId,
    id: Uuid,
    status: RunStatus,
    current_step: i32,
    trace: &serde_json::Value,
    error: Option<&str>,
) -> Result<()> {
    let trace_text = serde_json::to_string(trace)?;
    let affected = sqlx::query(
        "UPDATE workflow_runs \
         SET status = ?1, \
             current_step = ?2, \
             execution_trace = ?3, \
             error_message = ?4, \
             started_at = CASE WHEN ?1 = 'running' AND started_at IS NULL \
                               THEN unixepoch() ELSE started_at END, \
             completed_at = CASE WHEN ?1 IN ('completed', 'failed', 'cancelled') \
                                 THEN unixepoch() ELSE completed_at END \
         WHERE community_id = ?5 AND id = ?6",
    )
    .bind(status.to_string())
    .bind(current_step)
    .bind(trace_text)
    .bind(error)
    .bind(community_text(community_id))
    .bind(uuid_text(id))
    .execute(pool)
    .await?
    .rows_affected();

    if affected == 0 {
        return Err(DbError::NotFound(format!("workflow_run {id}")));
    }
    Ok(())
}

// ── Approvals ───────────────────────────────────────────────────────────────

/// Insert a new `pending` approval request. The raw token in `params` is
/// SHA-256-hashed before storage — the DB never holds the plaintext.
pub(crate) async fn create_approval(
    pool: &SqlitePool,
    params: CreateApprovalParams<'_>,
) -> Result<()> {
    let CreateApprovalParams {
        community_id,
        token,
        workflow_id,
        run_id,
        step_id,
        step_index,
        approver_spec,
        expires_at,
    } = params;
    let token_hash = hash_approval_token(token);

    sqlx::query(
        "INSERT INTO workflow_approvals \
             (community_id, token, workflow_id, run_id, step_id, step_index, approver_spec, \
              status, expires_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8)",
    )
    .bind(community_text(community_id))
    .bind(token_hash)
    .bind(uuid_text(workflow_id))
    .bind(uuid_text(run_id))
    .bind(step_id)
    .bind(step_index)
    .bind(approver_spec)
    .bind(expires_at.timestamp())
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetch an approval record by its already-hashed token value, scoped to the
/// server-resolved community.
pub(crate) async fn get_approval_by_stored_hash(
    pool: &SqlitePool,
    community_id: CommunityId,
    token_hash: &[u8],
) -> Result<ApprovalRecord> {
    let row = sqlx::query(
        "SELECT token, workflow_id, run_id, step_id, step_index, approver_spec, status, \
                approver_pubkey, note, expires_at, created_at \
         FROM workflow_approvals WHERE community_id = ?1 AND token = ?2",
    )
    .bind(community_text(community_id))
    .bind(token_hash)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| DbError::NotFound("approval token (hashed)".to_string()))?;

    row_to_approval_record(&row)
}

/// Update an approval by its already-hashed token, one-shot.
///
/// TOCTOU-safe pending→granted/denied transition: `AND status = 'pending'`
/// inside the single UPDATE means concurrent grant/deny requests cannot both
/// succeed — the loser touches 0 rows and receives `Ok(false)` (callers
/// treat that as a conflict). `granted_at`/`denied_at` are stamped from the
/// bound status.
pub(crate) async fn update_approval_by_stored_hash(
    pool: &SqlitePool,
    community_id: CommunityId,
    token_hash: &[u8],
    status: ApprovalStatus,
    approver_pubkey: Option<&[u8]>,
    note: Option<&str>,
) -> Result<bool> {
    let affected = sqlx::query(
        "UPDATE workflow_approvals \
         SET status = ?1, \
             approver_pubkey = ?2, \
             note = ?3, \
             granted_at = CASE WHEN ?1 = 'granted' THEN unixepoch() ELSE granted_at END, \
             denied_at  = CASE WHEN ?1 = 'denied'  THEN unixepoch() ELSE denied_at  END \
         WHERE community_id = ?4 AND token = ?5 AND status = 'pending'",
    )
    .bind(status.to_string())
    .bind(approver_pubkey)
    .bind(note)
    .bind(community_text(community_id))
    .bind(token_hash)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(affected > 0)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
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
            .bind(format!("workflow-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    async fn make_user(pool: &SqlitePool, community: CommunityId, pubkey: &[u8]) {
        sqlx::query("INSERT INTO users (community_id, pubkey) VALUES (?1, ?2)")
            .bind(community_text(community))
            .bind(pubkey)
            .execute(pool)
            .await
            .expect("insert user");
    }

    async fn make_channel(pool: &SqlitePool, community: CommunityId, creator: &[u8]) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO channels (id, community_id, name, created_by) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(id.to_string())
        .bind(community_text(community))
        .bind(format!("wf-chan-{}", id.simple()))
        .bind(creator)
        .execute(pool)
        .await
        .expect("insert channel");
        id
    }

    fn definition(marker: &str) -> serde_json::Value {
        serde_json::json!({
            "name": marker,
            "trigger": { "on": "schedule", "cron": "* * * * *" },
            "steps": [{ "id": "s1", "action": "send_message", "text": marker }]
        })
    }

    #[tokio::test]
    async fn upsert_is_idempotent_and_owner_channel_guarded() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0x11u8; 32];
        let other = [0x22u8; 32];
        make_user(&pool, community, &owner).await;
        make_user(&pool, community, &other).await;
        let channel = make_channel(&pool, community, &owner).await;
        let wf = Uuid::new_v4();
        let def = definition("v1").to_string();

        upsert_workflow(&pool, community, wf, None, &owner, "wf", &def, &[1u8; 32])
            .await
            .expect("fresh upsert");
        // Idempotent retry (same owner, same channel) succeeds and updates.
        let def2 = definition("v2").to_string();
        upsert_workflow(
            &pool, community, wf, None, &owner, "wf-2", &def2, &[2u8; 32],
        )
        .await
        .expect("same-owner re-upsert");
        let record = get_workflow(&pool, community, wf).await.expect("get");
        assert_eq!(record.name, "wf-2");
        assert_eq!(record.definition, definition("v2"));
        assert_eq!(record.owner_pubkey, owner.to_vec());
        assert!(record.channel_id.is_none());

        // A different owner cannot overwrite the row via its learned UUID.
        let err = upsert_workflow(
            &pool, community, wf, None, &other, "steal", &def, &[3u8; 32],
        )
        .await
        .expect_err("cross-owner upsert must fail");
        assert!(matches!(err, DbError::AccessDenied(_)), "got {err:?}");

        // Same owner but a different channel is also rejected (NULL vs value
        // exercises the `IS` null-safe comparison).
        let err = upsert_workflow(
            &pool,
            community,
            wf,
            Some(channel),
            &owner,
            "move",
            &def,
            &[4u8; 32],
        )
        .await
        .expect_err("cross-channel upsert must fail");
        assert!(matches!(err, DbError::AccessDenied(_)), "got {err:?}");

        // The guarded-out attempts must not have changed the row.
        let record = get_workflow(&pool, community, wf).await.expect("get again");
        assert_eq!(record.name, "wf-2");
    }

    #[tokio::test]
    async fn find_and_owner_scoped_delete() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0x33u8; 32];
        let other = [0x44u8; 32];
        make_user(&pool, community, &owner).await;
        make_user(&pool, community, &other).await;
        let channel = make_channel(&pool, community, &owner).await;
        let wf = Uuid::new_v4();
        let def = definition("findme").to_string();

        upsert_workflow(
            &pool,
            community,
            wf,
            Some(channel),
            &owner,
            "findme",
            &def,
            &[5u8; 32],
        )
        .await
        .expect("upsert");

        let found = find_workflow_by_owner_and_name(&pool, community, &owner, "findme")
            .await
            .expect("find")
            .expect("present");
        assert_eq!(found.id, wf);
        assert_eq!(found.channel_id, Some(channel));
        assert!(
            find_workflow_by_owner_and_name(&pool, community, &other, "findme")
                .await
                .expect("find other")
                .is_none()
        );

        // A non-owner cannot delete another user's workflow via its UUID.
        let err = delete_workflow_for_owner(&pool, community, wf, &other)
            .await
            .expect_err("cross-owner delete must fail");
        assert!(matches!(err, DbError::NotFound(_)), "got {err:?}");

        let deleted_channel = delete_workflow_for_owner(&pool, community, wf, &owner)
            .await
            .expect("owner delete");
        assert_eq!(deleted_channel, Some(channel));
        assert!(matches!(
            get_workflow(&pool, community, wf).await,
            Err(DbError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn enabled_workflow_listings_are_scoped_and_filtered() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0x55u8; 32];
        make_user(&pool, community, &owner).await;
        let channel = make_channel(&pool, community, &owner).await;
        let scheduled = Uuid::new_v4();
        let evented = Uuid::new_v4();

        upsert_workflow(
            &pool,
            community,
            scheduled,
            Some(channel),
            &owner,
            "cron",
            &definition("cron").to_string(),
            &[6u8; 32],
        )
        .await
        .expect("upsert scheduled");
        let event_def = serde_json::json!({
            "trigger": { "on": "message_posted" },
            "steps": []
        });
        upsert_workflow(
            &pool,
            community,
            evented,
            Some(channel),
            &owner,
            "on-message",
            &event_def.to_string(),
            &[7u8; 32],
        )
        .await
        .expect("upsert evented");

        let per_channel = list_enabled_channel_workflows(&pool, community, channel)
            .await
            .expect("list channel");
        assert_eq!(per_channel.len(), 2);

        // Scheduler scan filters on the JSON trigger type.
        let scans = list_all_enabled_workflows(&pool).await.expect("scan");
        assert!(scans.iter().any(|w| w.id == scheduled));
        assert!(scans.iter().all(|w| w.id != evented));

        // Disabled workflows drop out of both listings.
        sqlx::query("UPDATE workflows SET enabled = 0 WHERE community_id = ?1 AND id = ?2")
            .bind(community_text(community))
            .bind(uuid_text(scheduled))
            .execute(&pool)
            .await
            .expect("disable");
        assert!(list_all_enabled_workflows(&pool)
            .await
            .expect("scan disabled")
            .iter()
            .all(|w| w.id != scheduled));
        assert_eq!(
            list_enabled_channel_workflows(&pool, community, channel)
                .await
                .expect("list channel disabled")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn run_status_transitions_latch_timestamps() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0x66u8; 32];
        make_user(&pool, community, &owner).await;
        let wf = Uuid::new_v4();
        upsert_workflow(
            &pool,
            community,
            wf,
            None,
            &owner,
            "runs",
            &definition("runs").to_string(),
            &[8u8; 32],
        )
        .await
        .expect("upsert");

        let trigger_ctx = serde_json::json!({"trigger": {"content": "hi"}});
        let run = create_workflow_run(
            &pool,
            community,
            wf,
            Some(&[0xEEu8; 32]),
            Some(&trigger_ctx),
        )
        .await
        .expect("create run");

        let record = get_workflow_run(&pool, community, run).await.expect("get");
        assert_eq!(record.status, RunStatus::Pending);
        assert_eq!(record.current_step, 0);
        assert_eq!(record.execution_trace, serde_json::json!([]));
        assert_eq!(record.trigger_context, Some(trigger_ctx));
        assert_eq!(record.trigger_event_id, Some(vec![0xEEu8; 32]));
        assert!(record.started_at.is_none());
        assert!(record.completed_at.is_none());

        // pending → running stamps started_at exactly once.
        let trace = serde_json::json!([{"step": "s1", "status": "running"}]);
        update_workflow_run(&pool, community, run, RunStatus::Running, 1, &trace, None)
            .await
            .expect("to running");
        let running = get_workflow_run(&pool, community, run).await.expect("get");
        assert_eq!(running.status, RunStatus::Running);
        let started_at = running.started_at.expect("started_at stamped");
        assert!(running.completed_at.is_none());

        // running → completed stamps completed_at and latches started_at.
        let trace = serde_json::json!([{"step": "s1", "status": "completed"}]);
        update_workflow_run(&pool, community, run, RunStatus::Completed, 1, &trace, None)
            .await
            .expect("to completed");
        let done = get_workflow_run(&pool, community, run).await.expect("get");
        assert_eq!(done.status, RunStatus::Completed);
        assert_eq!(done.started_at, Some(started_at), "started_at must latch");
        assert!(done.completed_at.is_some());
        assert_eq!(done.execution_trace, trace);

        // Unknown run id is NotFound.
        let err = update_workflow_run(
            &pool,
            community,
            Uuid::new_v4(),
            RunStatus::Failed,
            0,
            &serde_json::json!([]),
            Some("boom"),
        )
        .await
        .expect_err("unknown run");
        assert!(matches!(err, DbError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn approval_transition_is_one_shot() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0x77u8; 32];
        make_user(&pool, community, &owner).await;
        let wf = Uuid::new_v4();
        upsert_workflow(
            &pool,
            community,
            wf,
            None,
            &owner,
            "approvals",
            &definition("approvals").to_string(),
            &[9u8; 32],
        )
        .await
        .expect("upsert");
        let run = create_workflow_run(&pool, community, wf, None, None)
            .await
            .expect("create run");

        let token = "raw-approval-token";
        let expires_at = Utc
            .with_ymd_and_hms(2027, 1, 1, 0, 0, 0)
            .single()
            .expect("ts");
        create_approval(
            &pool,
            CreateApprovalParams {
                community_id: community,
                token,
                workflow_id: wf,
                run_id: run,
                step_id: "gate",
                step_index: 2,
                approver_spec: "@owner",
                expires_at,
            },
        )
        .await
        .expect("create approval");

        let token_hash = hash_approval_token(token);
        let pending = get_approval_by_stored_hash(&pool, community, &token_hash)
            .await
            .expect("get pending");
        assert_eq!(pending.status, ApprovalStatus::Pending);
        assert_eq!(pending.token, token_hash, "stored token is the hash");
        assert_eq!(pending.workflow_id, wf);
        assert_eq!(pending.run_id, run);
        assert_eq!(pending.step_index, 2);
        assert_eq!(pending.expires_at, expires_at);
        assert!(pending.approver_pubkey.is_none());

        // First transition wins…
        let approver = [0x88u8; 32];
        assert!(update_approval_by_stored_hash(
            &pool,
            community,
            &token_hash,
            ApprovalStatus::Granted,
            Some(&approver),
            Some("lgtm"),
        )
        .await
        .expect("grant"));

        // …the racing second transition loses (0 rows, false) and does not
        // clobber the decision.
        assert!(!update_approval_by_stored_hash(
            &pool,
            community,
            &token_hash,
            ApprovalStatus::Denied,
            Some(&[0x99u8; 32]),
            Some("too late"),
        )
        .await
        .expect("second update"));

        let decided = get_approval_by_stored_hash(&pool, community, &token_hash)
            .await
            .expect("get decided");
        assert_eq!(decided.status, ApprovalStatus::Granted);
        assert_eq!(decided.approver_pubkey, Some(approver.to_vec()));
        assert_eq!(decided.note.as_deref(), Some("lgtm"));

        // A different community cannot act on the token.
        let community_b = make_community(&pool).await;
        assert!(matches!(
            get_approval_by_stored_hash(&pool, community_b, &token_hash).await,
            Err(DbError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn scheduled_fire_claim_is_exclusive_and_attaches_once() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = [0xAAu8; 32];
        make_user(&pool, community, &owner).await;
        let wf = Uuid::new_v4();
        upsert_workflow(
            &pool,
            community,
            wf,
            None,
            &owner,
            "cron",
            &definition("cron").to_string(),
            &[10u8; 32],
        )
        .await
        .expect("upsert");

        let t0 = Utc
            .with_ymd_and_hms(2026, 7, 23, 12, 0, 0)
            .single()
            .expect("ts");

        // First claim of the instant wins; the re-claim gets None.
        let claim = claim_scheduled_workflow_fire(&pool, community, wf, t0)
            .await
            .expect("first claim")
            .expect("won");
        assert_eq!(claim.community_id, community);
        assert_eq!(claim.workflow_id, wf);
        assert_eq!(claim.scheduled_for, t0);
        assert!(claim_scheduled_workflow_fire(&pool, community, wf, t0)
            .await
            .expect("second claim")
            .is_none());

        // A later instant is an independent claim; MAX anchors the interval.
        let t1 = t0 + chrono::Duration::seconds(60);
        assert!(claim_scheduled_workflow_fire(&pool, community, wf, t1)
            .await
            .expect("next instant")
            .is_some());
        assert_eq!(
            latest_scheduled_workflow_fire(&pool, community, wf)
                .await
                .expect("latest"),
            Some(t1)
        );

        // A nonexistent workflow yields no claim row at all.
        assert!(
            claim_scheduled_workflow_fire(&pool, community, Uuid::new_v4(), t0)
                .await
                .expect("claim for missing workflow")
                .is_none()
        );

        // Attach links the run once; the second attach is a no-op false.
        let run = create_workflow_run(&pool, community, wf, None, None)
            .await
            .expect("run");
        assert!(attach_scheduled_workflow_run(&pool, community, wf, t0, run)
            .await
            .expect("first attach"));
        let run2 = create_workflow_run(&pool, community, wf, None, None)
            .await
            .expect("run2");
        assert!(
            !attach_scheduled_workflow_run(&pool, community, wf, t0, run2)
                .await
                .expect("second attach")
        );
        assert!(
            latest_scheduled_workflow_fire(&pool, community, Uuid::new_v4())
                .await
                .expect("latest for unknown workflow")
                .is_none()
        );
    }
}

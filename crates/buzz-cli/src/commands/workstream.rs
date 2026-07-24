//! Agent-facing commands for the Workstream kind family (Hive plan §5.1/§6).
//!
//! Reads return sig-stripped JSON arrays; writes return
//! `{event_id, accepted, message}` (creates add the entity id). Every query
//! passes an explicit `kinds` filter — an open-ended query trips the relay's
//! p-gate — and scopes by `h` tag whenever a channel is supplied.

use buzz_core::kind::{
    KIND_ARTIFACT, KIND_ARTIFACT_VERSION, KIND_DECISION_RECORD, KIND_EXPERIMENT_LOG, KIND_HANDOFF,
    KIND_MEASUREMENT, KIND_REVIEW_COMMENT, KIND_REVIEW_DECISION, KIND_REVIEW_REQUEST,
    KIND_TASK_STATUS_CHANGE, KIND_WORKSTREAM, KIND_WORKSTREAM_TASK,
};
use buzz_sdk::workstream::{
    build_artifact, build_artifact_version, build_decision_record, build_experiment_log,
    build_handoff, build_measurement, build_review_comment, build_review_decision,
    build_review_request, build_task_status_change, build_workstream, build_workstream_task,
    ArtifactParams, Coordinate, DecisionParams, DecisionStatus, TaskParams, TaskStatus,
    WorkstreamParams, WorkstreamStatus, WsType,
};
use nostr::{EventBuilder, Tag, Timestamp};
use uuid::Uuid;

use crate::client::{normalize_events, normalize_write_response, BuzzClient};
use crate::error::CliError;
use crate::validate::{read_or_stdin, sdk_err, validate_hex64, validate_uuid};
use crate::{
    ArtifactCmd, DecisionCmd, EntityKindArg, ExperimentCmd, HandoffCmd, MeasureCmd, OutputFormat,
    ReviewCmd, TaskCmd, WorkstreamCmd,
};

/// Default page size for `list` commands.
const DEFAULT_LIMIT: u32 = 50;
/// Hard ceiling on `--limit`, mirroring the other read commands.
const MAX_LIMIT: u32 = 500;
/// How many candidate heads to fetch when resolving an addressable head.
///
/// The relay keeps one head per `(kind, author, d)`, but a replica that has
/// not yet converged can briefly serve two. Fetching a small window and
/// picking the winner locally keeps `show`/`edit` deterministic.
const HEAD_CANDIDATES: u32 = 8;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Parse and validate `--channel` values into UUIDs, requiring at least one.
fn parse_channels(channels: &[String]) -> Result<Vec<Uuid>, CliError> {
    if channels.is_empty() {
        return Err(CliError::Usage(
            "--channel is required — workstream events are channel-scoped (h tag)".into(),
        ));
    }
    channels
        .iter()
        .map(|c| crate::validate::parse_uuid(c))
        .collect()
}

/// Resolve an id-or-coordinate argument into a validated [`Coordinate`].
///
/// A value containing two `:` separators is parsed as a full
/// `<kind>:<pubkey>:<d>` coordinate (and must then match `expected_kind`);
/// anything else is treated as a bare `d` tag owned by `owner`, defaulting to
/// the caller's own pubkey.
fn resolve_coordinate(
    client: &BuzzClient,
    value: &str,
    expected_kind: u32,
    owner: Option<&str>,
) -> Result<Coordinate, CliError> {
    if value.matches(':').count() >= 2 {
        let coord = Coordinate::parse(value).map_err(sdk_err)?;
        if coord.kind != expected_kind {
            return Err(CliError::Usage(format!(
                "coordinate {value:?} names kind {} but kind {expected_kind} was expected",
                coord.kind
            )));
        }
        return Ok(coord);
    }
    let owner = match owner {
        Some(hex) => {
            validate_hex64(hex)?;
            hex.to_string()
        }
        None => client.keys().public_key().to_hex(),
    };
    Coordinate::new(expected_kind, &owner, value).map_err(sdk_err)
}

/// Resolve a review/measurement target that may name any addressable kind.
fn resolve_entity(
    client: &BuzzClient,
    value: &str,
    kind: Option<EntityKindArg>,
    owner: Option<&str>,
) -> Result<Coordinate, CliError> {
    if value.matches(':').count() >= 2 {
        return Coordinate::parse(value).map_err(sdk_err);
    }
    let kind = kind.ok_or_else(|| {
        CliError::Usage(
            "--target-kind is required unless the target is a full <kind>:<pubkey>:<id> coordinate"
                .into(),
        )
    })?;
    resolve_coordinate(client, value, kind.kind(), owner)
}

/// Pick the winning head among candidates: highest `created_at`, then the
/// lexicographically lowest event id (NIP-01 replaceable-event tie-break).
fn select_head(mut events: Vec<serde_json::Value>) -> Option<serde_json::Value> {
    events.sort_by(|a, b| {
        let created =
            |e: &serde_json::Value| e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
        let id = |e: &serde_json::Value| {
            e.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        created(b).cmp(&created(a)).then_with(|| id(a).cmp(&id(b)))
    });
    events.into_iter().next()
}

/// Fetch the current head for an addressable coordinate.
async fn fetch_head(
    client: &BuzzClient,
    coord: &Coordinate,
) -> Result<Option<serde_json::Value>, CliError> {
    let filter = serde_json::json!({
        "kinds": [coord.kind],
        "authors": [coord.pubkey],
        "#d": [coord.id],
        "limit": HEAD_CANDIDATES,
    });
    let raw = client.query(&filter).await?;
    let events: Vec<serde_json::Value> = serde_json::from_str(&raw)
        .map_err(|e| CliError::Other(format!("failed to parse relay response: {e}")))?;
    Ok(select_head(events))
}

/// Fetch the head or fail with a not-found error (exit 1).
async fn require_head(
    client: &BuzzClient,
    coord: &Coordinate,
) -> Result<serde_json::Value, CliError> {
    fetch_head(client, coord).await?.ok_or_else(|| {
        CliError::NotFound(format!(
            "no head found for coordinate {}",
            coord.to_a_value()
        ))
    })
}

/// Extract the first value of a named tag from a normalized event JSON object.
fn tag_value<'a>(event: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    event
        .get("tags")?
        .as_array()?
        .iter()
        .find_map(|t| {
            let parts = t.as_array()?;
            (parts.first()?.as_str()? == name).then(|| parts.get(1)?.as_str())
        })
        .flatten()
}

/// Extract every value of a named tag.
fn tag_values(event: &serde_json::Value, name: &str) -> Vec<String> {
    event
        .get("tags")
        .and_then(|t| t.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    let parts = t.as_array()?;
                    (parts.first()?.as_str()? == name)
                        .then(|| parts.get(1)?.as_str().map(str::to_string))
                        .flatten()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Rebuild the `h` channel UUIDs recorded on an existing head.
fn channels_of(event: &serde_json::Value) -> Result<Vec<Uuid>, CliError> {
    let channels: Vec<Uuid> = tag_values(event, "h")
        .iter()
        .filter_map(|v| Uuid::parse_str(v).ok())
        .collect();
    if channels.is_empty() {
        return Err(CliError::Other(
            "existing head carries no valid h tag — refusing to publish an unscoped replacement"
                .into(),
        ));
    }
    Ok(channels)
}

/// Render a list of raw relay events for output.
fn print_events(events: &[serde_json::Value], format: &OutputFormat) {
    let normalized = normalize_events(events);
    match format {
        OutputFormat::Json => println!("{normalized}"),
        OutputFormat::Compact => {
            let parsed: Vec<serde_json::Value> =
                serde_json::from_str(&normalized).unwrap_or_default();
            let compact: Vec<serde_json::Value> = parsed
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "id": e.get("id").cloned().unwrap_or_default(),
                        "kind": e.get("kind").cloned().unwrap_or_default(),
                        "created_at": e.get("created_at").cloned().unwrap_or_default(),
                        "d": tag_value(e, "d").unwrap_or_default(),
                        "name": tag_value(e, "name").unwrap_or_default(),
                        "status": tag_value(e, "status").unwrap_or_default(),
                        "a": tag_value(e, "a").unwrap_or_default(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string(&compact).unwrap_or_default());
        }
    }
}

/// Run a query and print its (sorted, newest-first) results.
async fn query_and_print(
    client: &BuzzClient,
    filter: serde_json::Value,
    limit: u32,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let mut events = client.query_paginated(filter, limit).await?;
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

/// Post-filter results on a tag the relay cannot index for us (`status`,
/// `ws-type`, `artifact-type`, `series`, `assignee`).
fn retain_tag_equals(events: &mut Vec<serde_json::Value>, name: &str, expected: &str) {
    events.retain(|e| tag_value(e, name) == Some(expected));
}

/// Submit a signed write, surfacing a relay `duplicate` as a NIP-33 conflict
/// (exit code 5) rather than reporting a success that did not happen.
async fn submit(client: &BuzzClient, builder: EventBuilder) -> Result<String, CliError> {
    let event = client.sign_event(builder)?;
    let raw = client.submit_event(event).await?;
    let response: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| CliError::Other(format!("relay response is not JSON: {e} ({raw})")))?;
    let accepted = response
        .get("accepted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let message = response
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !accepted {
        return Err(CliError::Other(format!("relay rejected event: {message}")));
    }
    if message == "duplicate" || message.starts_with("duplicate:") {
        return Err(CliError::Conflict(
            "the head was replaced concurrently; re-read it and retry".into(),
        ));
    }
    Ok(normalize_write_response(&raw))
}

/// Print a write response, adding the created entity's id.
fn print_write(response: &str, entity_id: Option<&str>) {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(response) else {
        println!("{response}");
        return;
    };
    if let (Some(id), Some(object)) = (entity_id, value.as_object_mut()) {
        object.insert("id".into(), serde_json::json!(id));
    }
    println!("{value}");
}

/// Advance an existing head's timestamp by exactly one second.
///
/// Wall-clock time would let a delayed writer leapfrog an intervening update
/// and silently erase it; `existing + 1` is the smallest value that still wins
/// NIP-33 LWW against the head we actually read.
fn next_created_at(existing: &serde_json::Value) -> Result<Timestamp, CliError> {
    let created = existing
        .get("created_at")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CliError::Other("existing head has no created_at".into()))?;
    let next = created
        .checked_add(1)
        .ok_or_else(|| CliError::Other("head timestamp cannot be advanced".into()))?;
    Ok(Timestamp::from(next))
}

/// Rebuild an existing head's tags, dropping the ones we are replacing and
/// any relay-injected `auth` tag.
fn retained_tags(existing: &serde_json::Value, drop: &[&str]) -> Result<Vec<Tag>, CliError> {
    let tags = existing
        .get("tags")
        .and_then(|t| t.as_array())
        .ok_or_else(|| CliError::Other("existing head has no tags".into()))?;
    tags.iter()
        .filter_map(|t| {
            let parts: Vec<String> = t
                .as_array()?
                .iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect();
            let name = parts.first()?.as_str();
            if name == "auth" || drop.contains(&name) {
                return None;
            }
            Some(Tag::parse(parts).map_err(|e| CliError::Other(format!("invalid tag: {e}"))))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// workstream
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_workstream_create(
    client: &BuzzClient,
    id: &str,
    ws_type: WsType,
    name: &str,
    content: &str,
    channels: &[String],
    status: WorkstreamStatus,
    members: &[String],
) -> Result<(), CliError> {
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let member_refs: Vec<&str> = members.iter().map(String::as_str).collect();
    for member in &member_refs {
        validate_hex64(member)?;
    }
    let builder = build_workstream(&WorkstreamParams {
        id,
        ws_type,
        status,
        name,
        content: &content,
        channels: &channels,
        members: &member_refs,
    })
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(id));
    Ok(())
}

async fn cmd_workstream_list(
    client: &BuzzClient,
    channel: Option<&str>,
    owner: Option<&str>,
    ws_type: Option<WsType>,
    status: Option<WorkstreamStatus>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_WORKSTREAM] });
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if let Some(owner) = owner {
        validate_hex64(owner)?;
        filter["authors"] = serde_json::json!([owner]);
    }

    let mut events = client.query_paginated(filter, limit).await?;
    if let Some(ws_type) = ws_type {
        retain_tag_equals(&mut events, "ws-type", ws_type.as_str());
    }
    if let Some(status) = status {
        retain_tag_equals(&mut events, "status", status.as_str());
    }
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

async fn cmd_workstream_show(
    client: &BuzzClient,
    id: &str,
    owner: Option<&str>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_WORKSTREAM, owner)?;
    let head = require_head(client, &coord).await?;
    print_events(std::slice::from_ref(&head), format);
    Ok(())
}

async fn cmd_workstream_set_status(
    client: &BuzzClient,
    id: &str,
    status: WorkstreamStatus,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_WORKSTREAM, None)?;
    let head = require_head(client, &coord).await?;
    let mut tags = retained_tags(&head, &["status"])?;
    tags.push(
        Tag::parse(["status", status.as_str()])
            .map_err(|e| CliError::Other(format!("invalid status tag: {e}")))?,
    );
    let content = head
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or_default();
    let builder = EventBuilder::new(nostr::Kind::Custom(KIND_WORKSTREAM as u16), content)
        .tags(tags)
        .custom_created_at(next_created_at(&head)?);
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));
    Ok(())
}

// ---------------------------------------------------------------------------
// task
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_task_create(
    client: &BuzzClient,
    id: &str,
    workstream: &str,
    workstream_owner: Option<&str>,
    name: &str,
    content: &str,
    channels: &[String],
    status: TaskStatus,
    assignee: Option<&str>,
    due: Option<&str>,
) -> Result<(), CliError> {
    let workstream = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    if let Some(assignee) = assignee {
        validate_hex64(assignee)?;
    }
    let builder = build_workstream_task(&TaskParams {
        id,
        workstream: &workstream,
        status,
        name,
        content: &content,
        channels: &channels,
        assignee,
        due,
    })
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(id));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_task_list(
    client: &BuzzClient,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    channel: Option<&str>,
    status: Option<TaskStatus>,
    assignee: Option<&str>,
    owner: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_WORKSTREAM_TASK] });
    if let Some(workstream) = workstream {
        let coord = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if let Some(owner) = owner {
        validate_hex64(owner)?;
        filter["authors"] = serde_json::json!([owner]);
    }

    let mut events = client.query_paginated(filter, limit).await?;
    if let Some(status) = status {
        retain_tag_equals(&mut events, "status", status.as_str());
    }
    if let Some(assignee) = assignee {
        validate_hex64(assignee)?;
        retain_tag_equals(&mut events, "assignee", assignee);
    }
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

async fn cmd_task_show(
    client: &BuzzClient,
    id: &str,
    owner: Option<&str>,
    head_only: bool,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_WORKSTREAM_TASK, owner)?;
    let head = require_head(client, &coord).await?;
    let mut events = vec![head];
    if !head_only {
        let filter = serde_json::json!({
            "kinds": [KIND_TASK_STATUS_CHANGE],
            "#a": [coord.to_a_value()],
        });
        let mut history = client.query_paginated(filter, MAX_LIMIT).await?;
        history.sort_by_key(|e| e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0));
        events.extend(history);
    }
    print_events(&events, format);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_task_edit(
    client: &BuzzClient,
    id: &str,
    name: Option<&str>,
    content: Option<&str>,
    status: Option<TaskStatus>,
    assignee: Option<&str>,
    clear_assignee: bool,
    due: Option<&str>,
    clear_due: bool,
) -> Result<(), CliError> {
    if name.is_none()
        && content.is_none()
        && status.is_none()
        && assignee.is_none()
        && due.is_none()
        && !clear_assignee
        && !clear_due
    {
        return Err(CliError::Usage(
            "nothing to edit — pass at least one of --name/--content/--status/--assignee/--due/--clear-assignee/--clear-due"
                .into(),
        ));
    }

    let coord = resolve_coordinate(client, id, KIND_WORKSTREAM_TASK, None)?;
    let head = require_head(client, &coord).await?;

    let mut drop: Vec<&str> = Vec::new();
    if name.is_some() {
        drop.push("name");
    }
    if status.is_some() {
        drop.push("status");
    }
    if assignee.is_some() || clear_assignee {
        drop.push("assignee");
        drop.push("p");
    }
    if due.is_some() || clear_due {
        drop.push("due");
    }

    let mut tags = retained_tags(&head, &drop)?;
    let mut push = |parts: [&str; 2]| -> Result<(), CliError> {
        tags.push(Tag::parse(parts).map_err(|e| CliError::Other(format!("invalid tag: {e}")))?);
        Ok(())
    };
    if let Some(name) = name {
        if name.trim().is_empty() {
            return Err(CliError::Usage("--name must not be empty".into()));
        }
        push(["name", name])?;
    }
    if let Some(status) = status {
        push(["status", status.as_str()])?;
    }
    if let Some(assignee) = assignee {
        validate_hex64(assignee)?;
        let lower = assignee.to_ascii_lowercase();
        push(["assignee", &lower])?;
        push(["p", &lower])?;
    }
    if let Some(due) = due {
        // Round-trip the value through the SDK's validator by building a
        // throwaway task; a malformed date must fail before we replace a head.
        let workstream_a = tag_value(&head, "a")
            .ok_or_else(|| CliError::Other("task head has no parent workstream a tag".into()))?;
        let parent = Coordinate::parse(workstream_a).map_err(sdk_err)?;
        build_workstream_task(&TaskParams {
            id: &coord.id,
            workstream: &parent,
            status: TaskStatus::Todo,
            name: "validation",
            content: "",
            channels: &channels_of(&head)?,
            assignee: None,
            due: Some(due),
        })
        .map_err(sdk_err)?;
        push(["due", due])?;
    }

    let new_content = match content {
        Some(value) => read_or_stdin(value)?,
        None => head
            .get("content")
            .and_then(|c| c.as_str())
            .unwrap_or_default()
            .to_string(),
    };

    let builder = EventBuilder::new(
        nostr::Kind::Custom(KIND_WORKSTREAM_TASK as u16),
        new_content,
    )
    .tags(tags)
    .custom_created_at(next_created_at(&head)?);
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));
    Ok(())
}

async fn cmd_task_status(
    client: &BuzzClient,
    id: &str,
    owner: Option<&str>,
    status: TaskStatus,
    note: &str,
    no_bump: bool,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_WORKSTREAM_TASK, owner)?;
    let head = require_head(client, &coord).await?;
    let channels = channels_of(&head)?;
    let previous = tag_value(&head, "status").and_then(|s| TaskStatus::parse(s).ok());
    if previous == Some(status) {
        return Err(CliError::Usage(format!(
            "task {} is already in status {status}",
            coord.id
        )));
    }
    let note = read_or_stdin(note)?;

    let builder =
        build_task_status_change(&coord, status, previous, &note, &channels).map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));

    if no_bump {
        return Ok(());
    }
    // Bumping the head is a separate write: the 47001 history event above is
    // already durable, so a conflict here means only that someone else moved
    // the head — the transition is still recorded.
    let mut tags = retained_tags(&head, &["status"])?;
    tags.push(
        Tag::parse(["status", status.as_str()])
            .map_err(|e| CliError::Other(format!("invalid status tag: {e}")))?,
    );
    let content = head
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or_default();
    let builder = EventBuilder::new(nostr::Kind::Custom(KIND_WORKSTREAM_TASK as u16), content)
        .tags(tags)
        .custom_created_at(next_created_at(&head)?);
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));
    Ok(())
}

// ---------------------------------------------------------------------------
// artifact
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_artifact_create(
    client: &BuzzClient,
    id: &str,
    artifact_type: &str,
    name: &str,
    content: &str,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    channels: &[String],
    version: Option<&str>,
    blobs: &[String],
) -> Result<(), CliError> {
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let workstream_coord = match workstream {
        Some(value) => Some(resolve_coordinate(
            client,
            value,
            KIND_WORKSTREAM,
            workstream_owner,
        )?),
        None => None,
    };
    let blob_refs: Vec<&str> = blobs.iter().map(String::as_str).collect();
    let builder = build_artifact(&ArtifactParams {
        id,
        artifact_type,
        name,
        content: &content,
        workstream: workstream_coord.as_ref(),
        channels: &channels,
        version,
        blobs: &blob_refs,
    })
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(id));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_artifact_version(
    client: &BuzzClient,
    id: &str,
    owner: Option<&str>,
    version: &str,
    content_hash: &str,
    changelog: &str,
    blobs: &[String],
    channels: &[String],
    bump_head: bool,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_ARTIFACT, owner)?;
    let head = require_head(client, &coord).await?;
    let channels = if channels.is_empty() {
        channels_of(&head)?
    } else {
        parse_channels(channels)?
    };
    let changelog = read_or_stdin(changelog)?;
    let blob_refs: Vec<&str> = blobs.iter().map(String::as_str).collect();

    let builder = build_artifact_version(
        &coord,
        version,
        content_hash,
        &changelog,
        &blob_refs,
        &channels,
    )
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));

    if !bump_head {
        return Ok(());
    }
    let mut tags = retained_tags(&head, &["version"])?;
    tags.push(
        Tag::parse(["version", version])
            .map_err(|e| CliError::Other(format!("invalid version tag: {e}")))?,
    );
    let content = head
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or_default();
    let builder = EventBuilder::new(nostr::Kind::Custom(KIND_ARTIFACT as u16), content)
        .tags(tags)
        .custom_created_at(next_created_at(&head)?);
    let response = submit(client, builder).await?;
    print_write(&response, Some(&coord.id));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_artifact_list(
    client: &BuzzClient,
    channel: Option<&str>,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    artifact_type: Option<&str>,
    owner: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_ARTIFACT] });
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if let Some(workstream) = workstream {
        let coord = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(owner) = owner {
        validate_hex64(owner)?;
        filter["authors"] = serde_json::json!([owner]);
    }

    let mut events = client.query_paginated(filter, limit).await?;
    if let Some(artifact_type) = artifact_type {
        retain_tag_equals(&mut events, "artifact-type", artifact_type);
    }
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

async fn cmd_artifact_show(
    client: &BuzzClient,
    id: &str,
    owner: Option<&str>,
    head_only: bool,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let coord = resolve_coordinate(client, id, KIND_ARTIFACT, owner)?;
    let head = require_head(client, &coord).await?;
    let mut events = vec![head];
    if !head_only {
        let filter = serde_json::json!({
            "kinds": [KIND_ARTIFACT_VERSION],
            "#a": [coord.to_a_value()],
        });
        let mut versions = client.query_paginated(filter, MAX_LIMIT).await?;
        versions.sort_by_key(|e| e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0));
        events.extend(versions);
    }
    print_events(&events, format);
    Ok(())
}

// ---------------------------------------------------------------------------
// review
// ---------------------------------------------------------------------------

async fn cmd_review_request(
    client: &BuzzClient,
    target: &str,
    target_kind: Option<EntityKindArg>,
    target_owner: Option<&str>,
    content: &str,
    reviewers: &[String],
    channels: &[String],
) -> Result<(), CliError> {
    let coord = resolve_entity(client, target, target_kind, target_owner)?;
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let reviewer_refs: Vec<&str> = reviewers.iter().map(String::as_str).collect();
    for reviewer in &reviewer_refs {
        validate_hex64(reviewer)?;
    }
    let builder =
        build_review_request(&coord, &content, &reviewer_refs, &channels).map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, None);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_review_comment(
    client: &BuzzClient,
    request: &str,
    parent: Option<&str>,
    content: &str,
    target: Option<&str>,
    target_kind: Option<EntityKindArg>,
    target_owner: Option<&str>,
    channels: &[String],
) -> Result<(), CliError> {
    let root = crate::validate::parse_event_id(request)?;
    let parent = match parent {
        Some(value) => crate::validate::parse_event_id(value)?,
        None => root,
    };
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let coord = match target {
        Some(value) => Some(resolve_entity(client, value, target_kind, target_owner)?),
        None => None,
    };
    let builder = build_review_comment(
        &buzz_sdk::ThreadRef {
            root_event_id: root,
            parent_event_id: parent,
        },
        coord.as_ref(),
        &content,
        &channels,
    )
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, None);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_review_decide(
    client: &BuzzClient,
    request: &str,
    decision: buzz_sdk::workstream::ReviewVerdict,
    target: &str,
    target_kind: Option<EntityKindArg>,
    target_owner: Option<&str>,
    content: &str,
    channels: &[String],
) -> Result<(), CliError> {
    let coord = resolve_entity(client, target, target_kind, target_owner)?;
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let builder =
        build_review_decision(&coord, request, decision, &content, &channels).map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, None);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_review_list(
    client: &BuzzClient,
    target: Option<&str>,
    target_kind: Option<EntityKindArg>,
    target_owner: Option<&str>,
    channel: Option<&str>,
    requests_only: bool,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let kinds = if requests_only {
        vec![KIND_REVIEW_REQUEST]
    } else {
        vec![
            KIND_REVIEW_REQUEST,
            KIND_REVIEW_COMMENT,
            KIND_REVIEW_DECISION,
        ]
    };
    let mut filter = serde_json::json!({ "kinds": kinds });
    if let Some(target) = target {
        let coord = resolve_entity(client, target, target_kind, target_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if target.is_none() && channel.is_none() {
        return Err(CliError::Usage(
            "pass --target or --channel to scope the review listing".into(),
        ));
    }
    query_and_print(client, filter, limit, format).await
}

// ---------------------------------------------------------------------------
// decision
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_decision_create(
    client: &BuzzClient,
    id: &str,
    workstream: &str,
    workstream_owner: Option<&str>,
    name: &str,
    content: &str,
    channels: &[String],
    status: DecisionStatus,
) -> Result<(), CliError> {
    let workstream = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let builder = build_decision_record(&DecisionParams {
        id,
        workstream: &workstream,
        status,
        name,
        content: &content,
        channels: &channels,
        supersedes: None,
    })
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(id));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_decision_list(
    client: &BuzzClient,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    channel: Option<&str>,
    status: Option<DecisionStatus>,
    owner: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_DECISION_RECORD] });
    if let Some(workstream) = workstream {
        let coord = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if let Some(owner) = owner {
        validate_hex64(owner)?;
        filter["authors"] = serde_json::json!([owner]);
    }

    let mut events = client.query_paginated(filter, limit).await?;
    if let Some(status) = status {
        retain_tag_equals(&mut events, "status", status.as_str());
    }
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

async fn cmd_decision_supersede(
    client: &BuzzClient,
    id: &str,
    supersedes: &str,
    name: &str,
    content: &str,
) -> Result<(), CliError> {
    if id == supersedes {
        return Err(CliError::Usage(
            "--id and --supersedes must name different records".into(),
        ));
    }
    let predecessor_coord = resolve_coordinate(client, supersedes, KIND_DECISION_RECORD, None)?;
    let predecessor = require_head(client, &predecessor_coord).await?;

    // Workstream and channel scope are inherited so the successor cannot drift
    // out of the predecessor's context.
    let workstream_a = tag_value(&predecessor, "a").ok_or_else(|| {
        CliError::Other("predecessor decision has no parent workstream a tag".into())
    })?;
    let workstream = Coordinate::parse(workstream_a).map_err(sdk_err)?;
    let channels = channels_of(&predecessor)?;
    let content = read_or_stdin(content)?;

    let builder = build_decision_record(&DecisionParams {
        id,
        workstream: &workstream,
        status: DecisionStatus::Accepted,
        name,
        content: &content,
        channels: &channels,
        supersedes: Some(&predecessor_coord),
    })
    .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(id));

    // Retire the predecessor. A conflict here means someone else replaced it
    // first — the successor is already published, so surface exit 5 and let
    // the caller re-run with the newer head.
    let mut tags = retained_tags(&predecessor, &["status"])?;
    tags.push(
        Tag::parse(["status", DecisionStatus::Superseded.as_str()])
            .map_err(|e| CliError::Other(format!("invalid status tag: {e}")))?,
    );
    let predecessor_content = predecessor
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or_default();
    let builder = EventBuilder::new(
        nostr::Kind::Custom(KIND_DECISION_RECORD as u16),
        predecessor_content,
    )
    .tags(tags)
    .custom_created_at(next_created_at(&predecessor)?);
    let response = submit(client, builder).await?;
    print_write(&response, Some(&predecessor_coord.id));
    Ok(())
}

// ---------------------------------------------------------------------------
// handoff / experiment / measure
// ---------------------------------------------------------------------------

async fn cmd_handoff_create(
    client: &BuzzClient,
    workstream: &str,
    workstream_owner: Option<&str>,
    to: &str,
    content: &str,
    items: &[String],
    channels: &[String],
) -> Result<(), CliError> {
    let workstream = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
    let channels = parse_channels(channels)?;
    validate_hex64(to)?;
    let content = read_or_stdin(content)?;
    let item_refs: Vec<&str> = items.iter().map(String::as_str).collect();
    let from = client.keys().public_key().to_hex();
    let builder =
        build_handoff(&workstream, &from, to, &content, &item_refs, &channels).map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, None);
    Ok(())
}

async fn cmd_handoff_list(
    client: &BuzzClient,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    channel: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_HANDOFF] });
    if let Some(workstream) = workstream {
        let coord = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if workstream.is_none() && channel.is_none() {
        return Err(CliError::Usage(
            "pass --workstream or --channel to scope the handoff listing".into(),
        ));
    }
    query_and_print(client, filter, limit, format).await
}

async fn cmd_experiment_log(
    client: &BuzzClient,
    workstream: &str,
    workstream_owner: Option<&str>,
    experiment: &str,
    content: &str,
    labels: &[String],
    channels: &[String],
) -> Result<(), CliError> {
    let workstream = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
    let channels = parse_channels(channels)?;
    let content = read_or_stdin(content)?;
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    let builder = build_experiment_log(&workstream, experiment, &content, &label_refs, &channels)
        .map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, Some(experiment));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_experiment_list(
    client: &BuzzClient,
    workstream: Option<&str>,
    workstream_owner: Option<&str>,
    channel: Option<&str>,
    label: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_EXPERIMENT_LOG] });
    if let Some(workstream) = workstream {
        let coord = resolve_coordinate(client, workstream, KIND_WORKSTREAM, workstream_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if let Some(label) = label {
        filter["#t"] = serde_json::json!([label]);
    }
    if workstream.is_none() && channel.is_none() {
        return Err(CliError::Usage(
            "pass --workstream or --channel to scope the experiment listing".into(),
        ));
    }
    query_and_print(client, filter, limit, format).await
}

#[allow(clippy::too_many_arguments)]
async fn cmd_measure_add(
    client: &BuzzClient,
    subject: &str,
    subject_kind: EntityKindArg,
    subject_owner: Option<&str>,
    series: &str,
    value: f64,
    unit: &str,
    note: &str,
    channels: &[String],
) -> Result<(), CliError> {
    let coord = resolve_entity(client, subject, Some(subject_kind), subject_owner)?;
    let channels = parse_channels(channels)?;
    let note = read_or_stdin(note)?;
    let builder =
        build_measurement(&coord, series, value, unit, &note, &channels).map_err(sdk_err)?;
    let response = submit(client, builder).await?;
    print_write(&response, None);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_measure_list(
    client: &BuzzClient,
    subject: Option<&str>,
    subject_kind: EntityKindArg,
    subject_owner: Option<&str>,
    series: Option<&str>,
    channel: Option<&str>,
    limit: Option<u32>,
    format: &OutputFormat,
) -> Result<(), CliError> {
    let limit = clamp_limit(limit);
    let mut filter = serde_json::json!({ "kinds": [KIND_MEASUREMENT] });
    if let Some(subject) = subject {
        let coord = resolve_entity(client, subject, Some(subject_kind), subject_owner)?;
        filter["#a"] = serde_json::json!([coord.to_a_value()]);
    }
    if let Some(channel) = channel {
        validate_uuid(channel)?;
        filter["#h"] = serde_json::json!([channel]);
    }
    if subject.is_none() && channel.is_none() {
        return Err(CliError::Usage(
            "pass --subject or --channel to scope the measurement listing".into(),
        ));
    }

    let mut events = client.query_paginated(filter, limit).await?;
    if let Some(series) = series {
        retain_tag_equals(&mut events, "series", series);
    }
    events.sort_by_key(|e| {
        std::cmp::Reverse(e.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0))
    });
    print_events(&events, format);
    Ok(())
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Dispatch `buzz workstream …`.
pub async fn dispatch_workstream(
    cmd: WorkstreamCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        WorkstreamCmd::Create {
            id,
            ws_type,
            name,
            content,
            channel,
            status,
            members,
        } => {
            cmd_workstream_create(
                client,
                &id,
                ws_type.into(),
                &name,
                &content,
                &channel,
                status.into(),
                &members,
            )
            .await
        }
        WorkstreamCmd::List {
            channel,
            owner,
            ws_type,
            status,
            limit,
        } => {
            cmd_workstream_list(
                client,
                channel.as_deref(),
                owner.as_deref(),
                ws_type.map(Into::into),
                status.map(Into::into),
                limit,
                format,
            )
            .await
        }
        WorkstreamCmd::Show { id, owner } => {
            cmd_workstream_show(client, &id, owner.as_deref(), format).await
        }
        WorkstreamCmd::SetStatus { id, status } => {
            cmd_workstream_set_status(client, &id, status.into()).await
        }
    }
}

/// Dispatch `buzz task …`.
pub async fn dispatch_task(
    cmd: TaskCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        TaskCmd::Create {
            id,
            workstream,
            workstream_owner,
            name,
            content,
            channel,
            status,
            assignee,
            due,
        } => {
            cmd_task_create(
                client,
                &id,
                &workstream,
                workstream_owner.as_deref(),
                &name,
                &content,
                &channel,
                status.into(),
                assignee.as_deref(),
                due.as_deref(),
            )
            .await
        }
        TaskCmd::List {
            workstream,
            workstream_owner,
            channel,
            status,
            assignee,
            owner,
            limit,
        } => {
            cmd_task_list(
                client,
                workstream.as_deref(),
                workstream_owner.as_deref(),
                channel.as_deref(),
                status.map(Into::into),
                assignee.as_deref(),
                owner.as_deref(),
                limit,
                format,
            )
            .await
        }
        TaskCmd::Show {
            id,
            owner,
            head_only,
        } => cmd_task_show(client, &id, owner.as_deref(), head_only, format).await,
        TaskCmd::Edit {
            id,
            name,
            content,
            status,
            assignee,
            clear_assignee,
            due,
            clear_due,
        } => {
            cmd_task_edit(
                client,
                &id,
                name.as_deref(),
                content.as_deref(),
                status.map(Into::into),
                assignee.as_deref(),
                clear_assignee,
                due.as_deref(),
                clear_due,
            )
            .await
        }
        TaskCmd::Status {
            id,
            owner,
            status,
            note,
            no_bump,
        } => cmd_task_status(client, &id, owner.as_deref(), status.into(), &note, no_bump).await,
    }
}

/// Dispatch `buzz artifact …`.
pub async fn dispatch_artifact(
    cmd: ArtifactCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        ArtifactCmd::Create {
            id,
            artifact_type,
            name,
            content,
            workstream,
            workstream_owner,
            channel,
            version,
            blobs,
        } => {
            cmd_artifact_create(
                client,
                &id,
                &artifact_type,
                &name,
                &content,
                workstream.as_deref(),
                workstream_owner.as_deref(),
                &channel,
                version.as_deref(),
                &blobs,
            )
            .await
        }
        ArtifactCmd::Version {
            id,
            owner,
            version,
            content_hash,
            changelog,
            blobs,
            channel,
            bump_head,
        } => {
            cmd_artifact_version(
                client,
                &id,
                owner.as_deref(),
                &version,
                &content_hash,
                &changelog,
                &blobs,
                &channel,
                bump_head,
            )
            .await
        }
        ArtifactCmd::List {
            channel,
            workstream,
            workstream_owner,
            artifact_type,
            owner,
            limit,
        } => {
            cmd_artifact_list(
                client,
                channel.as_deref(),
                workstream.as_deref(),
                workstream_owner.as_deref(),
                artifact_type.as_deref(),
                owner.as_deref(),
                limit,
                format,
            )
            .await
        }
        ArtifactCmd::Show {
            id,
            owner,
            head_only,
        } => cmd_artifact_show(client, &id, owner.as_deref(), head_only, format).await,
    }
}

/// Dispatch `buzz review …`.
pub async fn dispatch_review(
    cmd: ReviewCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        ReviewCmd::Request {
            target,
            target_kind,
            target_owner,
            content,
            reviewers,
            channel,
        } => {
            cmd_review_request(
                client,
                &target,
                target_kind,
                target_owner.as_deref(),
                &content,
                &reviewers,
                &channel,
            )
            .await
        }
        ReviewCmd::Comment {
            request,
            parent,
            content,
            target,
            target_kind,
            target_owner,
            channel,
        } => {
            cmd_review_comment(
                client,
                &request,
                parent.as_deref(),
                &content,
                target.as_deref(),
                target_kind,
                target_owner.as_deref(),
                &channel,
            )
            .await
        }
        ReviewCmd::Decide {
            request,
            decision,
            target,
            target_kind,
            target_owner,
            content,
            channel,
        } => {
            cmd_review_decide(
                client,
                &request,
                decision.into(),
                &target,
                target_kind,
                target_owner.as_deref(),
                &content,
                &channel,
            )
            .await
        }
        ReviewCmd::List {
            target,
            target_kind,
            target_owner,
            channel,
            requests_only,
            limit,
        } => {
            cmd_review_list(
                client,
                target.as_deref(),
                target_kind,
                target_owner.as_deref(),
                channel.as_deref(),
                requests_only,
                limit,
                format,
            )
            .await
        }
    }
}

/// Dispatch `buzz decision …`.
pub async fn dispatch_decision(
    cmd: DecisionCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        DecisionCmd::Create {
            id,
            workstream,
            workstream_owner,
            name,
            content,
            channel,
            status,
        } => {
            cmd_decision_create(
                client,
                &id,
                &workstream,
                workstream_owner.as_deref(),
                &name,
                &content,
                &channel,
                status.into(),
            )
            .await
        }
        DecisionCmd::List {
            workstream,
            workstream_owner,
            channel,
            status,
            owner,
            limit,
        } => {
            cmd_decision_list(
                client,
                workstream.as_deref(),
                workstream_owner.as_deref(),
                channel.as_deref(),
                status.map(Into::into),
                owner.as_deref(),
                limit,
                format,
            )
            .await
        }
        DecisionCmd::Supersede {
            id,
            supersedes,
            name,
            content,
        } => cmd_decision_supersede(client, &id, &supersedes, &name, &content).await,
    }
}

/// Dispatch `buzz handoff …`.
pub async fn dispatch_handoff(
    cmd: HandoffCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        HandoffCmd::Create {
            workstream,
            workstream_owner,
            to,
            content,
            items,
            channel,
        } => {
            cmd_handoff_create(
                client,
                &workstream,
                workstream_owner.as_deref(),
                &to,
                &content,
                &items,
                &channel,
            )
            .await
        }
        HandoffCmd::List {
            workstream,
            workstream_owner,
            channel,
            limit,
        } => {
            cmd_handoff_list(
                client,
                workstream.as_deref(),
                workstream_owner.as_deref(),
                channel.as_deref(),
                limit,
                format,
            )
            .await
        }
    }
}

/// Dispatch `buzz experiment …`.
pub async fn dispatch_experiment(
    cmd: ExperimentCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        ExperimentCmd::Log {
            workstream,
            workstream_owner,
            experiment,
            content,
            labels,
            channel,
        } => {
            cmd_experiment_log(
                client,
                &workstream,
                workstream_owner.as_deref(),
                &experiment,
                &content,
                &labels,
                &channel,
            )
            .await
        }
        ExperimentCmd::List {
            workstream,
            workstream_owner,
            channel,
            label,
            limit,
        } => {
            cmd_experiment_list(
                client,
                workstream.as_deref(),
                workstream_owner.as_deref(),
                channel.as_deref(),
                label.as_deref(),
                limit,
                format,
            )
            .await
        }
    }
}

/// Dispatch `buzz measure …`.
pub async fn dispatch_measure(
    cmd: MeasureCmd,
    client: &BuzzClient,
    format: &OutputFormat,
) -> Result<(), CliError> {
    match cmd {
        MeasureCmd::Add {
            subject,
            subject_kind,
            subject_owner,
            series,
            value,
            unit,
            note,
            channel,
        } => {
            cmd_measure_add(
                client,
                &subject,
                subject_kind,
                subject_owner.as_deref(),
                &series,
                value,
                &unit,
                &note,
                &channel,
            )
            .await
        }
        MeasureCmd::List {
            subject,
            subject_kind,
            subject_owner,
            series,
            channel,
            limit,
        } => {
            cmd_measure_list(
                client,
                subject.as_deref(),
                subject_kind,
                subject_owner.as_deref(),
                series.as_deref(),
                channel.as_deref(),
                limit,
                format,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tags: serde_json::Value, created_at: u64, id: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "pubkey": "a".repeat(64),
            "kind": KIND_WORKSTREAM,
            "content": "body",
            "created_at": created_at,
            "tags": tags,
        })
    }

    #[test]
    fn head_selection_prefers_newest_then_lowest_id() {
        let older = event(serde_json::json!([]), 100, "ff");
        let newer_a = event(serde_json::json!([]), 200, "bb");
        let newer_b = event(serde_json::json!([]), 200, "aa");
        let head = select_head(vec![older, newer_a, newer_b]).expect("a head");
        assert_eq!(head["created_at"], 200);
        assert_eq!(head["id"], "aa", "ties break on the lowest event id");
    }

    #[test]
    fn head_selection_of_nothing_is_none() {
        assert!(select_head(Vec::new()).is_none());
    }

    #[test]
    fn tag_helpers_read_single_and_repeated_tags() {
        let e = event(
            serde_json::json!([
                ["d", "thermal-v2"],
                ["h", "11111111-1111-4111-8111-111111111111"],
                ["h", "22222222-2222-4222-8222-222222222222"],
                ["status", "active"],
            ]),
            1,
            "aa",
        );
        assert_eq!(tag_value(&e, "d"), Some("thermal-v2"));
        assert_eq!(tag_value(&e, "missing"), None);
        assert_eq!(tag_values(&e, "h").len(), 2);
        assert_eq!(channels_of(&e).expect("channels").len(), 2);
    }

    #[test]
    fn channels_of_rejects_a_head_with_no_usable_h_tag() {
        let e = event(serde_json::json!([["h", "not-a-uuid"]]), 1, "aa");
        assert!(channels_of(&e).is_err());
    }

    #[test]
    fn retained_tags_drops_auth_and_the_named_tags() {
        let e = event(
            serde_json::json!([
                ["d", "thermal-v2"],
                ["status", "active"],
                ["auth", "x", "y"],
                ["name", "Thermal"],
            ]),
            1,
            "aa",
        );
        let kept = retained_tags(&e, &["status"]).expect("tags");
        let names: Vec<String> = kept
            .iter()
            .filter_map(|t| t.as_slice().first().cloned())
            .collect();
        assert_eq!(names, vec!["d", "name"]);
    }

    #[test]
    fn next_created_at_advances_by_one_second() {
        let e = event(serde_json::json!([]), 1_700_000_000, "aa");
        assert_eq!(
            next_created_at(&e).expect("timestamp").as_secs(),
            1_700_000_001
        );
    }

    #[test]
    fn next_created_at_rejects_a_head_without_a_timestamp() {
        let e = serde_json::json!({"id": "aa", "tags": []});
        assert!(next_created_at(&e).is_err());
    }

    #[test]
    fn limits_are_clamped_to_the_read_ceiling() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(10_000)), MAX_LIMIT);
        assert_eq!(clamp_limit(Some(25)), 25);
    }

    #[test]
    fn parse_channels_requires_at_least_one_valid_uuid() {
        assert!(parse_channels(&[]).is_err());
        assert!(parse_channels(&["not-a-uuid".to_string()]).is_err());
        assert_eq!(
            parse_channels(&["11111111-1111-4111-8111-111111111111".to_string()])
                .expect("channels")
                .len(),
            1
        );
    }

    #[test]
    fn tag_post_filter_keeps_only_exact_matches() {
        let mut events = vec![
            event(serde_json::json!([["status", "active"]]), 1, "a"),
            event(serde_json::json!([["status", "done"]]), 2, "b"),
            event(serde_json::json!([]), 3, "c"),
        ];
        retain_tag_equals(&mut events, "status", "active");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"], "a");
    }

    #[test]
    fn write_output_carries_the_entity_id() {
        let response = r#"{"event_id":"abc","accepted":true,"message":"saved"}"#;
        let mut captured = serde_json::from_str::<serde_json::Value>(response).expect("json");
        captured
            .as_object_mut()
            .expect("object")
            .insert("id".into(), serde_json::json!("thermal-v2"));
        assert_eq!(captured["id"], "thermal-v2");
        assert_eq!(captured["event_id"], "abc");
    }
}

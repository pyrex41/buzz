//! Typed builders for the Workstream kind family (Hive plan §5.1).
//!
//! The Workstream model is the non-git-centric work container: a
//! [`build_workstream`] head holds tasks, artifacts, and decision records;
//! append-only events record status changes, artifact versions, reviews,
//! experiments, measurements, and handoffs.
//!
//! # Shape
//!
//! | Kind | Builder | Addressable |
//! |---|---|---|
//! | 35000 | [`build_workstream`] | yes (`d` = workstream id) |
//! | 35001 | [`build_workstream_task`] | yes (`d` = task id) |
//! | 35002 | [`build_artifact`] | yes (`d` = artifact id) |
//! | 35003 | [`build_decision_record`] | yes (`d` = decision id) |
//! | 47001 | [`build_task_status_change`] | no |
//! | 47002 | [`build_artifact_version`] | no |
//! | 47010 | [`build_review_request`] | no |
//! | 47011 | [`build_review_comment`] | no |
//! | 47012 | [`build_review_decision`] | no |
//! | 47020 | [`build_experiment_log`] | no |
//! | 47021 | [`build_measurement`] | no |
//! | 47030 | [`build_handoff`] | no |
//!
//! Every builder emits at least one `h` tag (NIP-29 channel scoping) so the
//! relay's existing tenant/channel isolation applies unchanged, and every
//! reference to an addressable head is a [`Coordinate`] rendered as
//! `<kind>:<pubkey-hex>:<d-tag>` in an `a` tag.
//!
//! All builders validate before constructing and return `Result` — they never
//! panic. The caller signs: `builder.sign_with_keys(&keys)?`.

use buzz_core::kind::{
    KIND_ARTIFACT, KIND_ARTIFACT_VERSION, KIND_DECISION_RECORD, KIND_EXPERIMENT_LOG, KIND_HANDOFF,
    KIND_MEASUREMENT, KIND_REVIEW_COMMENT, KIND_REVIEW_DECISION, KIND_REVIEW_REQUEST,
    KIND_TASK_STATUS_CHANGE, KIND_WORKSTREAM, KIND_WORKSTREAM_TASK,
};
use nostr::{EventBuilder, Kind, Tag};
use uuid::Uuid;

use crate::builders::{check_content, check_hex_exact, check_pubkey_hex, tag};
use crate::{SdkError, ThreadRef};

/// Maximum bytes for a workstream-family event body.
pub const MAX_WORKSTREAM_CONTENT_BYTES: usize = 64 * 1024;

/// Maximum bytes for a human-readable `name`/`title`/`subject` tag value.
pub const MAX_NAME_BYTES: usize = 256;

/// Maximum number of `h` (channel) tags one event may carry.
pub const MAX_CHANNELS: usize = 16;

/// Maximum number of `p` (member/reviewer) tags one event may carry.
pub const MAX_PARTICIPANTS: usize = 64;

/// Maximum number of media `x` (sha-256) references one event may carry.
pub const MAX_BLOB_REFS: usize = 32;

/// Maximum number of `checklist` items on a handoff.
pub const MAX_CHECKLIST_ITEMS: usize = 64;

// ---------------------------------------------------------------------------
// Vocabularies
// ---------------------------------------------------------------------------

/// Workstream type — the `ws-type` tag (Hive plan §5.2).
///
/// The type selects client presentation and the default agent-persona pack;
/// it does **not** change relay behavior. The vocabulary is closed here so a
/// typo can't create a silently unreachable workstream category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsType {
    /// Software work — may additionally carry `repo` tags binding NIP-34 kinds.
    Code,
    /// Systems / infrastructure / operations work.
    Systems,
    /// Physical hardware: bring-up, BOMs, mechanical, electrical.
    Hardware,
    /// Data engineering, analysis, and pipelines.
    Data,
    /// Product, visual, or interaction design.
    Design,
    /// Process, program management, and organizational work.
    Process,
    /// Documentation and technical writing.
    Docs,
    /// Anything that does not fit the more specific types.
    General,
}

impl WsType {
    /// Every valid `ws-type` value, in declaration order.
    pub const ALL: &'static [WsType] = &[
        WsType::Code,
        WsType::Systems,
        WsType::Hardware,
        WsType::Data,
        WsType::Design,
        WsType::Process,
        WsType::Docs,
        WsType::General,
    ];

    /// The wire value written to the `ws-type` tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            WsType::Code => "code",
            WsType::Systems => "systems",
            WsType::Hardware => "hardware",
            WsType::Data => "data",
            WsType::Design => "design",
            WsType::Process => "process",
            WsType::Docs => "docs",
            WsType::General => "general",
        }
    }

    /// Parse a `ws-type` wire value.
    ///
    /// ```
    /// use buzz_sdk::workstream::WsType;
    /// assert_eq!(WsType::parse("hardware")?, WsType::Hardware);
    /// assert!(WsType::parse("firmware").is_err());
    /// # Ok::<(), buzz_sdk::SdkError>(())
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == value)
            .ok_or_else(|| {
                SdkError::InvalidInput(format!(
                    "ws-type must be one of {} (got {value:?})",
                    vocabulary_list(Self::ALL.iter().map(|t| t.as_str()))
                ))
            })
    }
}

impl std::fmt::Display for WsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle status of a workstream head — the `status` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkstreamStatus {
    /// Being worked on now.
    Active,
    /// Intentionally on hold; expected to resume.
    Paused,
    /// Finished — the work landed.
    Done,
    /// Closed out without completing, or retired.
    Archived,
}

impl WorkstreamStatus {
    /// Every valid workstream status, in declaration order.
    pub const ALL: &'static [WorkstreamStatus] = &[
        WorkstreamStatus::Active,
        WorkstreamStatus::Paused,
        WorkstreamStatus::Done,
        WorkstreamStatus::Archived,
    ];

    /// The wire value written to the `status` tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkstreamStatus::Active => "active",
            WorkstreamStatus::Paused => "paused",
            WorkstreamStatus::Done => "done",
            WorkstreamStatus::Archived => "archived",
        }
    }

    /// Parse a workstream `status` wire value.
    ///
    /// ```
    /// use buzz_sdk::workstream::WorkstreamStatus;
    /// assert_eq!(WorkstreamStatus::parse("paused")?, WorkstreamStatus::Paused);
    /// # Ok::<(), buzz_sdk::SdkError>(())
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == value)
            .ok_or_else(|| {
                SdkError::InvalidInput(format!(
                    "workstream status must be one of {} (got {value:?})",
                    vocabulary_list(Self::ALL.iter().map(|s| s.as_str()))
                ))
            })
    }
}

impl std::fmt::Display for WorkstreamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle status of a task — the `status` tag on kinds 35001 and 47001.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Not started.
    Todo,
    /// Actively being worked.
    InProgress,
    /// Waiting on something external.
    Blocked,
    /// Work is complete and awaiting review.
    InReview,
    /// Finished.
    Done,
    /// Abandoned — will not be done.
    Cancelled,
}

impl TaskStatus {
    /// Every valid task status, in declaration order.
    pub const ALL: &'static [TaskStatus] = &[
        TaskStatus::Todo,
        TaskStatus::InProgress,
        TaskStatus::Blocked,
        TaskStatus::InReview,
        TaskStatus::Done,
        TaskStatus::Cancelled,
    ];

    /// The wire value written to the `status` tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Todo => "todo",
            TaskStatus::InProgress => "in-progress",
            TaskStatus::Blocked => "blocked",
            TaskStatus::InReview => "in-review",
            TaskStatus::Done => "done",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    /// Parse a task `status` wire value.
    ///
    /// ```
    /// use buzz_sdk::workstream::TaskStatus;
    /// assert_eq!(TaskStatus::parse("in-progress")?, TaskStatus::InProgress);
    /// assert!(TaskStatus::parse("wip").is_err());
    /// # Ok::<(), buzz_sdk::SdkError>(())
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == value)
            .ok_or_else(|| {
                SdkError::InvalidInput(format!(
                    "task status must be one of {} (got {value:?})",
                    vocabulary_list(Self::ALL.iter().map(|s| s.as_str()))
                ))
            })
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A review verdict — the `decision` tag on kind 47012.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewVerdict {
    /// The change is good to land.
    Approve,
    /// The change needs work before it can land.
    RequestChanges,
    /// The change should not land at all.
    Reject,
}

impl ReviewVerdict {
    /// Every valid review verdict, in declaration order.
    pub const ALL: &'static [ReviewVerdict] = &[
        ReviewVerdict::Approve,
        ReviewVerdict::RequestChanges,
        ReviewVerdict::Reject,
    ];

    /// The wire value written to the `decision` tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReviewVerdict::Approve => "approve",
            ReviewVerdict::RequestChanges => "request-changes",
            ReviewVerdict::Reject => "reject",
        }
    }

    /// Parse a review `decision` wire value.
    ///
    /// ```
    /// use buzz_sdk::workstream::ReviewVerdict;
    /// assert_eq!(ReviewVerdict::parse("request-changes")?, ReviewVerdict::RequestChanges);
    /// assert!(ReviewVerdict::parse("lgtm").is_err());
    /// # Ok::<(), buzz_sdk::SdkError>(())
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == value)
            .ok_or_else(|| {
                SdkError::InvalidInput(format!(
                    "review decision must be one of {} (got {value:?})",
                    vocabulary_list(Self::ALL.iter().map(|d| d.as_str()))
                ))
            })
    }
}

impl std::fmt::Display for ReviewVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Status of a decision record — the `status` tag on kind 35003.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionStatus {
    /// Drafted but not agreed.
    Proposed,
    /// Agreed and in force.
    Accepted,
    /// Considered and turned down.
    Rejected,
    /// No longer in force, replaced by a `supersedes` successor.
    Superseded,
}

impl DecisionStatus {
    /// Every valid decision status, in declaration order.
    pub const ALL: &'static [DecisionStatus] = &[
        DecisionStatus::Proposed,
        DecisionStatus::Accepted,
        DecisionStatus::Rejected,
        DecisionStatus::Superseded,
    ];

    /// The wire value written to the `status` tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            DecisionStatus::Proposed => "proposed",
            DecisionStatus::Accepted => "accepted",
            DecisionStatus::Rejected => "rejected",
            DecisionStatus::Superseded => "superseded",
        }
    }

    /// Parse a decision-record `status` wire value.
    ///
    /// ```
    /// use buzz_sdk::workstream::DecisionStatus;
    /// assert_eq!(DecisionStatus::parse("accepted")?, DecisionStatus::Accepted);
    /// # Ok::<(), buzz_sdk::SdkError>(())
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == value)
            .ok_or_else(|| {
                SdkError::InvalidInput(format!(
                    "decision status must be one of {} (got {value:?})",
                    vocabulary_list(Self::ALL.iter().map(|s| s.as_str()))
                ))
            })
    }
}

impl std::fmt::Display for DecisionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn vocabulary_list<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values.collect::<Vec<_>>().join(" | ")
}

// ---------------------------------------------------------------------------
// Shared validation
// ---------------------------------------------------------------------------

/// Validate an addressable identifier used as a `d` tag: `[a-zA-Z0-9._-]{1,64}`,
/// no leading dot, no `..`.
///
/// Applied to workstream, task, artifact, and decision ids so a `d` value can
/// always round-trip through an `a`-tag coordinate (which is `:`-delimited)
/// without ambiguity.
///
/// ```
/// use buzz_sdk::workstream::check_entity_id;
/// assert!(check_entity_id("thermal-v2", "workstream id").is_ok());
/// assert!(check_entity_id("bad:id", "workstream id").is_err());
/// ```
pub fn check_entity_id(id: &str, field: &str) -> Result<(), SdkError> {
    if id.is_empty() {
        return Err(SdkError::InvalidInput(format!("{field} must not be empty")));
    }
    if id.len() > 64 {
        return Err(SdkError::InvalidInput(format!(
            "{field} exceeds 64 characters (got {})",
            id.len()
        )));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(SdkError::InvalidInput(format!(
            "{field} may only contain [a-zA-Z0-9._-] (got {id:?})"
        )));
    }
    if id.starts_with('.') {
        return Err(SdkError::InvalidInput(format!(
            "{field} must not start with a dot"
        )));
    }
    if id.contains("..") {
        return Err(SdkError::InvalidInput(format!(
            "{field} must not contain '..'"
        )));
    }
    Ok(())
}

/// Validate a lowercase 64-character SHA-256 hex digest, returning it lowercased.
///
/// Used for media `x` tags and artifact-version content hashes: a truncated or
/// non-hex digest would produce a blob reference that can never resolve.
///
/// ```
/// use buzz_sdk::workstream::check_sha256;
/// let digest = "a".repeat(64);
/// assert_eq!(check_sha256(&digest, "x")?, digest);
/// assert!(check_sha256("abc", "x").is_err());
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn check_sha256(value: &str, field: &str) -> Result<String, SdkError> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(SdkError::InvalidInput(format!(
            "{field} must be a 64-character SHA-256 hex digest (got {value:?})"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

fn check_name(name: &str, field: &str) -> Result<(), SdkError> {
    if name.trim().is_empty() {
        return Err(SdkError::InvalidInput(format!("{field} must not be empty")));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(SdkError::InvalidInput(format!(
            "{field} exceeds {MAX_NAME_BYTES} bytes (got {})",
            name.len()
        )));
    }
    if name.contains(['\n', '\r']) {
        return Err(SdkError::InvalidInput(format!(
            "{field} must be a single line"
        )));
    }
    Ok(())
}

/// Validate a free-form label written to a tag (artifact type, measurement
/// unit, series name): 1–64 bytes, no whitespace or control characters.
fn check_label(value: &str, field: &str) -> Result<(), SdkError> {
    if value.is_empty() {
        return Err(SdkError::InvalidInput(format!("{field} must not be empty")));
    }
    if value.len() > 64 {
        return Err(SdkError::InvalidInput(format!(
            "{field} exceeds 64 bytes (got {})",
            value.len()
        )));
    }
    if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(SdkError::InvalidInput(format!(
            "{field} must not contain whitespace or control characters (got {value:?})"
        )));
    }
    Ok(())
}

fn channel_tags(channels: &[Uuid], tags: &mut Vec<Tag>) -> Result<(), SdkError> {
    if channels.is_empty() {
        return Err(SdkError::InvalidInput(
            "at least one channel (h tag) is required — workstream events are channel-scoped"
                .into(),
        ));
    }
    if channels.len() > MAX_CHANNELS {
        return Err(SdkError::InvalidInput(format!(
            "too many channels (max {MAX_CHANNELS}, got {})",
            channels.len()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for channel in channels {
        if seen.insert(*channel) {
            tags.push(tag(&["h", &channel.to_string()])?);
        }
    }
    Ok(())
}

fn participant_tags(pubkeys: &[&str], field: &str, tags: &mut Vec<Tag>) -> Result<(), SdkError> {
    if pubkeys.len() > MAX_PARTICIPANTS {
        return Err(SdkError::InvalidInput(format!(
            "too many {field} (max {MAX_PARTICIPANTS}, got {})",
            pubkeys.len()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for pubkey in pubkeys {
        let hex = check_pubkey_hex(pubkey, field)?;
        if seen.insert(hex.clone()) {
            tags.push(tag(&["p", &hex])?);
        }
    }
    Ok(())
}

fn blob_tags(blobs: &[&str], tags: &mut Vec<Tag>) -> Result<(), SdkError> {
    if blobs.len() > MAX_BLOB_REFS {
        return Err(SdkError::InvalidInput(format!(
            "too many blob references (max {MAX_BLOB_REFS}, got {})",
            blobs.len()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for blob in blobs {
        let digest = check_sha256(blob, "blob reference (x tag)")?;
        if seen.insert(digest.clone()) {
            tags.push(tag(&["x", &digest])?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Coordinates
// ---------------------------------------------------------------------------

/// An addressable NIP-33 coordinate: `<kind>:<pubkey-hex>:<d-tag>`.
///
/// This is the value carried in an `a` tag when a workstream event references
/// an addressable head (workstream, task, artifact, decision record).
///
/// ```
/// use buzz_sdk::workstream::Coordinate;
///
/// let owner = "b".repeat(64);
/// let coord = Coordinate::new(35000, &owner, "thermal-v2")?;
/// assert_eq!(coord.to_a_value(), format!("35000:{owner}:thermal-v2"));
/// assert_eq!(Coordinate::parse(&coord.to_a_value())?, coord);
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coordinate {
    /// The addressable event kind (35000–35003 for the workstream family).
    pub kind: u32,
    /// 64-character lowercase hex pubkey of the head's author.
    pub pubkey: String,
    /// The head's `d`-tag identifier.
    pub id: String,
}

impl Coordinate {
    /// Build and validate a coordinate from its parts.
    ///
    /// Rejects a non-hex pubkey and any `d` value that would not survive the
    /// `:`-delimited encoding (see [`check_entity_id`]).
    pub fn new(kind: u32, pubkey: &str, id: &str) -> Result<Self, SdkError> {
        let pubkey = check_pubkey_hex(pubkey, "coordinate pubkey")?;
        check_entity_id(id, "coordinate id")?;
        Ok(Self {
            kind,
            pubkey,
            id: id.to_string(),
        })
    }

    /// Parse an `a`-tag value of the form `<kind>:<pubkey-hex>:<d-tag>`.
    ///
    /// ```
    /// use buzz_sdk::workstream::Coordinate;
    /// assert!(Coordinate::parse("35001:not-hex:task-1").is_err());
    /// assert!(Coordinate::parse("35001").is_err());
    /// ```
    pub fn parse(value: &str) -> Result<Self, SdkError> {
        let mut parts = value.splitn(3, ':');
        let (Some(kind), Some(pubkey), Some(id)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(SdkError::InvalidInput(format!(
                "coordinate must be <kind>:<pubkey-hex>:<d-tag> (got {value:?})"
            )));
        };
        let kind: u32 = kind.parse().map_err(|_| {
            SdkError::InvalidInput(format!(
                "coordinate kind must be an unsigned integer (got {kind:?})"
            ))
        })?;
        Self::new(kind, pubkey, id)
    }

    /// Parse a coordinate and require it to name one of `expected` kinds.
    ///
    /// Review requests, comments, and decisions accept several target kinds;
    /// task status changes accept exactly one. This keeps an `a` tag from
    /// pointing at, say, a channel metadata event.
    pub fn parse_of_kinds(value: &str, expected: &[u32]) -> Result<Self, SdkError> {
        let coord = Self::parse(value)?;
        if !expected.contains(&coord.kind) {
            return Err(SdkError::InvalidInput(format!(
                "coordinate kind {} is not one of {} (got {value:?})",
                coord.kind,
                vocabulary_list(
                    expected
                        .iter()
                        .map(|k| k.to_string())
                        .collect::<Vec<_>>()
                        .iter()
                        .map(String::as_str)
                )
            )));
        }
        Ok(coord)
    }

    /// Render the coordinate as an `a`-tag value.
    pub fn to_a_value(&self) -> String {
        format!("{}:{}:{}", self.kind, self.pubkey, self.id)
    }
}

impl std::fmt::Display for Coordinate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_a_value())
    }
}

/// Every addressable kind a review may target.
pub const REVIEWABLE_KINDS: &[u32] = &[
    KIND_WORKSTREAM_TASK,
    KIND_ARTIFACT,
    KIND_DECISION_RECORD,
    KIND_WORKSTREAM,
];

// ---------------------------------------------------------------------------
// 35000 — workstream
// ---------------------------------------------------------------------------

/// Parameters for [`build_workstream`].
pub struct WorkstreamParams<'a> {
    /// Workstream identifier — the `d` tag. `[a-zA-Z0-9._-]{1,64}`.
    pub id: &'a str,
    /// Workstream type — the `ws-type` tag.
    pub ws_type: WsType,
    /// Lifecycle status — the `status` tag.
    pub status: WorkstreamStatus,
    /// Human-readable display name — the `name` tag. Single line, ≤256 bytes.
    pub name: &'a str,
    /// Markdown description body. May be empty.
    pub content: &'a str,
    /// Channels this workstream lives in — at least one `h` tag.
    pub channels: &'a [Uuid],
    /// Members and agents — `p` tags. 64-char hex pubkeys.
    pub members: &'a [&'a str],
}

/// Build a workstream head (kind 35000, addressable).
///
/// Republishing with the same `id` replaces the head under NIP-33 LWW.
///
/// ```
/// use buzz_sdk::workstream::{build_workstream, WorkstreamParams, WorkstreamStatus, WsType};
/// use uuid::Uuid;
///
/// let channel = Uuid::nil();
/// let builder = build_workstream(&WorkstreamParams {
///     id: "thermal-v2",
///     ws_type: WsType::Hardware,
///     status: WorkstreamStatus::Active,
///     name: "Thermal chamber v2",
///     content: "Bring-up of the second thermal chamber.",
///     channels: &[channel],
///     members: &[],
/// })?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_workstream(params: &WorkstreamParams<'_>) -> Result<EventBuilder, SdkError> {
    check_entity_id(params.id, "workstream id")?;
    check_name(params.name, "workstream name")?;
    check_content(params.content, MAX_WORKSTREAM_CONTENT_BYTES)?;

    let mut tags = vec![
        tag(&["d", params.id])?,
        tag(&["ws-type", params.ws_type.as_str()])?,
        tag(&["status", params.status.as_str()])?,
        tag(&["name", params.name])?,
    ];
    channel_tags(params.channels, &mut tags)?;
    participant_tags(params.members, "workstream member", &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_WORKSTREAM as u16), params.content).tags(tags))
}

// ---------------------------------------------------------------------------
// 35001 — task
// ---------------------------------------------------------------------------

/// Parameters for [`build_workstream_task`].
pub struct TaskParams<'a> {
    /// Task identifier — the `d` tag. `[a-zA-Z0-9._-]{1,64}`.
    pub id: &'a str,
    /// Parent workstream coordinate — the `a` tag. Must name kind 35000.
    pub workstream: &'a Coordinate,
    /// Lifecycle status — the `status` tag.
    pub status: TaskStatus,
    /// Short task title — the `name` tag.
    pub name: &'a str,
    /// Markdown body. May be empty.
    pub content: &'a str,
    /// Channels this task is visible in — at least one `h` tag.
    pub channels: &'a [Uuid],
    /// Optional assignee pubkey — emits both an `assignee` tag and a `p` tag.
    pub assignee: Option<&'a str>,
    /// Optional due date as an ISO-8601 date (`YYYY-MM-DD`) — the `due` tag.
    pub due: Option<&'a str>,
}

/// Build a task head (kind 35001, addressable).
///
/// The head is last-write-wins; the append-only history lives in
/// [`build_task_status_change`] events.
///
/// ```
/// use buzz_sdk::workstream::{build_workstream_task, Coordinate, TaskParams, TaskStatus};
/// use uuid::Uuid;
///
/// let workstream = Coordinate::new(35000, &"a".repeat(64), "thermal-v2")?;
/// let builder = build_workstream_task(&TaskParams {
///     id: "calibrate-probe",
///     workstream: &workstream,
///     status: TaskStatus::Todo,
///     name: "Calibrate the thermocouple probe",
///     content: "",
///     channels: &[Uuid::nil()],
///     assignee: None,
///     due: Some("2026-08-01"),
/// })?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_workstream_task(params: &TaskParams<'_>) -> Result<EventBuilder, SdkError> {
    check_entity_id(params.id, "task id")?;
    check_name(params.name, "task name")?;
    check_content(params.content, MAX_WORKSTREAM_CONTENT_BYTES)?;
    require_kind(params.workstream, KIND_WORKSTREAM, "parent workstream")?;

    let mut tags = vec![
        tag(&["d", params.id])?,
        tag(&["a", &params.workstream.to_a_value()])?,
        tag(&["status", params.status.as_str()])?,
        tag(&["name", params.name])?,
    ];
    if let Some(assignee) = params.assignee {
        let hex = check_pubkey_hex(assignee, "task assignee")?;
        tags.push(tag(&["assignee", &hex])?);
        tags.push(tag(&["p", &hex])?);
    }
    if let Some(due) = params.due {
        check_iso_date(due)?;
        tags.push(tag(&["due", due])?);
    }
    channel_tags(params.channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_WORKSTREAM_TASK as u16), params.content).tags(tags))
}

/// Validate an ISO-8601 calendar date (`YYYY-MM-DD`) used in a `due` tag.
fn check_iso_date(value: &str) -> Result<(), SdkError> {
    let bytes = value.as_bytes();
    let shaped = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit());
    if !shaped {
        return Err(SdkError::InvalidInput(format!(
            "due must be an ISO-8601 date (YYYY-MM-DD), got {value:?}"
        )));
    }
    let month: u32 = value[5..7].parse().unwrap_or(0);
    let day: u32 = value[8..10].parse().unwrap_or(0);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(SdkError::InvalidInput(format!(
            "due is not a valid calendar date: {value:?}"
        )));
    }
    Ok(())
}

fn require_kind(coord: &Coordinate, expected: u32, field: &str) -> Result<(), SdkError> {
    if coord.kind != expected {
        return Err(SdkError::InvalidInput(format!(
            "{field} coordinate must name kind {expected} (got {})",
            coord.kind
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 35002 — artifact
// ---------------------------------------------------------------------------

/// Parameters for [`build_artifact`].
pub struct ArtifactParams<'a> {
    /// Artifact identifier — the `d` tag.
    pub id: &'a str,
    /// Artifact type — the `artifact-type` tag. An open vocabulary of
    /// whitespace-free labels; conventional values are `doc`, `design`,
    /// `dataset`, `measurement`, `bom`, `sim-result`, `spec`.
    pub artifact_type: &'a str,
    /// Human-readable display name — the `name` tag.
    pub name: &'a str,
    /// Markdown description or inline body. May be empty.
    pub content: &'a str,
    /// Owning workstream coordinate — optional `a` tag naming kind 35000.
    pub workstream: Option<&'a Coordinate>,
    /// Channels this artifact is visible in — at least one `h` tag.
    pub channels: &'a [Uuid],
    /// Current version label — the `version` tag (free-form, e.g. `v3`).
    pub version: Option<&'a str>,
    /// Media blob references — `x` tags, each a 64-char SHA-256 hex digest.
    pub blobs: &'a [&'a str],
}

/// Build an artifact head (kind 35002, addressable).
///
/// The head carries the current-version pointer; immutable version history
/// lives in [`build_artifact_version`] events.
///
/// ```
/// use buzz_sdk::workstream::{build_artifact, ArtifactParams};
/// use uuid::Uuid;
///
/// let builder = build_artifact(&ArtifactParams {
///     id: "chamber-bom",
///     artifact_type: "bom",
///     name: "Thermal chamber bill of materials",
///     content: "",
///     workstream: None,
///     channels: &[Uuid::nil()],
///     version: Some("v1"),
///     blobs: &[],
/// })?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_artifact(params: &ArtifactParams<'_>) -> Result<EventBuilder, SdkError> {
    check_entity_id(params.id, "artifact id")?;
    check_label(params.artifact_type, "artifact-type")?;
    check_name(params.name, "artifact name")?;
    check_content(params.content, MAX_WORKSTREAM_CONTENT_BYTES)?;

    let mut tags = vec![
        tag(&["d", params.id])?,
        tag(&["artifact-type", params.artifact_type])?,
        tag(&["name", params.name])?,
    ];
    if let Some(workstream) = params.workstream {
        require_kind(workstream, KIND_WORKSTREAM, "owning workstream")?;
        tags.push(tag(&["a", &workstream.to_a_value()])?);
    }
    if let Some(version) = params.version {
        check_label(version, "artifact version")?;
        tags.push(tag(&["version", version])?);
    }
    blob_tags(params.blobs, &mut tags)?;
    channel_tags(params.channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_ARTIFACT as u16), params.content).tags(tags))
}

// ---------------------------------------------------------------------------
// 35003 — decision record
// ---------------------------------------------------------------------------

/// Parameters for [`build_decision_record`].
pub struct DecisionParams<'a> {
    /// Decision identifier — the `d` tag.
    pub id: &'a str,
    /// Owning workstream coordinate — the `a` tag. Must name kind 35000.
    pub workstream: &'a Coordinate,
    /// Decision status — the `status` tag.
    pub status: DecisionStatus,
    /// Decision title — the `name` tag.
    pub name: &'a str,
    /// Markdown body: context, decision, consequences.
    pub content: &'a str,
    /// Channels this record is visible in — at least one `h` tag.
    pub channels: &'a [Uuid],
    /// Coordinate of a decision this one replaces — the `supersedes` tag.
    /// Must name kind 35003.
    pub supersedes: Option<&'a Coordinate>,
}

/// Build a decision record (kind 35003, addressable).
///
/// Supersession is a chain: the replacement carries `supersedes` pointing at
/// its predecessor, and the predecessor is republished with
/// [`DecisionStatus::Superseded`].
///
/// ```
/// use buzz_sdk::workstream::{build_decision_record, Coordinate, DecisionParams, DecisionStatus};
/// use uuid::Uuid;
///
/// let workstream = Coordinate::new(35000, &"c".repeat(64), "thermal-v2")?;
/// let builder = build_decision_record(&DecisionParams {
///     id: "adr-0002",
///     workstream: &workstream,
///     status: DecisionStatus::Accepted,
///     name: "Use PT100 probes",
///     content: "## Context\nThermocouples drift.\n\n## Decision\nUse PT100.",
///     channels: &[Uuid::nil()],
///     supersedes: None,
/// })?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_decision_record(params: &DecisionParams<'_>) -> Result<EventBuilder, SdkError> {
    check_entity_id(params.id, "decision id")?;
    check_name(params.name, "decision name")?;
    check_content(params.content, MAX_WORKSTREAM_CONTENT_BYTES)?;
    require_kind(params.workstream, KIND_WORKSTREAM, "owning workstream")?;

    let mut tags = vec![
        tag(&["d", params.id])?,
        tag(&["a", &params.workstream.to_a_value()])?,
        tag(&["status", params.status.as_str()])?,
        tag(&["name", params.name])?,
    ];
    if let Some(superseded) = params.supersedes {
        require_kind(superseded, KIND_DECISION_RECORD, "superseded decision")?;
        if superseded.id == params.id && superseded.kind == KIND_DECISION_RECORD {
            return Err(SdkError::InvalidInput(
                "a decision record cannot supersede itself".into(),
            ));
        }
        tags.push(tag(&["supersedes", &superseded.to_a_value()])?);
    }
    channel_tags(params.channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_DECISION_RECORD as u16), params.content).tags(tags))
}

// ---------------------------------------------------------------------------
// 47001 — task status change
// ---------------------------------------------------------------------------

/// Build a task status change (kind 47001, append-only).
///
/// The task head (kind 35001) is last-write-wins, so status *history* lives in
/// these events. `note` becomes the event content.
///
/// ```
/// use buzz_sdk::workstream::{build_task_status_change, Coordinate, TaskStatus};
/// use uuid::Uuid;
///
/// let task = Coordinate::new(35001, &"d".repeat(64), "calibrate-probe")?;
/// let builder = build_task_status_change(
///     &task,
///     TaskStatus::InProgress,
///     Some(TaskStatus::Todo),
///     "starting on the bench",
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_task_status_change(
    task: &Coordinate,
    status: TaskStatus,
    previous_status: Option<TaskStatus>,
    note: &str,
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_kind(task, KIND_WORKSTREAM_TASK, "task")?;
    check_content(note, MAX_WORKSTREAM_CONTENT_BYTES)?;
    if previous_status == Some(status) {
        return Err(SdkError::InvalidInput(format!(
            "status change is a no-op: previous and new status are both {status}"
        )));
    }

    let mut tags = vec![
        tag(&["a", &task.to_a_value()])?,
        tag(&["status", status.as_str()])?,
    ];
    if let Some(previous) = previous_status {
        tags.push(tag(&["previous-status", previous.as_str()])?);
    }
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_TASK_STATUS_CHANGE as u16), note).tags(tags))
}

// ---------------------------------------------------------------------------
// 47002 — artifact version
// ---------------------------------------------------------------------------

/// Build an immutable artifact version (kind 47002, append-only).
///
/// `content_hash` is the SHA-256 of the versioned payload; `blobs` are the
/// media blobs that make up this version. `changelog` becomes the content.
///
/// ```
/// use buzz_sdk::workstream::{build_artifact_version, Coordinate};
/// use uuid::Uuid;
///
/// let artifact = Coordinate::new(35002, &"e".repeat(64), "chamber-bom")?;
/// let digest = "f".repeat(64);
/// let builder = build_artifact_version(
///     &artifact,
///     "v2",
///     &digest,
///     "swapped in PT100 probes",
///     &[digest.as_str()],
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_artifact_version(
    artifact: &Coordinate,
    version: &str,
    content_hash: &str,
    changelog: &str,
    blobs: &[&str],
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_kind(artifact, KIND_ARTIFACT, "artifact")?;
    check_label(version, "artifact version")?;
    let digest = check_sha256(content_hash, "content hash")?;
    check_content(changelog, MAX_WORKSTREAM_CONTENT_BYTES)?;

    let mut tags = vec![
        tag(&["a", &artifact.to_a_value()])?,
        tag(&["version", version])?,
        tag(&["content-hash", &digest])?,
    ];
    blob_tags(blobs, &mut tags)?;
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_ARTIFACT_VERSION as u16), changelog).tags(tags))
}

// ---------------------------------------------------------------------------
// 47010 / 47011 / 47012 — review
// ---------------------------------------------------------------------------

/// Build a review request (kind 47010, append-only).
///
/// The target is any addressable workstream entity — an artifact, a task, a
/// decision record, or a whole workstream. This is deliberately not
/// git-specific: `buzz review request` works the same for a CAD drawing as for
/// a patch series.
///
/// ```
/// use buzz_sdk::workstream::{build_review_request, Coordinate};
/// use uuid::Uuid;
///
/// let artifact = Coordinate::new(35002, &"1".repeat(64), "chamber-bom")?;
/// let builder = build_review_request(
///     &artifact,
///     "Please sanity-check the connector choices.",
///     &[&"2".repeat(64)],
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_review_request(
    target: &Coordinate,
    content: &str,
    reviewers: &[&str],
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_reviewable(target)?;
    check_content(content, MAX_WORKSTREAM_CONTENT_BYTES)?;

    let mut tags = vec![tag(&["a", &target.to_a_value()])?];
    participant_tags(reviewers, "reviewer", &mut tags)?;
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_REVIEW_REQUEST as u16), content).tags(tags))
}

/// Build a review comment (kind 47011, append-only).
///
/// Threading follows NIP-10 markers via [`ThreadRef`], exactly like ordinary
/// Buzz message replies, so existing thread rendering works unchanged.
///
/// ```
/// use buzz_sdk::workstream::build_review_comment;
/// use buzz_sdk::ThreadRef;
/// use nostr::EventId;
/// use uuid::Uuid;
///
/// let root = EventId::from_slice(&[7u8; 32])?;
/// let thread = ThreadRef { root_event_id: root, parent_event_id: root };
/// let builder = build_review_comment(&thread, None, "connector J4 is wrong", &[Uuid::nil()])?;
/// # let _ = builder;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn build_review_comment(
    thread: &ThreadRef,
    target: Option<&Coordinate>,
    content: &str,
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    if content.trim().is_empty() {
        return Err(SdkError::InvalidInput(
            "review comment content must not be empty".into(),
        ));
    }
    check_content(content, MAX_WORKSTREAM_CONTENT_BYTES)?;

    let mut tags = Vec::new();
    let root = thread.root_event_id.to_hex();
    let parent = thread.parent_event_id.to_hex();
    if root == parent {
        tags.push(tag(&["e", &root, "", "reply"])?);
    } else {
        tags.push(tag(&["e", &root, "", "root"])?);
        tags.push(tag(&["e", &parent, "", "reply"])?);
    }
    if let Some(target) = target {
        require_reviewable(target)?;
        tags.push(tag(&["a", &target.to_a_value()])?);
    }
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_REVIEW_COMMENT as u16), content).tags(tags))
}

/// Build a review decision (kind 47012, append-only).
///
/// `request_event_id` is the 64-char hex id of the [`build_review_request`]
/// event being answered — emitted as an `e` tag so a verdict is always
/// attributable to a specific request.
///
/// ```
/// use buzz_sdk::workstream::{build_review_decision, Coordinate, ReviewVerdict};
/// use uuid::Uuid;
///
/// let artifact = Coordinate::new(35002, &"3".repeat(64), "chamber-bom")?;
/// let builder = build_review_decision(
///     &artifact,
///     &"4".repeat(64),
///     ReviewVerdict::RequestChanges,
///     "J4 pinout needs a second look",
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_review_decision(
    target: &Coordinate,
    request_event_id: &str,
    verdict: ReviewVerdict,
    content: &str,
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_reviewable(target)?;
    let request_id = check_hex_exact(request_event_id, 64, "review request id")?;
    check_content(content, MAX_WORKSTREAM_CONTENT_BYTES)?;
    if verdict != ReviewVerdict::Approve && content.trim().is_empty() {
        return Err(SdkError::InvalidInput(format!(
            "a {verdict} decision must explain itself — content must not be empty"
        )));
    }

    let mut tags = vec![
        tag(&["a", &target.to_a_value()])?,
        tag(&["e", &request_id, "", "reply"])?,
        tag(&["decision", verdict.as_str()])?,
    ];
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_REVIEW_DECISION as u16), content).tags(tags))
}

fn require_reviewable(target: &Coordinate) -> Result<(), SdkError> {
    if !REVIEWABLE_KINDS.contains(&target.kind) {
        return Err(SdkError::InvalidInput(format!(
            "review target must be a workstream (35000), task (35001), artifact (35002), \
             or decision record (35003) coordinate (got kind {})",
            target.kind
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 47020 — experiment log
// ---------------------------------------------------------------------------

/// Build an experiment log entry (kind 47020, append-only).
///
/// `labels` become `t` tags so a series of runs can be filtered without
/// inventing a new kind.
///
/// ```
/// use buzz_sdk::workstream::{build_experiment_log, Coordinate};
/// use uuid::Uuid;
///
/// let workstream = Coordinate::new(35000, &"5".repeat(64), "thermal-v2")?;
/// let builder = build_experiment_log(
///     &workstream,
///     "run-14",
///     "Soak at 85C for 6h; no drift observed.",
///     &["soak", "thermal"],
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_experiment_log(
    workstream: &Coordinate,
    experiment: &str,
    content: &str,
    labels: &[&str],
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_kind(workstream, KIND_WORKSTREAM, "workstream")?;
    check_label(experiment, "experiment id")?;
    if content.trim().is_empty() {
        return Err(SdkError::InvalidInput(
            "experiment log content must not be empty".into(),
        ));
    }
    check_content(content, MAX_WORKSTREAM_CONTENT_BYTES)?;
    if labels.len() > 16 {
        return Err(SdkError::InvalidInput(format!(
            "too many labels (max 16, got {})",
            labels.len()
        )));
    }

    let mut tags = vec![
        tag(&["a", &workstream.to_a_value()])?,
        tag(&["experiment", experiment])?,
    ];
    for label in labels {
        check_label(label, "label")?;
        tags.push(tag(&["t", label])?);
    }
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_EXPERIMENT_LOG as u16), content).tags(tags))
}

// ---------------------------------------------------------------------------
// 47021 — measurement
// ---------------------------------------------------------------------------

/// Build a measurement (kind 47021, append-only).
///
/// `value` must be a finite number; `unit` and `series` are whitespace-free
/// labels so a chart can group points without parsing content.
///
/// ```
/// use buzz_sdk::workstream::{build_measurement, Coordinate};
/// use uuid::Uuid;
///
/// let workstream = Coordinate::new(35000, &"6".repeat(64), "thermal-v2")?;
/// let builder = build_measurement(
///     &workstream,
///     "chamber-temp",
///     84.7,
///     "celsius",
///     "steady state after 6h",
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_measurement(
    subject: &Coordinate,
    series: &str,
    value: f64,
    unit: &str,
    note: &str,
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    if subject.kind != KIND_WORKSTREAM && subject.kind != KIND_ARTIFACT {
        return Err(SdkError::InvalidInput(format!(
            "measurement subject must be a workstream (35000) or artifact (35002) coordinate \
             (got kind {})",
            subject.kind
        )));
    }
    check_label(series, "series")?;
    check_label(unit, "unit")?;
    check_content(note, MAX_WORKSTREAM_CONTENT_BYTES)?;
    if !value.is_finite() {
        return Err(SdkError::InvalidInput(format!(
            "measurement value must be a finite number (got {value})"
        )));
    }

    let mut tags = vec![
        tag(&["a", &subject.to_a_value()])?,
        tag(&["series", series])?,
        tag(&["value", &value.to_string()])?,
        tag(&["unit", unit])?,
    ];
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_MEASUREMENT as u16), note).tags(tags))
}

// ---------------------------------------------------------------------------
// 47030 — handoff
// ---------------------------------------------------------------------------

/// Build a cross-functional handoff (kind 47030, append-only).
///
/// The baton pass is explicit: `["p", from, "", "from"]` and
/// `["p", to, "", "to"]` NIP-10-style markers, plus one `checklist` tag per
/// item so a client can render the acceptance list without parsing content.
///
/// ```
/// use buzz_sdk::workstream::{build_handoff, Coordinate};
/// use uuid::Uuid;
///
/// let workstream = Coordinate::new(35000, &"7".repeat(64), "thermal-v2")?;
/// let builder = build_handoff(
///     &workstream,
///     &"8".repeat(64),
///     &"9".repeat(64),
///     "Chamber is calibrated; over to data.",
///     &["probe cal sheet attached", "raw logs uploaded"],
///     &[Uuid::nil()],
/// )?;
/// # let _ = builder;
/// # Ok::<(), buzz_sdk::SdkError>(())
/// ```
pub fn build_handoff(
    workstream: &Coordinate,
    from_pubkey: &str,
    to_pubkey: &str,
    content: &str,
    checklist: &[&str],
    channels: &[Uuid],
) -> Result<EventBuilder, SdkError> {
    require_kind(workstream, KIND_WORKSTREAM, "workstream")?;
    let from = check_pubkey_hex(from_pubkey, "handoff from")?;
    let to = check_pubkey_hex(to_pubkey, "handoff to")?;
    if from == to {
        return Err(SdkError::InvalidInput(
            "handoff from and to must be different identities".into(),
        ));
    }
    check_content(content, MAX_WORKSTREAM_CONTENT_BYTES)?;
    if checklist.len() > MAX_CHECKLIST_ITEMS {
        return Err(SdkError::InvalidInput(format!(
            "too many checklist items (max {MAX_CHECKLIST_ITEMS}, got {})",
            checklist.len()
        )));
    }

    let mut tags = vec![
        tag(&["a", &workstream.to_a_value()])?,
        tag(&["p", &from, "", "from"])?,
        tag(&["p", &to, "", "to"])?,
    ];
    for item in checklist {
        check_name(item, "checklist item")?;
        tags.push(tag(&["checklist", item])?);
    }
    channel_tags(channels, &mut tags)?;

    Ok(EventBuilder::new(Kind::Custom(KIND_HANDOFF as u16), content).tags(tags))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventId, Keys};

    fn pk(seed: char) -> String {
        std::iter::repeat_n(seed, 64).collect()
    }

    fn channel() -> Uuid {
        Uuid::nil()
    }

    fn workstream_coord() -> Coordinate {
        Coordinate::new(KIND_WORKSTREAM, &pk('a'), "thermal-v2").expect("valid coordinate")
    }

    fn task_coord() -> Coordinate {
        Coordinate::new(KIND_WORKSTREAM_TASK, &pk('a'), "calibrate").expect("valid coordinate")
    }

    fn artifact_coord() -> Coordinate {
        Coordinate::new(KIND_ARTIFACT, &pk('a'), "bom").expect("valid coordinate")
    }

    fn decision_coord(id: &str) -> Coordinate {
        Coordinate::new(KIND_DECISION_RECORD, &pk('a'), id).expect("valid coordinate")
    }

    /// Build and sign so tag construction is exercised end to end.
    fn signed(builder: EventBuilder) -> nostr::Event {
        builder
            .sign_with_keys(&Keys::generate())
            .expect("sign workstream event")
    }

    fn tag_values<'a>(event: &'a nostr::Event, name: &str) -> Vec<&'a str> {
        event
            .tags
            .iter()
            .filter_map(|t| {
                let parts = t.as_slice();
                (parts.first().map(String::as_str) == Some(name))
                    .then(|| parts.get(1).map(String::as_str))
                    .flatten()
            })
            .collect()
    }

    fn default_workstream_params<'a>(channels: &'a [Uuid]) -> WorkstreamParams<'a> {
        WorkstreamParams {
            id: "thermal-v2",
            ws_type: WsType::Hardware,
            status: WorkstreamStatus::Active,
            name: "Thermal chamber v2",
            content: "",
            channels,
            members: &[],
        }
    }

    // ---- vocabularies ----

    #[test]
    fn ws_type_vocabulary_round_trips() {
        for ws_type in WsType::ALL {
            assert_eq!(WsType::parse(ws_type.as_str()).expect("known"), *ws_type);
        }
        assert_eq!(WsType::ALL.len(), 8);
    }

    #[test]
    fn ws_type_rejects_unknown_value() {
        let error = WsType::parse("firmware").expect_err("closed vocabulary");
        assert!(error.to_string().contains("ws-type must be one of"));
    }

    #[test]
    fn review_verdict_vocabulary_is_exactly_three_values() {
        assert_eq!(ReviewVerdict::ALL.len(), 3);
        for verdict in ReviewVerdict::ALL {
            assert_eq!(
                ReviewVerdict::parse(verdict.as_str()).expect("known"),
                *verdict
            );
        }
        assert!(ReviewVerdict::parse("approved").is_err());
    }

    #[test]
    fn task_and_workstream_and_decision_statuses_round_trip() {
        for status in TaskStatus::ALL {
            assert_eq!(TaskStatus::parse(status.as_str()).expect("known"), *status);
        }
        for status in WorkstreamStatus::ALL {
            assert_eq!(
                WorkstreamStatus::parse(status.as_str()).expect("known"),
                *status
            );
        }
        for status in DecisionStatus::ALL {
            assert_eq!(
                DecisionStatus::parse(status.as_str()).expect("known"),
                *status
            );
        }
        assert!(TaskStatus::parse("wip").is_err());
        assert!(WorkstreamStatus::parse("open").is_err());
        assert!(DecisionStatus::parse("draft").is_err());
    }

    // ---- coordinates ----

    #[test]
    fn coordinate_round_trips_through_a_tag_value() {
        let coord = workstream_coord();
        assert_eq!(coord.to_a_value(), format!("35000:{}:thermal-v2", pk('a')));
        assert_eq!(
            Coordinate::parse(&coord.to_a_value()).expect("parse"),
            coord
        );
    }

    #[test]
    fn coordinate_rejects_malformed_values() {
        assert!(Coordinate::parse("35000").is_err(), "missing parts");
        assert!(
            Coordinate::parse(&format!("35000:{}", pk('a'))).is_err(),
            "missing d tag"
        );
        assert!(
            Coordinate::parse(&format!("notakind:{}:x", pk('a'))).is_err(),
            "non-numeric kind"
        );
        assert!(
            Coordinate::parse("35000:not-hex:x").is_err(),
            "non-hex pubkey"
        );
        assert!(
            Coordinate::parse(&format!("35000:{}:", pk('a'))).is_err(),
            "empty d tag"
        );
    }

    #[test]
    fn coordinate_normalizes_pubkey_case() {
        let coord = Coordinate::new(KIND_WORKSTREAM, &"A".repeat(64), "x").expect("valid");
        assert_eq!(coord.pubkey, "a".repeat(64));
    }

    #[test]
    fn coordinate_parse_of_kinds_enforces_the_expected_set() {
        let value = artifact_coord().to_a_value();
        assert!(Coordinate::parse_of_kinds(&value, REVIEWABLE_KINDS).is_ok());
        assert!(Coordinate::parse_of_kinds(&value, &[KIND_WORKSTREAM]).is_err());
    }

    #[test]
    fn entity_id_rejects_coordinate_delimiters_and_traversal() {
        assert!(check_entity_id("bad:id", "id").is_err());
        assert!(check_entity_id("..", "id").is_err());
        assert!(check_entity_id(".hidden", "id").is_err());
        assert!(check_entity_id("", "id").is_err());
        assert!(check_entity_id(&"x".repeat(65), "id").is_err());
        assert!(check_entity_id("ok.id_1-2", "id").is_ok());
    }

    #[test]
    fn sha256_check_requires_full_digest() {
        assert!(check_sha256("abc", "x").is_err());
        assert!(check_sha256(&"g".repeat(64), "x").is_err());
        assert_eq!(
            check_sha256(&"AB".repeat(32), "x").expect("hex"),
            "ab".repeat(32)
        );
    }

    // ---- 35000 workstream ----

    #[test]
    fn workstream_carries_required_tags() {
        let channels = [channel()];
        let event = signed(
            build_workstream(&default_workstream_params(&channels)).expect("valid workstream"),
        );
        assert_eq!(event.kind.as_u16(), KIND_WORKSTREAM as u16);
        assert_eq!(tag_values(&event, "d"), vec!["thermal-v2"]);
        assert_eq!(tag_values(&event, "ws-type"), vec!["hardware"]);
        assert_eq!(tag_values(&event, "status"), vec!["active"]);
        assert_eq!(tag_values(&event, "name"), vec!["Thermal chamber v2"]);
        assert_eq!(tag_values(&event, "h"), vec![channel().to_string()]);
    }

    #[test]
    fn workstream_requires_a_channel() {
        let error = build_workstream(&default_workstream_params(&[]))
            .expect_err("channel scoping is mandatory");
        assert!(error.to_string().contains("at least one channel"));
    }

    #[test]
    fn workstream_rejects_bad_id_and_name() {
        let channels = [channel()];
        let mut params = default_workstream_params(&channels);
        params.id = "has spaces";
        assert!(build_workstream(&params).is_err());

        let mut params = default_workstream_params(&channels);
        params.name = "";
        assert!(build_workstream(&params).is_err());

        let long = "n".repeat(MAX_NAME_BYTES + 1);
        let mut params = default_workstream_params(&channels);
        params.name = &long;
        assert!(build_workstream(&params).is_err());
    }

    #[test]
    fn workstream_deduplicates_members_and_channels() {
        let channels = [channel(), channel()];
        let member = pk('b');
        let mut params = default_workstream_params(&channels);
        let members = [member.as_str(), member.as_str()];
        params.members = &members;
        let event = signed(build_workstream(&params).expect("valid"));
        assert_eq!(tag_values(&event, "h").len(), 1);
        assert_eq!(tag_values(&event, "p").len(), 1);
    }

    #[test]
    fn workstream_rejects_oversized_content() {
        let channels = [channel()];
        let content = "x".repeat(MAX_WORKSTREAM_CONTENT_BYTES + 1);
        let mut params = default_workstream_params(&channels);
        params.content = &content;
        assert!(build_workstream(&params).is_err());
    }

    // ---- 35001 task ----

    #[test]
    fn task_links_parent_workstream_and_assignee() {
        let workstream = workstream_coord();
        let assignee = pk('b');
        let event = signed(
            build_workstream_task(&TaskParams {
                id: "calibrate",
                workstream: &workstream,
                status: TaskStatus::InProgress,
                name: "Calibrate probe",
                content: "",
                channels: &[channel()],
                assignee: Some(&assignee),
                due: Some("2026-08-01"),
            })
            .expect("valid task"),
        );
        assert_eq!(tag_values(&event, "a"), vec![workstream.to_a_value()]);
        assert_eq!(tag_values(&event, "status"), vec!["in-progress"]);
        assert_eq!(tag_values(&event, "assignee"), vec![assignee.as_str()]);
        assert_eq!(tag_values(&event, "p"), vec![assignee.as_str()]);
        assert_eq!(tag_values(&event, "due"), vec!["2026-08-01"]);
    }

    #[test]
    fn task_rejects_non_workstream_parent() {
        let parent = task_coord();
        let error = build_workstream_task(&TaskParams {
            id: "calibrate",
            workstream: &parent,
            status: TaskStatus::Todo,
            name: "Calibrate probe",
            content: "",
            channels: &[channel()],
            assignee: None,
            due: None,
        })
        .expect_err("parent must be a workstream");
        assert!(error.to_string().contains("must name kind 35000"));
    }

    #[test]
    fn task_rejects_malformed_due_dates() {
        let workstream = workstream_coord();
        for bad in ["2026-8-1", "01-08-2026", "2026-13-01", "2026-08-32", "soon"] {
            let error = build_workstream_task(&TaskParams {
                id: "calibrate",
                workstream: &workstream,
                status: TaskStatus::Todo,
                name: "Calibrate probe",
                content: "",
                channels: &[channel()],
                assignee: None,
                due: Some(bad),
            })
            .expect_err("bad due date");
            assert!(error.to_string().contains("due"), "unexpected: {error}");
        }
    }

    // ---- 35002 artifact ----

    #[test]
    fn artifact_carries_type_version_and_blobs() {
        let digest = "a".repeat(64);
        let workstream = workstream_coord();
        let event = signed(
            build_artifact(&ArtifactParams {
                id: "chamber-bom",
                artifact_type: "bom",
                name: "Chamber BOM",
                content: "",
                workstream: Some(&workstream),
                channels: &[channel()],
                version: Some("v3"),
                blobs: &[digest.as_str(), digest.as_str()],
            })
            .expect("valid artifact"),
        );
        assert_eq!(tag_values(&event, "artifact-type"), vec!["bom"]);
        assert_eq!(tag_values(&event, "version"), vec!["v3"]);
        assert_eq!(tag_values(&event, "x"), vec![digest.as_str()], "deduped");
        assert_eq!(tag_values(&event, "a"), vec![workstream.to_a_value()]);
    }

    #[test]
    fn artifact_rejects_bad_blob_and_type() {
        assert!(build_artifact(&ArtifactParams {
            id: "chamber-bom",
            artifact_type: "bom",
            name: "Chamber BOM",
            content: "",
            workstream: None,
            channels: &[channel()],
            version: None,
            blobs: &["not-a-digest"],
        })
        .is_err());

        assert!(build_artifact(&ArtifactParams {
            id: "chamber-bom",
            artifact_type: "bill of materials",
            name: "Chamber BOM",
            content: "",
            workstream: None,
            channels: &[channel()],
            version: None,
            blobs: &[],
        })
        .is_err());
    }

    // ---- 35003 decision record ----

    #[test]
    fn decision_record_emits_supersedes_chain() {
        let workstream = workstream_coord();
        let predecessor = decision_coord("adr-0001");
        let event = signed(
            build_decision_record(&DecisionParams {
                id: "adr-0002",
                workstream: &workstream,
                status: DecisionStatus::Accepted,
                name: "Use PT100 probes",
                content: "body",
                channels: &[channel()],
                supersedes: Some(&predecessor),
            })
            .expect("valid decision"),
        );
        assert_eq!(
            tag_values(&event, "supersedes"),
            vec![predecessor.to_a_value()]
        );
        assert_eq!(tag_values(&event, "status"), vec!["accepted"]);
    }

    #[test]
    fn decision_record_rejects_self_supersession_and_wrong_kinds() {
        let workstream = workstream_coord();
        let itself = decision_coord("adr-0002");
        assert!(build_decision_record(&DecisionParams {
            id: "adr-0002",
            workstream: &workstream,
            status: DecisionStatus::Accepted,
            name: "Use PT100 probes",
            content: "body",
            channels: &[channel()],
            supersedes: Some(&itself),
        })
        .is_err());

        let wrong = artifact_coord();
        assert!(build_decision_record(&DecisionParams {
            id: "adr-0002",
            workstream: &workstream,
            status: DecisionStatus::Accepted,
            name: "Use PT100 probes",
            content: "body",
            channels: &[channel()],
            supersedes: Some(&wrong),
        })
        .is_err());
    }

    // ---- 47001 task status change ----

    #[test]
    fn task_status_change_records_transition() {
        let task = task_coord();
        let event = signed(
            build_task_status_change(
                &task,
                TaskStatus::Done,
                Some(TaskStatus::InReview),
                "landed",
                &[channel()],
            )
            .expect("valid status change"),
        );
        assert_eq!(event.kind.as_u16(), KIND_TASK_STATUS_CHANGE as u16);
        assert_eq!(tag_values(&event, "a"), vec![task.to_a_value()]);
        assert_eq!(tag_values(&event, "status"), vec!["done"]);
        assert_eq!(tag_values(&event, "previous-status"), vec!["in-review"]);
        assert_eq!(event.content, "landed");
    }

    #[test]
    fn task_status_change_rejects_no_op_and_wrong_target() {
        let task = task_coord();
        assert!(build_task_status_change(
            &task,
            TaskStatus::Done,
            Some(TaskStatus::Done),
            "",
            &[channel()]
        )
        .is_err());

        let workstream = workstream_coord();
        assert!(
            build_task_status_change(&workstream, TaskStatus::Done, None, "", &[channel()])
                .is_err()
        );
    }

    // ---- 47002 artifact version ----

    #[test]
    fn artifact_version_requires_full_content_hash() {
        let artifact = artifact_coord();
        let digest = "b".repeat(64);
        let event = signed(
            build_artifact_version(
                &artifact,
                "v2",
                &digest,
                "changelog",
                &[digest.as_str()],
                &[channel()],
            )
            .expect("valid version"),
        );
        assert_eq!(tag_values(&event, "content-hash"), vec![digest.as_str()]);
        assert_eq!(tag_values(&event, "version"), vec!["v2"]);

        assert!(
            build_artifact_version(&artifact, "v2", "short", "", &[], &[channel()]).is_err(),
            "truncated digest"
        );
        let workstream = workstream_coord();
        assert!(
            build_artifact_version(&workstream, "v2", &digest, "", &[], &[channel()]).is_err(),
            "wrong target kind"
        );
    }

    // ---- 47010/47011/47012 review ----

    #[test]
    fn review_request_accepts_every_reviewable_kind() {
        for kind in REVIEWABLE_KINDS {
            let coord = Coordinate::new(*kind, &pk('a'), "x").expect("coord");
            assert!(
                build_review_request(&coord, "please review", &[], &[channel()]).is_ok(),
                "kind {kind} should be reviewable"
            );
        }
        let unrelated = Coordinate::new(30023, &pk('a'), "x").expect("coord");
        assert!(build_review_request(&unrelated, "please review", &[], &[channel()]).is_err());
    }

    #[test]
    fn review_comment_emits_nip10_markers() {
        let root = EventId::from_slice(&[1u8; 32]).expect("event id");
        let child = EventId::from_slice(&[2u8; 32]).expect("event id");

        let direct = signed(
            build_review_comment(
                &ThreadRef {
                    root_event_id: root,
                    parent_event_id: root,
                },
                None,
                "top-level",
                &[channel()],
            )
            .expect("valid comment"),
        );
        let markers: Vec<&str> = direct
            .tags
            .iter()
            .filter_map(|t| {
                let parts = t.as_slice();
                (parts.first().map(String::as_str) == Some("e"))
                    .then(|| parts.get(3).map(String::as_str))
                    .flatten()
            })
            .collect();
        assert_eq!(markers, vec!["reply"]);

        let nested = signed(
            build_review_comment(
                &ThreadRef {
                    root_event_id: root,
                    parent_event_id: child,
                },
                None,
                "nested",
                &[channel()],
            )
            .expect("valid comment"),
        );
        let markers: Vec<&str> = nested
            .tags
            .iter()
            .filter_map(|t| {
                let parts = t.as_slice();
                (parts.first().map(String::as_str) == Some("e"))
                    .then(|| parts.get(3).map(String::as_str))
                    .flatten()
            })
            .collect();
        assert_eq!(markers, vec!["root", "reply"]);
    }

    #[test]
    fn review_comment_rejects_empty_content() {
        let root = EventId::from_slice(&[1u8; 32]).expect("event id");
        assert!(build_review_comment(
            &ThreadRef {
                root_event_id: root,
                parent_event_id: root,
            },
            None,
            "   ",
            &[channel()],
        )
        .is_err());
    }

    #[test]
    fn review_decision_requires_rationale_for_non_approvals() {
        let artifact = artifact_coord();
        let request = "c".repeat(64);

        assert!(build_review_decision(
            &artifact,
            &request,
            ReviewVerdict::Approve,
            "",
            &[channel()]
        )
        .is_ok());

        for verdict in [ReviewVerdict::RequestChanges, ReviewVerdict::Reject] {
            assert!(
                build_review_decision(&artifact, &request, verdict, "  ", &[channel()]).is_err(),
                "{verdict} must explain itself"
            );
        }

        let event = signed(
            build_review_decision(
                &artifact,
                &request,
                ReviewVerdict::Reject,
                "wrong approach",
                &[channel()],
            )
            .expect("valid decision"),
        );
        assert_eq!(tag_values(&event, "decision"), vec!["reject"]);
        assert_eq!(tag_values(&event, "e"), vec![request.as_str()]);
    }

    #[test]
    fn review_decision_rejects_malformed_request_id() {
        let artifact = artifact_coord();
        assert!(
            build_review_decision(&artifact, "abc", ReviewVerdict::Approve, "", &[channel()])
                .is_err()
        );
    }

    // ---- 47020 experiment log ----

    #[test]
    fn experiment_log_emits_labels_as_t_tags() {
        let workstream = workstream_coord();
        let event = signed(
            build_experiment_log(
                &workstream,
                "run-14",
                "soak passed",
                &["soak", "thermal"],
                &[channel()],
            )
            .expect("valid log"),
        );
        assert_eq!(tag_values(&event, "experiment"), vec!["run-14"]);
        assert_eq!(tag_values(&event, "t"), vec!["soak", "thermal"]);

        assert!(build_experiment_log(&workstream, "run-14", "  ", &[], &[channel()]).is_err());
        let artifact = artifact_coord();
        assert!(build_experiment_log(&artifact, "run-14", "x", &[], &[channel()]).is_err());
    }

    // ---- 47021 measurement ----

    #[test]
    fn measurement_carries_series_value_unit() {
        let workstream = workstream_coord();
        let event = signed(
            build_measurement(
                &workstream,
                "chamber-temp",
                84.5,
                "celsius",
                "",
                &[channel()],
            )
            .expect("valid measurement"),
        );
        assert_eq!(tag_values(&event, "series"), vec!["chamber-temp"]);
        assert_eq!(tag_values(&event, "value"), vec!["84.5"]);
        assert_eq!(tag_values(&event, "unit"), vec!["celsius"]);
    }

    #[test]
    fn measurement_rejects_non_finite_values_and_bad_subject() {
        let workstream = workstream_coord();
        assert!(build_measurement(&workstream, "s", f64::NAN, "c", "", &[channel()]).is_err());
        assert!(build_measurement(&workstream, "s", f64::INFINITY, "c", "", &[channel()]).is_err());
        assert!(build_measurement(&workstream, "s", 1.0, "deg C", "", &[channel()]).is_err());

        let task = task_coord();
        assert!(build_measurement(&task, "s", 1.0, "c", "", &[channel()]).is_err());
    }

    #[test]
    fn measurement_accepts_an_artifact_subject() {
        let artifact = artifact_coord();
        assert!(build_measurement(&artifact, "mass", 1.25, "kg", "", &[channel()]).is_ok());
    }

    // ---- 47030 handoff ----

    #[test]
    fn handoff_marks_from_and_to() {
        let workstream = workstream_coord();
        let from = pk('b');
        let to = pk('c');
        let event = signed(
            build_handoff(
                &workstream,
                &from,
                &to,
                "over to you",
                &["cal sheet attached"],
                &[channel()],
            )
            .expect("valid handoff"),
        );
        let markers: Vec<(&str, &str)> = event
            .tags
            .iter()
            .filter_map(|t| {
                let parts = t.as_slice();
                if parts.first().map(String::as_str) != Some("p") {
                    return None;
                }
                Some((parts.get(1)?.as_str(), parts.get(3)?.as_str()))
            })
            .collect();
        assert_eq!(markers, vec![(from.as_str(), "from"), (to.as_str(), "to")]);
        assert_eq!(tag_values(&event, "checklist"), vec!["cal sheet attached"]);
    }

    #[test]
    fn handoff_rejects_self_pass_and_bad_pubkeys() {
        let workstream = workstream_coord();
        let same = pk('b');
        assert!(build_handoff(&workstream, &same, &same, "", &[], &[channel()]).is_err());
        assert!(build_handoff(&workstream, "nope", &pk('c'), "", &[], &[channel()]).is_err());
    }

    #[test]
    fn handoff_enforces_checklist_cap() {
        let workstream = workstream_coord();
        let items: Vec<String> = (0..=MAX_CHECKLIST_ITEMS)
            .map(|i| format!("item {i}"))
            .collect();
        let refs: Vec<&str> = items.iter().map(String::as_str).collect();
        assert!(build_handoff(&workstream, &pk('b'), &pk('c'), "", &refs, &[channel()]).is_err());
    }

    // ---- cross-cutting ----

    #[test]
    fn every_builder_emits_a_channel_tag() {
        let workstream = workstream_coord();
        let task = task_coord();
        let artifact = artifact_coord();
        let digest = "d".repeat(64);
        let root = EventId::from_slice(&[3u8; 32]).expect("event id");
        let channels = [channel()];

        let events = vec![
            signed(build_workstream(&default_workstream_params(&channels)).expect("workstream")),
            signed(
                build_workstream_task(&TaskParams {
                    id: "t",
                    workstream: &workstream,
                    status: TaskStatus::Todo,
                    name: "t",
                    content: "",
                    channels: &channels,
                    assignee: None,
                    due: None,
                })
                .expect("task"),
            ),
            signed(
                build_artifact(&ArtifactParams {
                    id: "a",
                    artifact_type: "doc",
                    name: "a",
                    content: "",
                    workstream: None,
                    channels: &channels,
                    version: None,
                    blobs: &[],
                })
                .expect("artifact"),
            ),
            signed(
                build_decision_record(&DecisionParams {
                    id: "adr-1",
                    workstream: &workstream,
                    status: DecisionStatus::Proposed,
                    name: "d",
                    content: "",
                    channels: &channels,
                    supersedes: None,
                })
                .expect("decision"),
            ),
            signed(
                build_task_status_change(&task, TaskStatus::Done, None, "", &channels)
                    .expect("status change"),
            ),
            signed(
                build_artifact_version(&artifact, "v1", &digest, "", &[], &channels)
                    .expect("version"),
            ),
            signed(build_review_request(&artifact, "r", &[], &channels).expect("request")),
            signed(
                build_review_comment(
                    &ThreadRef {
                        root_event_id: root,
                        parent_event_id: root,
                    },
                    None,
                    "c",
                    &channels,
                )
                .expect("comment"),
            ),
            signed(
                build_review_decision(
                    &artifact,
                    &"e".repeat(64),
                    ReviewVerdict::Approve,
                    "",
                    &channels,
                )
                .expect("decision"),
            ),
            signed(
                build_experiment_log(&workstream, "run", "x", &[], &channels).expect("experiment"),
            ),
            signed(
                build_measurement(&workstream, "s", 1.0, "kg", "", &channels).expect("measurement"),
            ),
            signed(
                build_handoff(&workstream, &pk('b'), &pk('c'), "", &[], &channels)
                    .expect("handoff"),
            ),
        ];

        assert_eq!(events.len(), 12, "one event per workstream kind");
        for event in &events {
            assert_eq!(
                tag_values(event, "h"),
                vec![channel().to_string()],
                "kind {} must be channel-scoped",
                event.kind.as_u16()
            );
        }

        let kinds: Vec<u16> = events.iter().map(|e| e.kind.as_u16()).collect();
        assert_eq!(
            kinds,
            vec![
                KIND_WORKSTREAM as u16,
                KIND_WORKSTREAM_TASK as u16,
                KIND_ARTIFACT as u16,
                KIND_DECISION_RECORD as u16,
                KIND_TASK_STATUS_CHANGE as u16,
                KIND_ARTIFACT_VERSION as u16,
                KIND_REVIEW_REQUEST as u16,
                KIND_REVIEW_COMMENT as u16,
                KIND_REVIEW_DECISION as u16,
                KIND_EXPERIMENT_LOG as u16,
                KIND_MEASUREMENT as u16,
                KIND_HANDOFF as u16,
            ]
        );
    }
}

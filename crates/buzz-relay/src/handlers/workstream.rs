//! Workstream kind-family validation (Hive plan §5.1–5.2).
//!
//! The Workstream model is expressed entirely as Nostr events — there are no
//! Workstream-specific HTTP endpoints and no Workstream-specific storage
//! paths. Everything else these kinds need (NIP-29 `h` scoping, tenant
//! isolation, filter matching, FTS, audit, fan-out, the `POST /events|/query
//! |/count` bridge) rides the generic event pipeline. This module is the one
//! Workstream-specific thing the relay does: a tag-shape gate at ingest so a
//! malformed head cannot land in storage.
//!
//! Two families:
//!
//! * **Addressable heads (35000–35003)** — NIP-33 parameterized-replaceable,
//!   identified by their `d` tag. They are replaced last-write-wins by the
//!   *existing* [`buzz_db`] machinery (`replace_parameterized_event`), reached
//!   because [`buzz_core::kind::is_parameterized_replaceable`] is range-based
//!   over 30000–39999; no Workstream-specific replace path exists.
//! * **Append-only history (47001–47030)** — regular events that reference a
//!   head through an `a` tag (NIP-01 coordinate) or an `e` tag.
//!
//! # Offline-first: references are format-validated, not resolved
//!
//! Every `a`/`e`/`supersedes` reference below is checked for *shape* only —
//! the relay never asks whether the referenced coordinate or event actually
//! exists. This is deliberate. Workstream clients are expected to compose
//! offline and sync later, so a task may legitimately arrive before the
//! workstream that contains it, or a version before its artifact head. An
//! existence check would make admission depend on arrival order and turn a
//! transient sync gap into permanent data loss. Dangling references are a
//! client-side display concern, not an admission concern.

use nostr::Event;

use buzz_core::kind::{
    KIND_ARTIFACT, KIND_ARTIFACT_VERSION, KIND_DECISION_RECORD, KIND_EXPERIMENT_LOG, KIND_HANDOFF,
    KIND_MEASUREMENT, KIND_REVIEW_COMMENT, KIND_REVIEW_DECISION, KIND_REVIEW_REQUEST,
    KIND_TASK_STATUS_CHANGE, KIND_WORKSTREAM, KIND_WORKSTREAM_TASK,
};

/// The `ws-type` tag vocabulary (Hive plan §5.2).
///
/// The type selects client presentation and the default agent-persona pack —
/// it never changes relay behavior. The relay validates it only so a typo
/// cannot silently create an unroutable workstream. Extending the vocabulary
/// is an edit to this list.
pub const WS_TYPES: [&str; 8] = [
    "code", "systems", "hardware", "data", "design", "process", "docs", "general",
];

/// Verdicts a `kind:47012` review decision may carry.
pub const REVIEW_DECISIONS: [&str; 3] = ["approve", "request-changes", "reject"];

/// Returns `true` when `kind` belongs to the Workstream family and must be
/// checked by [`validate_workstream_event`].
///
/// Covers both the addressable heads (35000–35003) and the append-only
/// history events (47001–47030).
pub const fn is_workstream_kind(kind: u32) -> bool {
    matches!(
        kind,
        KIND_WORKSTREAM
            | KIND_WORKSTREAM_TASK
            | KIND_ARTIFACT
            | KIND_DECISION_RECORD
            | KIND_TASK_STATUS_CHANGE
            | KIND_ARTIFACT_VERSION
            | KIND_REVIEW_REQUEST
            | KIND_REVIEW_COMMENT
            | KIND_REVIEW_DECISION
            | KIND_EXPERIMENT_LOG
            | KIND_MEASUREMENT
            | KIND_HANDOFF
    )
}

/// Collect the first value of every tag named `name`.
///
/// Tags with no value (`["d"]`) are skipped — they carry no information and
/// every rule below is expressed in terms of values.
fn tag_values<'a>(event: &'a Event, name: &str) -> Vec<&'a str> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            if parts.len() >= 2 && parts[0].as_str() == name {
                Some(parts[1].as_str())
            } else {
                None
            }
        })
        .collect()
}

/// Returns `true` when `s` is exactly `len` lowercase-hex characters.
///
/// Lowercase-only, on purpose: Nostr tag matching is byte-exact, so a head
/// stored under an uppercase pubkey or id would be invisible to the
/// lowercase filters every client actually sends.
fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Require exactly one non-empty `d` tag — the addressable identity.
///
/// An addressable event with no `d` tag replaces under the empty coordinate
/// `(pubkey, kind, "")`, which collapses every such event of that kind into a
/// single slot. That is silent last-write-wins data loss, so it is rejected
/// at admission rather than absorbed.
fn require_d_tag(event: &Event, label: &str) -> Result<(), String> {
    let d_tags = tag_values(event, "d");
    match d_tags.len() {
        1 if !d_tags[0].trim().is_empty() => Ok(()),
        1 => Err(format!("{label} `d` tag must not be empty")),
        0 => Err(format!("{label} requires a `d` tag")),
        n => Err(format!("{label} must have exactly one `d` tag (got {n})")),
    }
}

/// Parse a NIP-01 addressable coordinate `<kind>:<pubkey-hex>:<d>`.
///
/// Returns the referenced kind on success. Format only — see the module
/// docs on why existence is deliberately not checked.
fn parse_coordinate(value: &str) -> Result<u32, String> {
    let mut parts = value.splitn(3, ':');
    let (kind_str, pubkey, d) = match (parts.next(), parts.next(), parts.next()) {
        (Some(k), Some(p), Some(d)) => (k, p, d),
        _ => {
            return Err(format!(
                "`a` tag must be a `<kind>:<pubkey-hex>:<d>` coordinate (got `{value}`)"
            ))
        }
    };
    let kind: u32 = kind_str
        .parse()
        .map_err(|_| format!("`a` tag coordinate has a non-numeric kind (got `{kind_str}`)"))?;
    if !is_lower_hex(pubkey, 64) {
        return Err("`a` tag coordinate pubkey must be 64 lowercase hex chars".to_string());
    }
    if d.is_empty() {
        return Err("`a` tag coordinate must name a non-empty `d` identifier".to_string());
    }
    Ok(kind)
}

/// Require at least one `a` tag whose coordinate names one of `expected`.
///
/// Extra `a` tags pointing elsewhere are allowed — an event may reference
/// several things — as long as one of them satisfies the rule.
fn require_a_coordinate(event: &Event, label: &str, expected: &[u32]) -> Result<(), String> {
    let a_tags = tag_values(event, "a");
    if a_tags.is_empty() {
        return Err(format!(
            "{label} requires an `a` tag referencing {}",
            describe_kinds(expected)
        ));
    }
    let mut last_err: Option<String> = None;
    for value in &a_tags {
        match parse_coordinate(value) {
            Ok(kind) if expected.contains(&kind) => return Ok(()),
            Ok(kind) => {
                last_err = Some(format!(
                    "{label} `a` tag must reference {} (got kind {kind})",
                    describe_kinds(expected)
                ));
            }
            Err(e) => last_err = Some(format!("{label} {e}")),
        }
    }
    Err(last_err.unwrap_or_else(|| format!("{label} has no usable `a` tag")))
}

/// Render an expected-kind list for an error message: `35001` or
/// `one of 35001/35002/35003`.
fn describe_kinds(kinds: &[u32]) -> String {
    if kinds.len() == 1 {
        format!("a kind {} coordinate", kinds[0])
    } else {
        let list = kinds
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join("/");
        format!("one of the kind {list} coordinates")
    }
}

/// Validate a Workstream-family event's tag shape.
///
/// Returns `Err(reason)` with a bare reason; the caller in `ingest.rs`
/// prefixes it with `invalid: ` and turns it into an `OK false` frame. This
/// function never panics and never touches the database — it is a pure
/// function of the event, which is what makes it exhaustively unit-testable.
///
/// Kinds outside the family (see [`is_workstream_kind`]) are accepted
/// unchanged so a mistaken call site cannot start rejecting unrelated events.
///
/// The NIP-29 `h` tag requirement common to all these kinds is **not**
/// enforced here — it is enforced by the shared
/// `ingest::requires_h_channel_scope` gate, which these kinds are registered
/// with, so Workstream events report the same
/// `invalid: channel-scoped events must include an h tag` message every other
/// channel-scoped kind does.
pub fn validate_workstream_event(event: &Event) -> Result<(), String> {
    let kind = buzz_core::kind::event_kind_u32(event);
    match kind {
        KIND_WORKSTREAM => validate_workstream(event),
        KIND_WORKSTREAM_TASK => {
            require_d_tag(event, "workstream-task")?;
            require_a_coordinate(event, "workstream-task", &[KIND_WORKSTREAM])
        }
        KIND_ARTIFACT => validate_artifact(event),
        KIND_DECISION_RECORD => validate_decision_record(event),
        KIND_TASK_STATUS_CHANGE => {
            require_a_coordinate(event, "task-status-change", &[KIND_WORKSTREAM_TASK])
        }
        KIND_ARTIFACT_VERSION => require_a_coordinate(event, "artifact-version", &[KIND_ARTIFACT]),
        KIND_REVIEW_REQUEST => require_a_coordinate(
            event,
            "review-request",
            &[KIND_WORKSTREAM_TASK, KIND_ARTIFACT, KIND_DECISION_RECORD],
        ),
        KIND_REVIEW_COMMENT => validate_review_comment(event),
        KIND_REVIEW_DECISION => validate_review_decision(event),
        KIND_EXPERIMENT_LOG => require_a_coordinate(event, "experiment-log", &[KIND_WORKSTREAM]),
        KIND_MEASUREMENT => require_a_coordinate(event, "measurement", &[KIND_WORKSTREAM]),
        KIND_HANDOFF => validate_handoff(event),
        _ => Ok(()),
    }
}

/// kind:35000 — the workstream container.
///
/// Requires a non-empty `d` (the workstream id). A `ws-type` tag is optional
/// but, when present, must name a known type. `status`, `h`, `p` and any
/// other tags pass through untouched.
fn validate_workstream(event: &Event) -> Result<(), String> {
    require_d_tag(event, "workstream")?;
    for ws_type in tag_values(event, "ws-type") {
        if !WS_TYPES.contains(&ws_type) {
            return Err(format!(
                "unknown ws-type `{ws_type}` (expected one of {})",
                WS_TYPES.join(", ")
            ));
        }
    }
    Ok(())
}

/// kind:35002 — the artifact head.
///
/// Requires a non-empty `d`. An `x` tag (the current content hash, a
/// Blossom/MediaStore sha-256) is optional but must be well-formed when
/// present — a truncated hash would silently never resolve to a blob.
fn validate_artifact(event: &Event) -> Result<(), String> {
    require_d_tag(event, "artifact")?;
    for x in tag_values(event, "x") {
        if !is_lower_hex(x, 64) {
            return Err("artifact `x` tag must be a 64 lowercase hex sha-256".to_string());
        }
    }
    Ok(())
}

/// kind:35003 — the decision record.
///
/// Requires a non-empty `d` and an `a` tag naming its workstream. The
/// optional `supersedes` tag chains ADRs and must be a 64-hex event id.
fn validate_decision_record(event: &Event) -> Result<(), String> {
    require_d_tag(event, "decision-record")?;
    require_a_coordinate(event, "decision-record", &[KIND_WORKSTREAM])?;
    for supersedes in tag_values(event, "supersedes") {
        if !is_lower_hex(supersedes, 64) {
            return Err(
                "decision-record `supersedes` tag must be a 64 lowercase hex event id".to_string(),
            );
        }
    }
    Ok(())
}

/// kind:47011 — a comment inside a review thread.
///
/// NIP-10 threading tags pass through as-is; the relay only insists that at
/// least one `e` tag anchors the comment to something, and that the ids are
/// well-formed. Without an anchor a comment is unreachable from any thread.
fn validate_review_comment(event: &Event) -> Result<(), String> {
    let e_tags = tag_values(event, "e");
    if e_tags.is_empty() {
        return Err("review-comment requires an `e` tag (NIP-10 thread anchor)".to_string());
    }
    for e in e_tags {
        if !is_lower_hex(e, 64) {
            return Err("review-comment `e` tag must be a 64 lowercase hex event id".to_string());
        }
    }
    Ok(())
}

/// kind:47012 — a review verdict.
///
/// Needs a target (an `a` coordinate or an `e` event id — a decision may
/// answer either a review request event or the reviewed head directly) and a
/// `decision` tag drawn from [`REVIEW_DECISIONS`].
fn validate_review_decision(event: &Event) -> Result<(), String> {
    let a_tags = tag_values(event, "a");
    let e_tags = tag_values(event, "e");
    if a_tags.is_empty() && e_tags.is_empty() {
        return Err("review-decision requires an `a` or `e` tag naming what was reviewed".into());
    }
    for a in &a_tags {
        parse_coordinate(a).map_err(|e| format!("review-decision {e}"))?;
    }
    for e in &e_tags {
        if !is_lower_hex(e, 64) {
            return Err("review-decision `e` tag must be a 64 lowercase hex event id".to_string());
        }
    }
    let decisions = tag_values(event, "decision");
    let verdict = match decisions.len() {
        1 => decisions[0],
        0 => return Err("review-decision requires a `decision` tag".to_string()),
        n => {
            return Err(format!(
                "review-decision must have exactly one `decision` tag (got {n})"
            ))
        }
    };
    if !REVIEW_DECISIONS.contains(&verdict) {
        return Err(format!(
            "unknown decision `{verdict}` (expected one of {})",
            REVIEW_DECISIONS.join(", ")
        ));
    }
    Ok(())
}

/// kind:47030 — a cross-functional handoff.
///
/// A handoff is a baton pass, so it must name at least one counterparty via a
/// `p` tag. The author's own pubkey is the implicit "from"; an explicit
/// `from`-style `p` tag is allowed and equally well-formed. Payload/checklist
/// content is opaque to the relay.
fn validate_handoff(event: &Event) -> Result<(), String> {
    let p_tags = tag_values(event, "p");
    if p_tags.is_empty() {
        return Err("handoff requires at least one `p` tag (the counterparty)".to_string());
    }
    for p in p_tags {
        if !is_lower_hex(p, 64) {
            return Err("handoff `p` tag must be a 64 lowercase hex pubkey".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    /// Build a signed event of `kind` carrying `tags` (`[name, value, ...]`).
    fn ev(kind: u32, tags: &[&[&str]]) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::Custom(kind as u16), "")
            .tags(
                tags.iter()
                    .map(|t| Tag::parse(t.iter().copied()).expect("tag parses"))
                    .collect::<Vec<_>>(),
            )
            .sign_with_keys(&keys)
            .expect("sign")
    }

    fn pk() -> String {
        Keys::generate().public_key().to_hex()
    }

    fn hex64() -> String {
        "a".repeat(64)
    }

    fn ws_coord() -> String {
        format!("{KIND_WORKSTREAM}:{}:ws-1", pk())
    }

    fn task_coord() -> String {
        format!("{KIND_WORKSTREAM_TASK}:{}:task-1", pk())
    }

    fn artifact_coord() -> String {
        format!("{KIND_ARTIFACT}:{}:art-1", pk())
    }

    fn decision_coord() -> String {
        format!("{KIND_DECISION_RECORD}:{}:dec-1", pk())
    }

    #[test]
    fn family_membership_is_exactly_the_twelve_kinds() {
        for kind in [
            KIND_WORKSTREAM,
            KIND_WORKSTREAM_TASK,
            KIND_ARTIFACT,
            KIND_DECISION_RECORD,
            KIND_TASK_STATUS_CHANGE,
            KIND_ARTIFACT_VERSION,
            KIND_REVIEW_REQUEST,
            KIND_REVIEW_COMMENT,
            KIND_REVIEW_DECISION,
            KIND_EXPERIMENT_LOG,
            KIND_MEASUREMENT,
            KIND_HANDOFF,
        ] {
            assert!(is_workstream_kind(kind), "kind {kind} should be in family");
        }
        for kind in [9u32, 39000, 34999, 35004, 47000, 47031, 48001] {
            assert!(
                !is_workstream_kind(kind),
                "kind {kind} should not be in family"
            );
        }
    }

    /// The 35xxx block must reach the *existing* NIP-33 replace path — that
    /// is the whole reason no Workstream-specific LWW machinery exists.
    #[test]
    fn workstream_heads_are_parameterized_replaceable() {
        for kind in [
            KIND_WORKSTREAM,
            KIND_WORKSTREAM_TASK,
            KIND_ARTIFACT,
            KIND_DECISION_RECORD,
        ] {
            assert!(
                buzz_core::kind::is_parameterized_replaceable(kind),
                "kind {kind} must be NIP-33 parameterized-replaceable"
            );
            assert!(
                !buzz_core::kind::is_replaceable(kind),
                "kind {kind} must not also be NIP-16 replaceable"
            );
        }
        // The 47xxx history block must NOT be replaceable — it is the record.
        for kind in [
            KIND_TASK_STATUS_CHANGE,
            KIND_ARTIFACT_VERSION,
            KIND_REVIEW_REQUEST,
            KIND_REVIEW_COMMENT,
            KIND_REVIEW_DECISION,
            KIND_EXPERIMENT_LOG,
            KIND_MEASUREMENT,
            KIND_HANDOFF,
        ] {
            assert!(!buzz_core::kind::is_parameterized_replaceable(kind));
            assert!(!buzz_core::kind::is_replaceable(kind));
        }
    }

    #[test]
    fn non_family_kinds_pass_through() {
        assert!(validate_workstream_event(&ev(9, &[])).is_ok());
    }

    // ── 35000 workstream ────────────────────────────────────────────────

    #[test]
    fn workstream_accepts_d_and_known_ws_type() {
        for ws_type in WS_TYPES {
            let e = ev(
                KIND_WORKSTREAM,
                &[&["d", "ws-1"], &["ws-type", ws_type], &["status", "active"]],
            );
            assert!(
                validate_workstream_event(&e).is_ok(),
                "ws-type {ws_type} should be accepted"
            );
        }
    }

    #[test]
    fn workstream_accepts_without_ws_type() {
        let e = ev(KIND_WORKSTREAM, &[&["d", "ws-1"]]);
        assert!(validate_workstream_event(&e).is_ok());
    }

    #[test]
    fn workstream_rejects_missing_d() {
        let e = ev(KIND_WORKSTREAM, &[&["ws-type", "code"]]);
        assert_eq!(
            validate_workstream_event(&e).unwrap_err(),
            "workstream requires a `d` tag"
        );
    }

    #[test]
    fn workstream_rejects_empty_d() {
        let e = ev(KIND_WORKSTREAM, &[&["d", "  "]]);
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("must not be empty"));
    }

    #[test]
    fn workstream_rejects_duplicate_d() {
        let e = ev(KIND_WORKSTREAM, &[&["d", "ws-1"], &["d", "ws-2"]]);
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("exactly one `d` tag"));
    }

    #[test]
    fn workstream_rejects_unknown_ws_type() {
        let e = ev(KIND_WORKSTREAM, &[&["d", "ws-1"], &["ws-type", "quantum"]]);
        let err = validate_workstream_event(&e).unwrap_err();
        assert!(err.starts_with("unknown ws-type"), "got: {err}");
    }

    // ── 35001 workstream task ───────────────────────────────────────────

    #[test]
    fn task_accepts_d_and_workstream_coordinate() {
        let e = ev(
            KIND_WORKSTREAM_TASK,
            &[
                &["d", "task-1"],
                &["a", &ws_coord()],
                &["status", "in-progress"],
            ],
        );
        assert!(validate_workstream_event(&e).is_ok());
    }

    #[test]
    fn task_rejects_missing_a() {
        let e = ev(KIND_WORKSTREAM_TASK, &[&["d", "task-1"]]);
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("requires an `a` tag"));
    }

    #[test]
    fn task_rejects_missing_d() {
        let e = ev(KIND_WORKSTREAM_TASK, &[&["a", &ws_coord()]]);
        assert_eq!(
            validate_workstream_event(&e).unwrap_err(),
            "workstream-task requires a `d` tag"
        );
    }

    #[test]
    fn task_rejects_wrong_coordinate_kind() {
        let e = ev(
            KIND_WORKSTREAM_TASK,
            &[&["d", "task-1"], &["a", &artifact_coord()]],
        );
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("got kind 35002"));
    }

    #[test]
    fn task_rejects_malformed_coordinate() {
        for bad in [
            "35000".to_string(),
            "35000:not-hex:ws-1".to_string(),
            format!("35000:{}:", pk()),
            format!("ws:{}:ws-1", pk()),
            format!("35000:{}:ws-1", pk().to_uppercase()),
        ] {
            let e = ev(KIND_WORKSTREAM_TASK, &[&["d", "task-1"], &["a", &bad]]);
            assert!(
                validate_workstream_event(&e).is_err(),
                "coordinate `{bad}` should be rejected"
            );
        }
    }

    #[test]
    fn task_accepts_when_one_of_several_a_tags_matches() {
        let e = ev(
            KIND_WORKSTREAM_TASK,
            &[
                &["d", "task-1"],
                &["a", &artifact_coord()],
                &["a", &ws_coord()],
            ],
        );
        assert!(validate_workstream_event(&e).is_ok());
    }

    // ── 35002 artifact ──────────────────────────────────────────────────

    #[test]
    fn artifact_accepts_with_and_without_x() {
        assert!(validate_workstream_event(&ev(KIND_ARTIFACT, &[&["d", "art-1"]])).is_ok());
        let e = ev(KIND_ARTIFACT, &[&["d", "art-1"], &["x", &hex64()]]);
        assert!(validate_workstream_event(&e).is_ok());
    }

    #[test]
    fn artifact_rejects_missing_d() {
        let e = ev(KIND_ARTIFACT, &[&["x", &hex64()]]);
        assert_eq!(
            validate_workstream_event(&e).unwrap_err(),
            "artifact requires a `d` tag"
        );
    }

    #[test]
    fn artifact_rejects_malformed_x() {
        for bad in ["deadbeef", &hex64().to_uppercase(), "zz"] {
            let e = ev(KIND_ARTIFACT, &[&["d", "art-1"], &["x", bad]]);
            assert!(
                validate_workstream_event(&e).is_err(),
                "x `{bad}` should be rejected"
            );
        }
    }

    // ── 35003 decision record ───────────────────────────────────────────

    #[test]
    fn decision_record_accepts_with_supersedes() {
        let e = ev(
            KIND_DECISION_RECORD,
            &[
                &["d", "dec-2"],
                &["a", &ws_coord()],
                &["supersedes", &hex64()],
            ],
        );
        assert!(validate_workstream_event(&e).is_ok());
    }

    #[test]
    fn decision_record_rejects_missing_a() {
        let e = ev(KIND_DECISION_RECORD, &[&["d", "dec-1"]]);
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("requires an `a` tag"));
    }

    #[test]
    fn decision_record_rejects_bad_supersedes() {
        let e = ev(
            KIND_DECISION_RECORD,
            &[&["d", "dec-2"], &["a", &ws_coord()], &["supersedes", "abc"]],
        );
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("`supersedes`"));
    }

    // ── 47001 / 47002 ───────────────────────────────────────────────────

    #[test]
    fn task_status_change_requires_task_coordinate() {
        let ok = ev(
            KIND_TASK_STATUS_CHANGE,
            &[&["a", &task_coord()], &["status", "done"]],
        );
        assert!(validate_workstream_event(&ok).is_ok());

        let missing = ev(KIND_TASK_STATUS_CHANGE, &[&["status", "done"]]);
        assert!(validate_workstream_event(&missing).is_err());

        let wrong = ev(KIND_TASK_STATUS_CHANGE, &[&["a", &ws_coord()]]);
        assert!(validate_workstream_event(&wrong).is_err());
    }

    #[test]
    fn artifact_version_requires_artifact_coordinate() {
        let ok = ev(KIND_ARTIFACT_VERSION, &[&["a", &artifact_coord()]]);
        assert!(validate_workstream_event(&ok).is_ok());

        let wrong = ev(KIND_ARTIFACT_VERSION, &[&["a", &task_coord()]]);
        assert!(validate_workstream_event(&wrong).is_err());

        let missing = ev(KIND_ARTIFACT_VERSION, &[]);
        assert!(validate_workstream_event(&missing).is_err());
    }

    // ── 47010 review request ────────────────────────────────────────────

    #[test]
    fn review_request_accepts_task_artifact_or_decision() {
        for coord in [task_coord(), artifact_coord(), decision_coord()] {
            let e = ev(KIND_REVIEW_REQUEST, &[&["a", &coord]]);
            assert!(
                validate_workstream_event(&e).is_ok(),
                "coordinate {coord} should be reviewable"
            );
        }
    }

    #[test]
    fn review_request_rejects_workstream_coordinate_and_missing_a() {
        let wrong = ev(KIND_REVIEW_REQUEST, &[&["a", &ws_coord()]]);
        assert!(validate_workstream_event(&wrong).is_err());
        assert!(validate_workstream_event(&ev(KIND_REVIEW_REQUEST, &[])).is_err());
    }

    // ── 47011 review comment ────────────────────────────────────────────

    #[test]
    fn review_comment_requires_well_formed_e_tag() {
        let ok = ev(
            KIND_REVIEW_COMMENT,
            &[&["e", &hex64(), "", "root"], &["p", &pk()]],
        );
        assert!(validate_workstream_event(&ok).is_ok());

        assert!(validate_workstream_event(&ev(KIND_REVIEW_COMMENT, &[])).is_err());

        let bad = ev(KIND_REVIEW_COMMENT, &[&["e", "nope"]]);
        assert!(validate_workstream_event(&bad).is_err());
    }

    // ── 47012 review decision ───────────────────────────────────────────

    #[test]
    fn review_decision_accepts_each_verdict_via_a_or_e() {
        for verdict in REVIEW_DECISIONS {
            let via_a = ev(
                KIND_REVIEW_DECISION,
                &[&["a", &artifact_coord()], &["decision", verdict]],
            );
            assert!(validate_workstream_event(&via_a).is_ok(), "{verdict} via a");

            let via_e = ev(
                KIND_REVIEW_DECISION,
                &[&["e", &hex64()], &["decision", verdict]],
            );
            assert!(validate_workstream_event(&via_e).is_ok(), "{verdict} via e");
        }
    }

    #[test]
    fn review_decision_rejects_missing_target() {
        let e = ev(KIND_REVIEW_DECISION, &[&["decision", "approve"]]);
        assert!(validate_workstream_event(&e)
            .unwrap_err()
            .contains("`a` or `e`"));
    }

    #[test]
    fn review_decision_rejects_missing_or_unknown_decision() {
        let missing = ev(KIND_REVIEW_DECISION, &[&["e", &hex64()]]);
        assert!(validate_workstream_event(&missing)
            .unwrap_err()
            .contains("requires a `decision` tag"));

        let unknown = ev(
            KIND_REVIEW_DECISION,
            &[&["e", &hex64()], &["decision", "lgtm"]],
        );
        assert!(validate_workstream_event(&unknown)
            .unwrap_err()
            .starts_with("unknown decision"));

        let dupe = ev(
            KIND_REVIEW_DECISION,
            &[
                &["e", &hex64()],
                &["decision", "approve"],
                &["decision", "reject"],
            ],
        );
        assert!(validate_workstream_event(&dupe)
            .unwrap_err()
            .contains("exactly one `decision` tag"));
    }

    // ── 47020 / 47021 ───────────────────────────────────────────────────

    #[test]
    fn experiment_log_and_measurement_require_workstream_coordinate() {
        for kind in [KIND_EXPERIMENT_LOG, KIND_MEASUREMENT] {
            let ok = ev(kind, &[&["a", &ws_coord()], &["unit", "mm"]]);
            assert!(validate_workstream_event(&ok).is_ok());

            let wrong = ev(kind, &[&["a", &task_coord()]]);
            assert!(validate_workstream_event(&wrong).is_err());

            let missing = ev(kind, &[&["unit", "mm"]]);
            assert!(validate_workstream_event(&missing).is_err());
        }
    }

    // ── 47030 handoff ───────────────────────────────────────────────────

    #[test]
    fn handoff_requires_at_least_one_p_tag() {
        let ok = ev(KIND_HANDOFF, &[&["p", &pk()], &["p", &pk()]]);
        assert!(validate_workstream_event(&ok).is_ok());

        let missing = ev(KIND_HANDOFF, &[&["a", &ws_coord()]]);
        assert!(validate_workstream_event(&missing)
            .unwrap_err()
            .contains("at least one `p` tag"));

        let bad = ev(KIND_HANDOFF, &[&["p", "not-a-pubkey"]]);
        assert!(validate_workstream_event(&bad).is_err());
    }
}

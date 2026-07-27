//! End-to-end integration tests for the Hive Workstream kind family
//! (`docs/hive-implementation-plan.md` §5.1–5.2).
//!
//! Two blocks of kinds:
//!
//! * **35000–35003** — NIP-33 addressable heads (workstream, task, artifact,
//!   decision record), replaced last-write-wins by the relay's *existing*
//!   parameterized-replace machinery.
//! * **47001–47030** — append-only history (status changes, artifact
//!   versions, review request/comment/decision, experiment logs,
//!   measurements, handoffs).
//!
//! What these tests pin down:
//!
//! 1. The full lifecycle survives a round trip over WebSocket — publish every
//!    kind in the family into one channel, then read them all back with plain
//!    NIP-01 REQ filters (`kinds` + `#h`). No Workstream-specific endpoint is
//!    involved anywhere: the model rides the generic event pipeline.
//! 2. NIP-33 LWW works on 35001 without any Workstream-specific code: a newer
//!    write replaces the head, and a *stale* write is reported back as a
//!    conflict rather than silently overwriting the newer head.
//! 3. Every ingest validation rule class rejects with `OK false` and an
//!    `invalid: …` reason — never a dropped connection or a 500.
//!
//! These tests require a running relay instance. By default they are marked
//! `#[ignore]` so that `cargo test` does not fail in CI when the relay is not
//! available.
//!
//! # Running
//!
//! Start the relay, then run:
//!
//! ```text
//! cargo test -p buzz-test-client --test e2e_workstream -- --ignored
//! ```
//!
//! Override the relay URL with the `RELAY_URL` environment variable:
//!
//! ```text
//! RELAY_URL=ws://relay.example.com cargo test --test e2e_workstream -- --ignored
//! ```

use std::time::Duration;

use buzz_test_client::BuzzTestClient;
use nostr::{Alphabet, EventBuilder, Filter, Keys, Kind, SingleLetterTag, Tag, Timestamp};

// Addressable heads (NIP-33, d-tag identified).
const KIND_WORKSTREAM: u16 = 35000;
const KIND_WORKSTREAM_TASK: u16 = 35001;
const KIND_ARTIFACT: u16 = 35002;
const KIND_DECISION_RECORD: u16 = 35003;
// Append-only history.
const KIND_TASK_STATUS_CHANGE: u16 = 47001;
const KIND_ARTIFACT_VERSION: u16 = 47002;
const KIND_REVIEW_REQUEST: u16 = 47010;
const KIND_REVIEW_COMMENT: u16 = 47011;
const KIND_REVIEW_DECISION: u16 = 47012;
const KIND_EXPERIMENT_LOG: u16 = 47020;
const KIND_MEASUREMENT: u16 = 47021;
const KIND_HANDOFF: u16 = 47030;

const HEAD_KINDS: [u16; 4] = [
    KIND_WORKSTREAM,
    KIND_WORKSTREAM_TASK,
    KIND_ARTIFACT,
    KIND_DECISION_RECORD,
];

const HISTORY_KINDS: [u16; 8] = [
    KIND_TASK_STATUS_CHANGE,
    KIND_ARTIFACT_VERSION,
    KIND_REVIEW_REQUEST,
    KIND_REVIEW_COMMENT,
    KIND_REVIEW_DECISION,
    KIND_EXPERIMENT_LOG,
    KIND_MEASUREMENT,
    KIND_HANDOFF,
];

fn relay_url() -> String {
    std::env::var("RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".to_string())
}

fn relay_http_url() -> String {
    relay_url()
        .replace("wss://", "https://")
        .replace("ws://", "http://")
        .trim_end_matches('/')
        .to_string()
}

fn sub_id(name: &str) -> String {
    format!("e2e-ws-{name}-{}", uuid::Uuid::new_v4())
}

/// A short unique suffix, so re-runs never collide on `d` tags.
fn nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Create a real channel in the DB via REST so the relay accepts `h`-scoped
/// events for it. Mirrors the helper in `e2e_nostr_interop.rs`.
async fn create_test_channel(keys: &Keys) -> String {
    let client = reqwest::Client::new();
    let pubkey_hex = keys.public_key().to_hex();
    let channel_uuid = uuid::Uuid::new_v4();
    let channel_name = format!("workstream-e2e-{channel_uuid}");

    let event = EventBuilder::new(Kind::Custom(9007), "")
        .tags(vec![
            Tag::parse(["h", &channel_uuid.to_string()]).unwrap(),
            Tag::parse(["name", &channel_name]).unwrap(),
            Tag::parse(["channel_type", "stream"]).unwrap(),
            Tag::parse(["visibility", "open"]).unwrap(),
        ])
        .sign_with_keys(keys)
        .unwrap();

    let resp = client
        .post(format!("{}/events", relay_http_url()))
        .header("X-Pubkey", &pubkey_hex)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&event).unwrap())
        .send()
        .await
        .expect("submit create-channel event");
    assert!(
        resp.status().is_success(),
        "channel creation event failed: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("parse event response");
    assert!(
        body["accepted"].as_bool().unwrap_or(false),
        "channel creation not accepted: {body}"
    );

    channel_uuid.to_string()
}

/// Build a signed Workstream event: `kind`, an `h` tag for `channel`, plus
/// `tags` given as `[name, value…]` slices.
fn build(keys: &Keys, kind: u16, channel: &str, tags: &[&[&str]], content: &str) -> nostr::Event {
    build_at(keys, kind, channel, tags, content, Timestamp::now())
}

/// Same as [`build`] but with an explicit `created_at` — the LWW tests need
/// to order two writes to the same coordinate deterministically.
fn build_at(
    keys: &Keys,
    kind: u16,
    channel: &str,
    tags: &[&[&str]],
    content: &str,
    created_at: Timestamp,
) -> nostr::Event {
    let mut all: Vec<Tag> = vec![Tag::parse(["h", channel]).expect("h tag")];
    all.extend(
        tags.iter()
            .map(|t| Tag::parse(t.iter().copied()).expect("tag parses")),
    );
    EventBuilder::new(Kind::Custom(kind), content)
        .tags(all)
        .custom_created_at(created_at)
        .sign_with_keys(keys)
        .expect("sign event")
}

/// Build an event *without* an `h` tag — used by the h-tag rejection test.
fn build_unscoped(keys: &Keys, kind: u16, tags: &[&[&str]]) -> nostr::Event {
    EventBuilder::new(Kind::Custom(kind), "")
        .tags(
            tags.iter()
                .map(|t| Tag::parse(t.iter().copied()).expect("tag parses"))
                .collect::<Vec<_>>(),
        )
        .sign_with_keys(keys)
        .expect("sign event")
}

/// Publish an event and assert the relay accepted it, returning its id hex.
async fn publish_ok(client: &mut BuzzTestClient, event: nostr::Event) -> String {
    let id = event.id.to_hex();
    let kind = event.kind.as_u16();
    let ok = client.send_event(event).await.expect("send event");
    assert!(ok.accepted, "relay rejected kind:{kind}: {}", ok.message);
    assert!(
        !ok.message.starts_with("duplicate:"),
        "kind:{kind} unexpectedly reported a write conflict: {}",
        ok.message
    );
    id
}

/// Publish an event expected to be rejected; assert `OK false` with an
/// `invalid:` reason and return the reason for further assertions.
async fn publish_rejected(client: &mut BuzzTestClient, event: nostr::Event) -> String {
    let kind = event.kind.as_u16();
    let ok = client.send_event(event).await.expect("send event");
    assert!(
        !ok.accepted,
        "relay accepted a malformed kind:{kind} event: {}",
        ok.message
    );
    assert!(
        ok.message.starts_with("invalid:"),
        "kind:{kind} rejection should be machine-readable `invalid: …`, got: {}",
        ok.message
    );
    ok.message
}

/// NIP-01 addressable coordinate for `(kind, pubkey, d)`.
fn coord(kind: u16, keys: &Keys, d: &str) -> String {
    format!("{kind}:{}:{d}", keys.public_key().to_hex())
}

/// Read events back with a plain NIP-01 REQ scoped by `kinds` + `#h`.
///
/// `kinds` is always populated: an open-ended filter trips the relay's
/// p-gate, exactly as it does for any other kind.
async fn query_by_kinds(
    client: &mut BuzzTestClient,
    label: &str,
    channel: &str,
    kinds: &[u16],
) -> Vec<nostr::Event> {
    let sid = sub_id(label);
    let filter = Filter::new()
        .kinds(kinds.iter().map(|k| Kind::Custom(*k)).collect::<Vec<_>>())
        .custom_tags(SingleLetterTag::lowercase(Alphabet::H), [channel])
        .limit(100);
    client
        .subscribe(&sid, vec![filter])
        .await
        .expect("subscribe");
    let events = client
        .collect_until_eose(&sid, Duration::from_secs(10))
        .await
        .expect("collect until EOSE");
    client
        .close_subscription(&sid)
        .await
        .expect("close subscription");
    events
}

/// First value of the `name` tag, if any.
fn tag_value<'a>(event: &'a nostr::Event, name: &str) -> Option<&'a str> {
    event.tags.iter().find_map(|t| {
        let parts = t.as_slice();
        if parts.len() >= 2 && parts[0].as_str() == name {
            Some(parts[1].as_str())
        } else {
            None
        }
    })
}

/// The whole Workstream model, published and read back over one WebSocket
/// connection: workstream → task (+ LWW edit + stale conflict) → status
/// change → artifact + version → review request/comment/decision → decision
/// record with `supersedes` → experiment log → measurement → handoff, then
/// REQ every kind back with `kinds` + `#h` filters.
#[tokio::test]
#[ignore]
async fn test_workstream_full_lifecycle() {
    let url = relay_url();
    let keys = Keys::generate();
    let peer = Keys::generate();
    let channel = create_test_channel(&keys).await;
    let mut client = BuzzTestClient::connect(&url, &keys).await.expect("connect");

    let n = nonce();
    let ws_d = format!("ws-{n}");
    let task_d = format!("task-{n}");
    let artifact_d = format!("art-{n}");
    let dec_a_d = format!("dec-a-{n}");
    let dec_b_d = format!("dec-b-{n}");
    let ws_coord = coord(KIND_WORKSTREAM, &keys, &ws_d);
    let task_coord = coord(KIND_WORKSTREAM_TASK, &keys, &task_d);
    let artifact_coord = coord(KIND_ARTIFACT, &keys, &artifact_d);
    let content_hash = "b".repeat(64);

    // ── 35000: the workstream container ─────────────────────────────────
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_WORKSTREAM,
            &channel,
            &[
                &["d", &ws_d],
                &["ws-type", "hardware"],
                &["status", "active"],
                &["p", &peer.public_key().to_hex()],
            ],
            "Thermal regulator v2",
        ),
    )
    .await;

    // ── 35001: a task, then a newer LWW edit of the same coordinate ─────
    let base_ts = Timestamp::now();
    publish_ok(
        &mut client,
        build_at(
            &keys,
            KIND_WORKSTREAM_TASK,
            &channel,
            &[
                &["d", &task_d],
                &["a", &ws_coord],
                &["status", "todo"],
                &["assignee", &peer.public_key().to_hex()],
            ],
            "Characterize the heat sink",
            base_ts,
        ),
    )
    .await;

    let newer = build_at(
        &keys,
        KIND_WORKSTREAM_TASK,
        &channel,
        &[
            &["d", &task_d],
            &["a", &ws_coord],
            &["status", "in-progress"],
        ],
        "Characterize the heat sink (in progress)",
        Timestamp::from(base_ts.as_secs() + 30),
    );
    publish_ok(&mut client, newer).await;

    // A *stale* write to the same coordinate must not clobber the newer
    // head. The relay reports this back through the existing NIP-33
    // convention — `OK true` with a `duplicate:` message, which the CLI maps
    // to exit code 5 (write conflict).
    let stale = build_at(
        &keys,
        KIND_WORKSTREAM_TASK,
        &channel,
        &[&["d", &task_d], &["a", &ws_coord], &["status", "todo"]],
        "Stale editor overwrites nothing",
        Timestamp::from(base_ts.as_secs() - 30),
    );
    let stale_ok = client.send_event(stale).await.expect("send stale task");
    assert!(
        stale_ok.message.starts_with("duplicate:"),
        "stale NIP-33 write should report a conflict, got accepted={} message={:?}",
        stale_ok.accepted,
        stale_ok.message
    );

    // ── 47001: append-only status history ───────────────────────────────
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_TASK_STATUS_CHANGE,
            &channel,
            &[
                &["a", &task_coord],
                &["status", "in-progress"],
                &["previous-status", "todo"],
            ],
            "",
        ),
    )
    .await;

    // ── 35002 + 47002: artifact head and an immutable version ───────────
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_ARTIFACT,
            &channel,
            &[
                &["d", &artifact_d],
                &["artifact-type", "measurement"],
                &["x", &content_hash],
            ],
            "Heat sink sweep dataset",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_ARTIFACT_VERSION,
            &channel,
            &[
                &["a", &artifact_coord],
                &["x", &content_hash],
                &["version", "1"],
            ],
            "initial capture",
        ),
    )
    .await;

    // ── 47010 / 47011 / 47012: generic (non-git) review round ───────────
    let review_id = publish_ok(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_REQUEST,
            &channel,
            &[&["a", &artifact_coord], &["p", &peer.public_key().to_hex()]],
            "Please sanity-check the sweep",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_COMMENT,
            &channel,
            &[
                &["e", &review_id, "", "root"],
                &["p", &peer.public_key().to_hex()],
            ],
            "Point 7 looks off by a decade",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_DECISION,
            &channel,
            &[&["e", &review_id], &["decision", "request-changes"]],
            "Rerun with the corrected probe",
        ),
    )
    .await;

    // ── 35003: a decision record, then one that supersedes it ───────────
    let first_decision = publish_ok(
        &mut client,
        build(
            &keys,
            KIND_DECISION_RECORD,
            &channel,
            &[&["d", &dec_a_d], &["a", &ws_coord]],
            "ADR-1: aluminium fins",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_DECISION_RECORD,
            &channel,
            &[
                &["d", &dec_b_d],
                &["a", &ws_coord],
                &["supersedes", &first_decision],
            ],
            "ADR-2: copper fins",
        ),
    )
    .await;

    // ── 47020 / 47021 / 47030 ───────────────────────────────────────────
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_EXPERIMENT_LOG,
            &channel,
            &[&["a", &ws_coord], &["run", "7"]],
            "Run 7: ambient 22C, load 40W",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_MEASUREMENT,
            &channel,
            &[
                &["a", &ws_coord],
                &["unit", "celsius"],
                &["value", "61.4"],
                &["series", "junction-temp"],
            ],
            "",
        ),
    )
    .await;
    publish_ok(
        &mut client,
        build(
            &keys,
            KIND_HANDOFF,
            &channel,
            &[
                &["p", &peer.public_key().to_hex()],
                &["p", &keys.public_key().to_hex()],
            ],
            "Handing the thermal model to manufacturing",
        ),
    )
    .await;

    // Small delay so the writes are visible to the read path.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Read back: addressable heads ────────────────────────────────────
    let heads = query_by_kinds(&mut client, "heads", &channel, &HEAD_KINDS).await;
    for kind in HEAD_KINDS {
        assert!(
            heads.iter().any(|e| e.kind.as_u16() == kind),
            "kind:{kind} head missing from REQ result ({} events)",
            heads.len()
        );
    }
    // Exactly one live head per coordinate — the LWW edit replaced, it did
    // not append.
    let tasks: Vec<_> = heads
        .iter()
        .filter(|e| e.kind.as_u16() == KIND_WORKSTREAM_TASK && tag_value(e, "d") == Some(&task_d))
        .collect();
    assert_eq!(
        tasks.len(),
        1,
        "NIP-33 replace should leave exactly one task head, got {}",
        tasks.len()
    );
    assert_eq!(
        tag_value(tasks[0], "status"),
        Some("in-progress"),
        "the newest write must win; stale write must not resurrect `todo`"
    );
    // Both decision records survive — they are distinct `d` coordinates, and
    // `supersedes` is a client-side chain, not a relay delete.
    let decisions: Vec<_> = heads
        .iter()
        .filter(|e| e.kind.as_u16() == KIND_DECISION_RECORD)
        .collect();
    assert_eq!(decisions.len(), 2, "both decision records should be live");
    assert!(
        decisions
            .iter()
            .any(|e| tag_value(e, "supersedes") == Some(first_decision.as_str())),
        "the superseding ADR should carry the supersedes chain"
    );

    // ── Read back: append-only history ──────────────────────────────────
    let history = query_by_kinds(&mut client, "history", &channel, &HISTORY_KINDS).await;
    for kind in HISTORY_KINDS {
        assert!(
            history.iter().any(|e| e.kind.as_u16() == kind),
            "kind:{kind} history event missing from REQ result ({} events)",
            history.len()
        );
    }

    client.disconnect().await.ok();
}

/// Every validation rule class, one rejection each. All must come back as
/// `OK false` with an `invalid: …` reason — the relay never panics or 500s on
/// a malformed Workstream event.
#[tokio::test]
#[ignore]
async fn test_workstream_validation_rejections() {
    let url = relay_url();
    let keys = Keys::generate();
    let channel = create_test_channel(&keys).await;
    let mut client = BuzzTestClient::connect(&url, &keys).await.expect("connect");

    let n = nonce();
    let ws_coord = coord(KIND_WORKSTREAM, &keys, &format!("ws-{n}"));
    let task_coord = coord(KIND_WORKSTREAM_TASK, &keys, &format!("task-{n}"));
    let artifact_coord = coord(KIND_ARTIFACT, &keys, &format!("art-{n}"));
    let hex64 = "c".repeat(64);

    // Rule class: addressable head requires a `d` tag.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_WORKSTREAM,
            &channel,
            &[&["ws-type", "code"]],
            "no d tag",
        ),
    )
    .await;

    // Rule class: `ws-type` vocabulary.
    let reason = publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_WORKSTREAM,
            &channel,
            &[&["d", &format!("ws-bad-{n}")], &["ws-type", "telepathy"]],
            "",
        ),
    )
    .await;
    assert!(
        reason.contains("unknown ws-type"),
        "expected an unknown-ws-type reason, got: {reason}"
    );

    // Rule class: required `a` coordinate (missing).
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_WORKSTREAM_TASK,
            &channel,
            &[&["d", &format!("task-orphan-{n}")]],
            "",
        ),
    )
    .await;

    // Rule class: `a` coordinate must be well-formed.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_WORKSTREAM_TASK,
            &channel,
            &[&["d", &format!("task-bad-{n}")], &["a", "35000:nothex:x"]],
            "",
        ),
    )
    .await;

    // Rule class: `a` coordinate must point at the right kind.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_TASK_STATUS_CHANGE,
            &channel,
            &[&["a", &ws_coord], &["status", "done"]],
            "",
        ),
    )
    .await;
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_ARTIFACT_VERSION,
            &channel,
            &[&["a", &task_coord]],
            "",
        ),
    )
    .await;
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_REQUEST,
            &channel,
            &[&["a", &ws_coord]],
            "",
        ),
    )
    .await;
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_EXPERIMENT_LOG,
            &channel,
            &[&["a", &task_coord]],
            "",
        ),
    )
    .await;
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_MEASUREMENT,
            &channel,
            &[&["a", &artifact_coord]],
            "",
        ),
    )
    .await;

    // Rule class: sha-256 `x` tag format.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_ARTIFACT,
            &channel,
            &[&["d", &format!("art-bad-{n}")], &["x", "deadbeef"]],
            "",
        ),
    )
    .await;

    // Rule class: `supersedes` event-id format.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_DECISION_RECORD,
            &channel,
            &[
                &["d", &format!("dec-bad-{n}")],
                &["a", &ws_coord],
                &["supersedes", "not-an-event-id"],
            ],
            "",
        ),
    )
    .await;

    // Rule class: NIP-10 anchor required on review comments.
    publish_rejected(
        &mut client,
        build(&keys, KIND_REVIEW_COMMENT, &channel, &[], "orphan comment"),
    )
    .await;

    // Rule class: review decision needs a target and a known verdict.
    publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_DECISION,
            &channel,
            &[&["decision", "approve"]],
            "",
        ),
    )
    .await;
    let reason = publish_rejected(
        &mut client,
        build(
            &keys,
            KIND_REVIEW_DECISION,
            &channel,
            &[&["e", &hex64], &["decision", "lgtm"]],
            "",
        ),
    )
    .await;
    assert!(
        reason.contains("unknown decision"),
        "expected an unknown-decision reason, got: {reason}"
    );

    // Rule class: handoff needs at least one `p` tag.
    publish_rejected(
        &mut client,
        build(&keys, KIND_HANDOFF, &channel, &[&["a", &ws_coord]], ""),
    )
    .await;

    client.disconnect().await.ok();
}

/// Every Workstream kind is channel-scoped: without an `h` tag the relay
/// rejects it with the same message every other NIP-29-scoped kind uses.
#[tokio::test]
#[ignore]
async fn test_workstream_kinds_require_h_tag() {
    let url = relay_url();
    let keys = Keys::generate();
    let mut client = BuzzTestClient::connect(&url, &keys).await.expect("connect");

    let n = nonce();
    let ws_coord = coord(KIND_WORKSTREAM, &keys, &format!("ws-{n}"));
    let task_coord = coord(KIND_WORKSTREAM_TASK, &keys, &format!("task-{n}"));
    let artifact_coord = coord(KIND_ARTIFACT, &keys, &format!("art-{n}"));
    let hex64 = "d".repeat(64);
    let peer_hex = Keys::generate().public_key().to_hex();
    let ws_d = format!("ws-{n}");
    let task_d = format!("task-{n}");
    let art_d = format!("art-{n}");
    let dec_d = format!("dec-{n}");

    // Owned rows: `build_unscoped` borrows the slices, so the tag vectors
    // must outlive the loop body.
    let cases: Vec<(u16, Vec<Vec<&str>>)> = vec![
        (KIND_WORKSTREAM, vec![vec!["d", &ws_d]]),
        (
            KIND_WORKSTREAM_TASK,
            vec![vec!["d", &task_d], vec!["a", &ws_coord]],
        ),
        (KIND_ARTIFACT, vec![vec!["d", &art_d]]),
        (
            KIND_DECISION_RECORD,
            vec![vec!["d", &dec_d], vec!["a", &ws_coord]],
        ),
        (KIND_TASK_STATUS_CHANGE, vec![vec!["a", &task_coord]]),
        (KIND_ARTIFACT_VERSION, vec![vec!["a", &artifact_coord]]),
        (KIND_REVIEW_REQUEST, vec![vec!["a", &artifact_coord]]),
        (KIND_REVIEW_COMMENT, vec![vec!["e", &hex64]]),
        (
            KIND_REVIEW_DECISION,
            vec![vec!["e", &hex64], vec!["decision", "approve"]],
        ),
        (KIND_EXPERIMENT_LOG, vec![vec!["a", &ws_coord]]),
        (KIND_MEASUREMENT, vec![vec!["a", &ws_coord]]),
        (KIND_HANDOFF, vec![vec!["p", &peer_hex]]),
    ];

    for (kind, tags) in &cases {
        let tag_refs: Vec<&[&str]> = tags.iter().map(|t| t.as_slice()).collect();
        let reason = publish_rejected(&mut client, build_unscoped(&keys, *kind, &tag_refs)).await;
        assert!(
            reason.contains("h tag"),
            "kind:{kind} without an h tag should be rejected for the h tag, got: {reason}"
        );
    }

    client.disconnect().await.ok();
}

//! Multi-agent Workstream review cycle
//! (`docs/hive-implementation-plan.md` §8 Phase 4).
//!
//! `e2e_workstream.rs` proves the kinds round-trip for a *single* author. This
//! suite proves the part that matters for a team: three separate identities —
//! a human, a spec-writer agent, and a critic agent — drive one `hardware`
//! workstream from task to approved artifact, and each party **observes** the
//! other's writes over its own WebSocket connection.
//!
//! The distinction is the point. `OK true` from the relay only says a write
//! was accepted; it says nothing about whether the next actor can see it.
//! Every step here re-queries from the *other* party's connection with a plain
//! NIP-01 REQ (`kinds` + `#h` + `#a`) and asserts the event is there. That is
//! the contract the `hive-workstream` persona pack
//! (`crates/buzz-persona/packs/hive-workstream/`) assumes when it tells an
//! agent to "wait for the verdict" — the personas' protocol is what is under
//! test, not any LLM.
//!
//! No agent runtime is involved: the three keypairs stand in for the three
//! participants, and the event sequence is exactly what the personas'
//! instructions prescribe.
//!
//! The cycle:
//!
//! 1. Human opens a `hardware` workstream (35000) and assigns a task (35001).
//! 2. Spec-writer publishes an artifact (35002), version 1 (47002), and a
//!    review request (47010) naming the critic.
//! 3. Critic comments in thread (47011), then records `request-changes`
//!    (47012).
//! 4. Spec-writer publishes version 2 (47002), bumps the head, and opens a
//!    *new* request — a decided request is never reused.
//! 5. Critic approves (47012).
//! 6. Human records the decision (35003) and completes the task (47001 plus a
//!    head bump).
//! 7. Spec-writer hands the workstream back to the human (47030).
//!
//! These tests require a running relay instance. By default they are marked
//! `#[ignore]` so that `cargo test` does not fail in CI when the relay is not
//! available.
//!
//! # Running
//!
//! ```text
//! cargo test -p buzz-test-client --test e2e_workstream_review -- --ignored
//! ```
//!
//! Override the relay URL with the `RELAY_URL` environment variable:
//!
//! ```text
//! RELAY_URL=ws://relay.example.com cargo test --test e2e_workstream_review -- --ignored
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
const KIND_HANDOFF: u16 = 47030;

/// How long an observer keeps re-querying before it declares an event
/// invisible. Each attempt is a fresh REQ, so this tolerates replication lag
/// without ever passing on a write the other party genuinely cannot see.
const OBSERVE_ATTEMPTS: usize = 12;
const OBSERVE_INTERVAL: Duration = Duration::from_millis(250);

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
    format!("e2e-wsr-{name}-{}", uuid::Uuid::new_v4())
}

/// A short unique suffix, so re-runs never collide on `d` tags.
fn nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// A stand-in content hash — the relay only checks that `x` is 64 lowercase
/// hex, and the payloads here are notional.
fn fake_hash(seed: char) -> String {
    seed.to_string().repeat(64)
}

/// Create a real channel in the DB via REST so the relay accepts `h`-scoped
/// events for it. Mirrors the helper in `e2e_workstream.rs`.
async fn create_test_channel(keys: &Keys) -> String {
    let client = reqwest::Client::new();
    let pubkey_hex = keys.public_key().to_hex();
    let channel_uuid = uuid::Uuid::new_v4();
    let channel_name = format!("workstream-review-e2e-{channel_uuid}");

    let event = EventBuilder::new(Kind::Custom(9007), "")
        .tags(vec![
            Tag::parse(["h", &channel_uuid.to_string()]).expect("h tag"),
            Tag::parse(["name", &channel_name]).expect("name tag"),
            Tag::parse(["channel_type", "stream"]).expect("channel_type tag"),
            Tag::parse(["visibility", "open"]).expect("visibility tag"),
        ])
        .sign_with_keys(keys)
        .expect("sign channel event");

    let resp = client
        .post(format!("{}/events", relay_http_url()))
        .header("X-Pubkey", &pubkey_hex)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&event).expect("serialize event"))
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

/// Same as [`build`] but with an explicit `created_at` — head replacements
/// need a deterministic NIP-33 ordering.
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

/// NIP-01 addressable coordinate for `(kind, pubkey, d)`.
fn coord(kind: u16, keys: &Keys, d: &str) -> String {
    format!("{kind}:{}:{d}", keys.public_key().to_hex())
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

/// Every value of the `name` tag.
fn tag_values<'a>(event: &'a nostr::Event, name: &str) -> Vec<&'a str> {
    event
        .tags
        .iter()
        .filter_map(|t| {
            let parts = t.as_slice();
            if parts.len() >= 2 && parts[0].as_str() == name {
                Some(parts[1].as_str())
            } else {
                None
            }
        })
        .collect()
}

/// Run one REQ scoped by `kinds` + `#h`, optionally narrowed to an `#a`
/// coordinate, and drain it to EOSE.
///
/// `kinds` is always populated: an open-ended filter trips the relay's p-gate,
/// exactly as it does for any other kind.
async fn query_scoped(
    client: &mut BuzzTestClient,
    label: &str,
    channel: &str,
    kinds: &[u16],
    a_coord: Option<&str>,
) -> Vec<nostr::Event> {
    let sid = sub_id(label);
    let mut filter = Filter::new()
        .kinds(kinds.iter().map(|k| Kind::Custom(*k)).collect::<Vec<_>>())
        .custom_tags(SingleLetterTag::lowercase(Alphabet::H), [channel])
        .limit(100);
    if let Some(a) = a_coord {
        filter = filter.custom_tags(SingleLetterTag::lowercase(Alphabet::A), [a]);
    }
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

/// Assert that `observer` — a *different* identity from the author — can read
/// `event_id` back with a plain REQ, and return everything that filter saw.
///
/// This is the assertion the whole suite exists for. A relay that accepts a
/// write but never serves it to the counterparty passes every `OK true` check
/// and still breaks the review cycle.
async fn assert_observes(
    observer: &mut BuzzTestClient,
    label: &str,
    channel: &str,
    kinds: &[u16],
    a_coord: Option<&str>,
    event_id: &str,
) -> Vec<nostr::Event> {
    let mut last: Vec<nostr::Event> = Vec::new();
    for attempt in 0..OBSERVE_ATTEMPTS {
        last = query_scoped(observer, label, channel, kinds, a_coord).await;
        if last.iter().any(|e| e.id.to_hex() == event_id) {
            return last;
        }
        if attempt + 1 < OBSERVE_ATTEMPTS {
            tokio::time::sleep(OBSERVE_INTERVAL).await;
        }
    }
    panic!(
        "{label}: counterparty never observed event {event_id} \
         (kinds={kinds:?} a={a_coord:?}); saw {} event(s): {:?}",
        last.len(),
        last.iter()
            .map(|e| (e.kind.as_u16(), e.id.to_hex()))
            .collect::<Vec<_>>()
    );
}

/// Find one event by id in a result set.
fn find<'a>(events: &'a [nostr::Event], id: &str) -> &'a nostr::Event {
    events
        .iter()
        .find(|e| e.id.to_hex() == id)
        .unwrap_or_else(|| panic!("event {id} missing from result set"))
}

/// A human, a spec-writer, and a critic complete a review cycle in a
/// `hardware` workstream — with every step observed by the party whose turn
/// comes next.
#[tokio::test]
#[ignore]
async fn test_multi_agent_workstream_review_cycle() {
    let url = relay_url();

    // Three identities: the human owns the workstream, the two agents do the
    // work. No agent runtime — the personas' protocol is the thing under test.
    let human = Keys::generate();
    let spec_writer = Keys::generate();
    let critic = Keys::generate();

    let human_pk = human.public_key().to_hex();
    let spec_pk = spec_writer.public_key().to_hex();
    let critic_pk = critic.public_key().to_hex();

    let channel = create_test_channel(&human).await;

    let mut human_c = BuzzTestClient::connect(&url, &human)
        .await
        .expect("human connect");
    let mut spec_c = BuzzTestClient::connect(&url, &spec_writer)
        .await
        .expect("spec-writer connect");
    let mut critic_c = BuzzTestClient::connect(&url, &critic)
        .await
        .expect("critic connect");

    let n = nonce();
    let ws_d = format!("chamber-{n}");
    let task_d = format!("cal-{n}");
    let art_d = format!("cal-sheet-{n}");
    let dec_d = format!("adr-{n}");

    let ws_coord = coord(KIND_WORKSTREAM, &human, &ws_d);
    let task_coord = coord(KIND_WORKSTREAM_TASK, &human, &task_d);
    // The artifact belongs to the spec-writer, so its coordinate names *their*
    // pubkey — reviews and versions must address it there, not under the
    // workstream owner.
    let art_coord = coord(KIND_ARTIFACT, &spec_writer, &art_d);

    let hash_v1 = fake_hash('a');
    let hash_v2 = fake_hash('b');

    // ── 1. Human opens the workstream and assigns the task ──────────────
    let ws_id = publish_ok(
        &mut human_c,
        build(
            &human,
            KIND_WORKSTREAM,
            &channel,
            &[
                &["d", &ws_d],
                &["ws-type", "hardware"],
                &["status", "active"],
                &["name", "Thermal chamber v2"],
                &["p", &spec_pk],
                &["p", &critic_pk],
            ],
            "Bring-up of the second thermal chamber.",
        ),
    )
    .await;

    let task_base = Timestamp::now();
    let task_id = publish_ok(
        &mut human_c,
        build_at(
            &human,
            KIND_WORKSTREAM_TASK,
            &channel,
            &[
                &["d", &task_d],
                &["a", &ws_coord],
                &["status", "todo"],
                &["name", "Write the calibration sheet"],
                &["assignee", &spec_pk],
                &["p", &spec_pk],
            ],
            "Characterize the chamber and write it up.",
            task_base,
        ),
    )
    .await;

    // The spec-writer must see both before it can act on either.
    let seen_ws = assert_observes(
        &mut spec_c,
        "spec-sees-workstream",
        &channel,
        &[KIND_WORKSTREAM],
        None,
        &ws_id,
    )
    .await;
    assert_eq!(
        tag_value(find(&seen_ws, &ws_id), "ws-type"),
        Some("hardware"),
        "the workstream the agents joined must be the hardware one"
    );

    let seen_task = assert_observes(
        &mut spec_c,
        "spec-sees-task",
        &channel,
        &[KIND_WORKSTREAM_TASK],
        Some(&ws_coord),
        &task_id,
    )
    .await;
    assert_eq!(
        tag_value(find(&seen_task, &task_id), "assignee"),
        Some(spec_pk.as_str()),
        "the task must name the spec-writer as assignee"
    );
    assert!(
        tag_values(find(&seen_task, &task_id), "p").contains(&spec_pk.as_str()),
        "the assignment must carry a p tag so the assignee's agent wakes"
    );

    // ── 2. Spec-writer publishes the artifact, v1, and a review request ──
    let art_base = Timestamp::now();
    publish_ok(
        &mut spec_c,
        build_at(
            &spec_writer,
            KIND_ARTIFACT,
            &channel,
            &[
                &["d", &art_d],
                &["a", &ws_coord],
                &["artifact-type", "doc"],
                &["name", "Chamber calibration sheet"],
                &["version", "1"],
                &["x", &hash_v1],
            ],
            "Calibration procedure and results.",
            art_base,
        ),
    )
    .await;

    let v1_id = publish_ok(
        &mut spec_c,
        build(
            &spec_writer,
            KIND_ARTIFACT_VERSION,
            &channel,
            &[&["a", &art_coord], &["x", &hash_v1], &["version", "1"]],
            "initial draft",
        ),
    )
    .await;

    let req1_id = publish_ok(
        &mut spec_c,
        build(
            &spec_writer,
            KIND_REVIEW_REQUEST,
            &channel,
            &[&["a", &art_coord], &["p", &critic_pk]],
            "v1 ready — the probe placement is the risk.",
        ),
    )
    .await;

    // The critic's turn starts here, so the critic must see the request.
    let seen_req1 = assert_observes(
        &mut critic_c,
        "critic-sees-request-1",
        &channel,
        &[KIND_REVIEW_REQUEST],
        Some(&art_coord),
        &req1_id,
    )
    .await;
    assert!(
        tag_values(find(&seen_req1, &req1_id), "p").contains(&critic_pk.as_str()),
        "the review request must p-tag the reviewer it is waiting on"
    );

    // …and the artifact version it is about.
    let seen_v1 = assert_observes(
        &mut critic_c,
        "critic-sees-v1",
        &channel,
        &[KIND_ARTIFACT_VERSION],
        Some(&art_coord),
        &v1_id,
    )
    .await;
    assert_eq!(tag_value(find(&seen_v1, &v1_id), "version"), Some("1"));

    // ── 3. Critic comments in thread, then requests changes ─────────────
    let comment_id = publish_ok(
        &mut critic_c,
        build(
            &critic,
            KIND_REVIEW_COMMENT,
            &channel,
            &[
                &["e", &req1_id, "", "root"],
                &["a", &art_coord],
                &["p", &spec_pk],
            ],
            "Probe P2 is on the wall, not the junction — the numbers are ambient.",
        ),
    )
    .await;

    let verdict1_id = publish_ok(
        &mut critic_c,
        build(
            &critic,
            KIND_REVIEW_DECISION,
            &channel,
            &[
                &["e", &req1_id],
                &["a", &art_coord],
                &["decision", "request-changes"],
                &["p", &spec_pk],
            ],
            "Blocking: probe placement invalidates the sweep. Re-run with P2 on the junction.",
        ),
    )
    .await;

    // The spec-writer must observe both halves of the round: the discussion
    // and the verdict that closes it.
    let seen_round1 = assert_observes(
        &mut spec_c,
        "spec-sees-comment",
        &channel,
        &[KIND_REVIEW_COMMENT, KIND_REVIEW_DECISION],
        Some(&art_coord),
        &comment_id,
    )
    .await;
    let comment = find(&seen_round1, &comment_id);
    assert_eq!(
        tag_value(comment, "e"),
        Some(req1_id.as_str()),
        "the comment must thread under the request it answers"
    );

    let seen_verdict1 = assert_observes(
        &mut spec_c,
        "spec-sees-request-changes",
        &channel,
        &[KIND_REVIEW_DECISION],
        Some(&art_coord),
        &verdict1_id,
    )
    .await;
    let verdict1 = find(&seen_verdict1, &verdict1_id);
    assert_eq!(tag_value(verdict1, "decision"), Some("request-changes"));
    assert!(
        !verdict1.content.is_empty(),
        "a request-changes verdict must carry a rationale"
    );

    // ── 4. Spec-writer ships v2, bumps the head, opens a NEW request ────
    let v2_id = publish_ok(
        &mut spec_c,
        build(
            &spec_writer,
            KIND_ARTIFACT_VERSION,
            &channel,
            &[&["a", &art_coord], &["x", &hash_v2], &["version", "2"]],
            "re-ran the sweep with P2 on the junction",
        ),
    )
    .await;

    // The head is replaced in place; the two versions both survive as history.
    let art_head2_id = publish_ok(
        &mut spec_c,
        build_at(
            &spec_writer,
            KIND_ARTIFACT,
            &channel,
            &[
                &["d", &art_d],
                &["a", &ws_coord],
                &["artifact-type", "doc"],
                &["name", "Chamber calibration sheet"],
                &["version", "2"],
                &["x", &hash_v2],
            ],
            "Calibration procedure and results (rev B).",
            Timestamp::from(art_base.as_secs() + 30),
        ),
    )
    .await;

    let req2_id = publish_ok(
        &mut spec_c,
        build(
            &spec_writer,
            KIND_REVIEW_REQUEST,
            &channel,
            &[&["a", &art_coord], &["p", &critic_pk]],
            "v2 — probe moved to the junction, sweep re-run.",
        ),
    )
    .await;

    let seen_versions = assert_observes(
        &mut critic_c,
        "critic-sees-v2",
        &channel,
        &[KIND_ARTIFACT_VERSION],
        Some(&art_coord),
        &v2_id,
    )
    .await;
    let mut labels: Vec<&str> = seen_versions
        .iter()
        .filter_map(|e| tag_value(e, "version"))
        .collect();
    labels.sort_unstable();
    assert_eq!(
        labels,
        ["1", "2"],
        "artifact versions are append-only — v1 must survive v2"
    );

    let seen_heads = assert_observes(
        &mut critic_c,
        "critic-sees-artifact-head",
        &channel,
        &[KIND_ARTIFACT],
        Some(&ws_coord),
        &art_head2_id,
    )
    .await;
    let art_heads: Vec<_> = seen_heads
        .iter()
        .filter(|e| tag_value(e, "d") == Some(art_d.as_str()))
        .collect();
    assert_eq!(
        art_heads.len(),
        1,
        "the artifact head is replaced, not appended — got {} live heads",
        art_heads.len()
    );
    assert_eq!(
        tag_value(art_heads[0], "x"),
        Some(hash_v2.as_str()),
        "the live head must point at the newest version's payload"
    );

    let seen_requests = assert_observes(
        &mut critic_c,
        "critic-sees-request-2",
        &channel,
        &[KIND_REVIEW_REQUEST],
        Some(&art_coord),
        &req2_id,
    )
    .await;
    assert_eq!(
        seen_requests.len(),
        2,
        "each revision opens a new request; a decided one is never reused"
    );

    // ── 5. Critic approves the second round ─────────────────────────────
    let verdict2_id = publish_ok(
        &mut critic_c,
        build(
            &critic,
            KIND_REVIEW_DECISION,
            &channel,
            &[
                &["e", &req2_id],
                &["a", &art_coord],
                &["decision", "approve"],
                &["p", &spec_pk],
            ],
            "Placement fixed and the sweep reproduces. Approved.",
        ),
    )
    .await;

    // The human — a third party to that exchange — must see the approval to
    // act on it.
    let seen_verdicts = assert_observes(
        &mut human_c,
        "human-sees-approval",
        &channel,
        &[KIND_REVIEW_DECISION],
        Some(&art_coord),
        &verdict2_id,
    )
    .await;
    assert_eq!(
        tag_value(find(&seen_verdicts, &verdict2_id), "decision"),
        Some("approve")
    );
    assert_eq!(
        seen_verdicts.len(),
        2,
        "both verdicts stay on the record — history is never rewritten"
    );

    // ── 6. Human records the decision and completes the task ────────────
    let decision_id = publish_ok(
        &mut human_c,
        build(
            &human,
            KIND_DECISION_RECORD,
            &channel,
            &[
                &["d", &dec_d],
                &["a", &ws_coord],
                &["status", "accepted"],
                &["name", "Junction-mounted probe is authoritative"],
            ],
            "## Context\nWall-mounted P2 read ambient.\n## Decision\nJunction mount.",
        ),
    )
    .await;

    let status_id = publish_ok(
        &mut human_c,
        build(
            &human,
            KIND_TASK_STATUS_CHANGE,
            &channel,
            &[
                &["a", &task_coord],
                &["status", "done"],
                &["previous-status", "todo"],
            ],
            "calibration sheet approved by review",
        ),
    )
    .await;

    let task_done_id = publish_ok(
        &mut human_c,
        build_at(
            &human,
            KIND_WORKSTREAM_TASK,
            &channel,
            &[
                &["d", &task_d],
                &["a", &ws_coord],
                &["status", "done"],
                &["name", "Write the calibration sheet"],
                &["assignee", &spec_pk],
                &["p", &spec_pk],
            ],
            "Characterize the chamber and write it up.",
            Timestamp::from(task_base.as_secs() + 120),
        ),
    )
    .await;

    let seen_decision = assert_observes(
        &mut spec_c,
        "spec-sees-decision-record",
        &channel,
        &[KIND_DECISION_RECORD],
        Some(&ws_coord),
        &decision_id,
    )
    .await;
    assert_eq!(
        tag_value(find(&seen_decision, &decision_id), "status"),
        Some("accepted")
    );

    let seen_status = assert_observes(
        &mut spec_c,
        "spec-sees-status-change",
        &channel,
        &[KIND_TASK_STATUS_CHANGE],
        Some(&task_coord),
        &status_id,
    )
    .await;
    let status = find(&seen_status, &status_id);
    assert_eq!(tag_value(status, "status"), Some("done"));
    assert_eq!(tag_value(status, "previous-status"), Some("todo"));

    let seen_task_heads = assert_observes(
        &mut spec_c,
        "spec-sees-task-done",
        &channel,
        &[KIND_WORKSTREAM_TASK],
        Some(&ws_coord),
        &task_done_id,
    )
    .await;
    let task_heads: Vec<_> = seen_task_heads
        .iter()
        .filter(|e| tag_value(e, "d") == Some(task_d.as_str()))
        .collect();
    assert_eq!(
        task_heads.len(),
        1,
        "the task head is replaced, not appended — got {} live heads",
        task_heads.len()
    );
    assert_eq!(
        tag_value(task_heads[0], "status"),
        Some("done"),
        "the head must reflect the completion the history recorded"
    );

    // ── 7. Spec-writer hands the workstream back to the human ───────────
    let handoff_id = publish_ok(
        &mut spec_c,
        build(
            &spec_writer,
            KIND_HANDOFF,
            &channel,
            &[
                &["a", &ws_coord],
                &["p", &spec_pk, "", "from"],
                &["p", &human_pk, "", "to"],
                &["checklist", "calibration sheet v2 approved"],
                &["checklist", "raw sweep logs uploaded"],
            ],
            "Chamber characterized and signed off. Back to you.",
        ),
    )
    .await;

    let seen_handoff = assert_observes(
        &mut human_c,
        "human-sees-handoff",
        &channel,
        &[KIND_HANDOFF],
        Some(&ws_coord),
        &handoff_id,
    )
    .await;
    let handoff = find(&seen_handoff, &handoff_id);
    let recipients: Vec<&str> = handoff
        .tags
        .iter()
        .filter_map(|t| {
            let parts = t.as_slice();
            match (parts.first(), parts.get(1), parts.get(3)) {
                (Some(k), Some(pk), Some(marker))
                    if k.as_str() == "p" && marker.as_str() == "to" =>
                {
                    Some(pk.as_str())
                }
                _ => None,
            }
        })
        .collect();
    assert_eq!(
        recipients,
        [human_pk.as_str()],
        "the handoff must name the human as the recipient of the baton"
    );

    human_c.disconnect().await.expect("disconnect human");
    spec_c.disconnect().await.expect("disconnect spec-writer");
    critic_c.disconnect().await.expect("disconnect critic");
}

# buzz-cli Live Testing Guide

Manual testing runbook for verifying every CLI command against a local relay.
An agent or developer follows this step by step, running each command and
checking the output.

---

## 1. Prerequisites

Docker services running and healthy:

```bash
docker compose ps
# buzz-postgres   healthy
# buzz-redis      healthy
```

If not running: `just setup` from the repo root.

Tools: `jq`, `curl`, Rust toolchain.

---

## 2. Build the CLI

```bash
cargo build -p buzz-cli
```

Use `cargo run -p buzz-cli --` or the built binary at `target/debug/buzz`.

---

## 3. Start the Relay

In a separate terminal:

```bash
cd REPOS/buzz-nostr
set -a && source .env && set +a
cargo run -p buzz-relay
```

Verify:

```bash
curl -s http://localhost:3000/_liveness
# "ok" or 200 status
```

The `.env` should have `BUZZ_REQUIRE_AUTH_TOKEN=false` for local dev.

---

## 4. Mint Test Credentials

### Option A: buzz-admin (full scopes including admin)

This mints a token with all CLI-relevant scopes (including `admin:channels`)
via direct DB access. Use this for testing admin operations (archive,
delete-channel, add/remove-channel-member).

```bash
DATABASE_URL="${DATABASE_URL:?set DATABASE_URL for the local Buzz database}" \
cargo run -p buzz-admin -- mint-token \
  --name "cli-test" \
  --scopes "messages:read,messages:write,channels:read,channels:write,users:read,users:write,files:read,files:write,admin:channels"
```

This generates a keypair and prints:
- **Private key (nsec)** — save for `BUZZ_PRIVATE_KEY` testing

Export:

```bash
export BUZZ_RELAY_URL="http://localhost:3000"
export BUZZ_PRIVATE_KEY="nsec1..."   # from the mint output
```

### Scope reference

| Scope | Self-mintable | Needed for |
|-------|:---:|------------|
| `messages:read` | ✅ | `messages get`, `messages thread`, `messages search`, `feed get` |
| `messages:write` | ✅ | `messages send`, `messages edit`, `messages delete`, `reactions`, `messages vote` |
| `channels:read` | ✅ | `channels list`, `channels get`, `channels members` |
| `channels:write` | ✅ | `channels create`, `channels update`, `channels join`, `channels leave`, `channels topic`, `channels purpose` |
| `users:read` | ✅ | `users get`, `users presence` |
| `users:write` | ✅ | `users set-profile`, `users set-presence` |
| `files:read` | ✅ | — |
| `files:write` | ✅ | — |
| `admin:channels` | ❌ | `channels archive`, `channels unarchive`, `channels delete`, `channels add-member`, `channels remove-member` |

---

## 5. Unit Tests

```bash
cargo test -p buzz-cli
# Expected: see cargo test -p buzz-cli for current count

cargo clippy -p buzz-cli -- -D warnings
# Expected: zero warnings
```

---

## 6. Live Testing — Command by Command

Run each command, verify exit code 0 and check output. Most commands
return JSON (pipe through `jq .` to validate). Commands are ordered so
earlier ones create resources that later ones need.

### 6.1 Channels

```bash
# channels create (stream)
buzz channels create --name "test-stream" --type stream --visibility open \
  --description "CLI test channel" | jq .
# Save the channel ID:
CHANNEL_ID=$(buzz channels create --name "test-cli" --type stream --visibility open | jq -r '.channel_id')
# Expected: {"event_id":"...","accepted":true,"message":"...","channel_id":"<uuid>"}

# channels create (forum) — needed for messages vote later
FORUM_ID=$(buzz channels create --name "test-forum" --type forum --visibility open | jq -r '.channel_id')

# channels list
buzz channels list | jq .
# Expected: [{"channel_id":"...","name":"...","description":"...","created_at":N}]
buzz channels list --visibility open | jq .
buzz channels list --member | jq .

# channels get
buzz channels get --channel "$CHANNEL_ID" | jq .
# Expected: {"channel_id":"...","name":"...","description":"...","created_at":N,"pubkey":"..."} or null

# channels update
buzz channels update --channel "$CHANNEL_ID" --name "test-cli-updated" \
  --description "Updated" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels topic
buzz channels topic --channel "$CHANNEL_ID" --topic "Test topic" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels purpose
buzz channels purpose --channel "$CHANNEL_ID" --purpose "Testing" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels join (may already be a member from create)
buzz channels join --channel "$CHANNEL_ID" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels leave
# NOTE: Fails with 400 "cannot remove the last owner" if this identity is the
# sole owner (which it is after channels create). To test leave successfully,
# first add-member a second pubkey as owner. The relay enforces ≥1 owner.
buzz channels leave --channel "$CHANNEL_ID" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."} (or 400 if last owner)

# Re-join so we can send messages
buzz channels join --channel "$CHANNEL_ID" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels archive (requires admin:channels scope)
buzz channels archive --channel "$CHANNEL_ID" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}

# channels unarchive
buzz channels unarchive --channel "$CHANNEL_ID" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"..."}
```

### 6.2 Canvas

```bash
# canvas set
buzz canvas set --channel "$CHANNEL_ID" --content "# Test Canvas" | jq .

# canvas set from stdin
echo "# Canvas from stdin" | buzz canvas set --channel "$CHANNEL_ID" --content - | jq .

# canvas get
buzz canvas get --channel "$CHANNEL_ID"
# Expected: raw markdown string, or: null
```

### 6.3 Messages

```bash
# messages send
MSG=$(buzz messages send --channel "$CHANNEL_ID" --content "Hello from CLI test" | jq .)
echo "$MSG"
EVENT_ID=$(echo "$MSG" | jq -r '.event_id')

# messages send with reply + broadcast
REPLY=$(buzz messages send --channel "$CHANNEL_ID" --content "Reply" \
  --reply-to "$EVENT_ID" --broadcast | jq .)
echo "$REPLY"
REPLY_ID=$(echo "$REPLY" | jq -r '.event_id')

# messages send with mentions — @name in content is auto-resolved, no flag needed
buzz messages send --channel "$CHANNEL_ID" --content "Hey @someone" | jq .

# messages send with NIP-27 nostr:npub1… inline mention — auto-resolved to p-tag
buzz messages send --channel "$CHANNEL_ID" \
  --content "Check with nostr:npub10elfcs4fr0l0r8af98jlmgdh9c8tcxjvz9qkw038js35mp4dma8qzvjptg on this" | jq .

# messages send from stdin — safe path for content with shell metacharacters
# (backticks, $vars, code blocks) that would otherwise be expanded by the shell.
echo 'Body with `backticks` and $vars stays literal.' \
  | buzz messages send --channel "$CHANNEL_ID" --content - | jq .

# messages get
buzz messages get --channel "$CHANNEL_ID" | jq .
buzz messages get --channel "$CHANNEL_ID" --limit 5 | jq .

# messages thread
buzz messages thread --channel "$CHANNEL_ID" --event "$EVENT_ID" | jq .

# messages search
buzz messages search --query "Hello" | jq .
buzz messages search --query "CLI test" --limit 5 | jq .

# messages edit
buzz messages edit --event "$EVENT_ID" --content "Edited by CLI test" | jq .

# messages delete
buzz messages delete --event "$REPLY_ID" | jq .
```

### 6.4 Diff Messages

```bash
# messages send-diff from stdin
echo '--- a/foo.rs
+++ b/foo.rs
@@ -1,3 +1,3 @@
-fn old() {}
+fn new() {}' | buzz messages send-diff \
  --channel "$CHANNEL_ID" \
  --diff - \
  --repo "https://github.com/example/repo" \
  --commit "abcdef1234567890abcdef1234567890abcdef12" | jq .

# messages send-diff with metadata
echo "diff content" | buzz messages send-diff \
  --channel "$CHANNEL_ID" \
  --diff - \
  --repo "https://github.com/example/repo" \
  --commit "abcdef1234567890abcdef1234567890abcdef12" \
  --file "src/main.rs" \
  --lang "rust" \
  --description "Refactored main" | jq .

# messages send-diff with branch + PR metadata
echo "diff content" | buzz messages send-diff \
  --channel "$CHANNEL_ID" \
  --diff - \
  --repo "https://github.com/example/repo" \
  --commit "abcdef1234567890abcdef1234567890abcdef12" \
  --parent-commit "1234567890abcdef1234567890abcdef12345678" \
  --source-branch "feature/cli" \
  --target-branch "main" \
  --pr 42 | jq .
```

### 6.5 Reactions

```bash
# Send a message to react to
REACT_MSG=$(buzz messages send --channel "$CHANNEL_ID" --content "React to this")
REACT_ID=$(echo "$REACT_MSG" | jq -r '.event_id')

# reactions add
buzz reactions add --event "$REACT_ID" --emoji "👍" | jq .

# reactions get
buzz reactions get --event "$REACT_ID" | jq .
# Expected: {"reactions":[{"emoji":"...","count":N,"pubkeys":["..."]}]}

# reactions remove
buzz reactions remove --event "$REACT_ID" --emoji "👍" | jq .
```

### 6.6 DMs

```bash
# dms list
buzz dms list | jq .
# Expected: [{"dm_id":"...","participants":["..."],"created_at":N}]

# dms open (needs a real pubkey — use your own or a test one)
# Get your own pubkey first:
MY_PUBKEY=$(buzz users get | jq -r '.[0].pubkey // empty')
echo "My pubkey: $MY_PUBKEY"

# dms open with a synthetic pubkey (relay will create the user)
DM_RESULT=$(buzz dms open --pubkey "0000000000000000000000000000000000000000000000000000000000000001")
echo "$DM_RESULT" | jq .
# Expected: {"event_id":"...","accepted":true,"message":"...","dm_id":"<uuid>"}
DM_ID=$(echo "$DM_RESULT" | jq -r '.dm_id')

# dms add-member (requires messages:write scope — NOT admin:channels)
buzz dms add-member --channel "$DM_ID" \
  --pubkey "0000000000000000000000000000000000000000000000000000000000000002" | jq .
```

### 6.7 Users & Presence

```bash
# users get — own profile (0 pubkeys)
buzz users get | jq .
# Expected: [{...profile...}] — always returns an array, even for single results

# users get — single pubkey
buzz users get --pubkey "$MY_PUBKEY" | jq .

# users get — batch (2+ pubkeys)
buzz users get --pubkey "$MY_PUBKEY" --pubkey "$MY_PUBKEY" | jq .

# users set-profile
buzz users set-profile --name "CLI Test Agent" --about "Testing buzz-cli" | jq .

# users presence
buzz users presence --pubkeys "$MY_PUBKEY" | jq .

# users set-presence
buzz users set-presence --status online | jq .
buzz users set-presence --status away | jq .
buzz users set-presence --status offline | jq .
# Note: set-presence may fail — kind:20001 is ephemeral and rejected by the HTTP bridge
```

### 6.8 Channel Members (add/remove require admin:channels)

```bash
# channels add-member
buzz channels add-member --channel "$CHANNEL_ID" \
  --pubkey "0000000000000000000000000000000000000000000000000000000000000001" \
  --role member | jq .

# channels members
buzz channels members --channel "$CHANNEL_ID" | jq .
# Expected: [{"pubkey":"...","role":"..."}]

# channels remove-member
buzz channels remove-member --channel "$CHANNEL_ID" \
  --pubkey "0000000000000000000000000000000000000000000000000000000000000001" | jq .
```

### 6.9 Workflows

```bash
# workflows create
# NOTE: trigger uses `on:` tag (serde internally tagged enum).
# Valid triggers: message_posted, reaction_added, diff_posted, schedule, webhook
# Steps use `action:` tag: send_message, send_dm, set_channel_topic, add_reaction, etc.
WF=$(buzz workflows create --channel "$CHANNEL_ID" \
  --yaml 'name: test-wf
trigger:
  on: webhook
steps:
  - id: step1
    action: send_message
    text: "Hello from workflow"' | jq .)
echo "$WF"
WF_ID=$(echo "$WF" | jq -r '.workflow_id')

# workflows list
buzz workflows list --channel "$CHANNEL_ID" | jq .

# workflows get
buzz workflows get --workflow "$WF_ID" | jq .
# Expected: {"workflow_id":"...","content":"<yaml>","created_at":N,"pubkey":"..."} or null

# workflows update (requires --channel)
buzz workflows update --channel "$CHANNEL_ID" --workflow "$WF_ID" \
  --yaml 'name: test-wf-updated
trigger:
  on: webhook
steps:
  - id: step1
    action: send_message
    text: "Updated"' | jq .

# workflows trigger
# NOTE: May return 400 "workflow not found" — the relay indexes workflow
# definitions into a DB table asynchronously. If the definition event hasn't
# been indexed yet, the trigger handler won't find it.
buzz workflows trigger --workflow "$WF_ID" | jq .

# workflows runs
buzz workflows runs --workflow "$WF_ID" | jq .
# Expected: [] — relay stores runs in DB, not as Nostr events; empty is normal

# workflows approve — requires a workflow run waiting for approval
# This is hard to test ad-hoc without a workflow that has an approval gate.
# Test the validation instead:
buzz workflows approve --token "00000000-0000-0000-0000-000000000000" 2>&1 || true
# Should fail with relay error (token not found), not a validation error
# To test the deny path: buzz workflows approve --token <UUID> --approved false

# workflows delete
buzz workflows delete --workflow "$WF_ID" | jq .
```

### 6.10 Feed

```bash
buzz feed get | jq .
buzz feed get --limit 5 | jq .
# Expected: [{id,pubkey,kind,content,created_at,tags}] — sig-stripped, sorted newest-first
```

### 6.11 Forum & Voting

```bash
# Send a forum post (kind 45001) to the forum channel
FORUM_POST=$(buzz messages send --channel "$FORUM_ID" \
  --content "Forum post for vote testing" --kind 45001 | jq .)
echo "$FORUM_POST"
FORUM_EVENT_ID=$(echo "$FORUM_POST" | jq -r '.event_id')

# messages vote (up)
buzz messages vote --event "$FORUM_EVENT_ID" --direction up | jq .

# messages vote (down)
buzz messages vote --event "$FORUM_EVENT_ID" --direction down | jq .
```

### 6.12 Notes (NIP-23 long-form, kind:30023)

Editable team-knowledge notes keyed by `(kind:30023, you, d=slug)`. `set` is an
idempotent upsert; `rm` is a NIP-09 a-tag deletion. Output is plain text (refs),
not JSON — except `get`/`ls`, which emit JSON.

```bash
# set (first publish — --title required, body from stdin)
cat <<'EOF' | buzz notes set --name dco-check --title "DCO Check" \
  --summary "How we verify DCO" --tag dco --tag ci --content -
Run `git log --format='%(trailers:key=Signed-off-by)'` ...
EOF
# → prints event_id / naddr / coordinate / slug / title

# set (edit — omit --title to carry it forward; published_at preserved)
echo "Updated body." | buzz notes set --name dco-check --content -

# get by name (own author resolves directly; cross-author #d query otherwise)
buzz notes get --name dco-check | jq .
buzz notes get --name dco-check --content-only

# get by naddr (exact coordinate; paste the naddr from a set/get above)
buzz notes get --naddr "$NADDR" | jq .

# ls (own by default; --author all across the team; --tag filters)
buzz notes ls | jq .
buzz notes ls --tag dco | jq .
buzz notes ls --author all --limit 10 | jq .

# rm (NIP-09 a-tag deletion; subsequent get must 404)
buzz notes rm --name dco-check
# → prints deleted <coordinate> / deletion <event-id>
buzz notes get --name dco-check   # exits non-zero: not found

# rm of a slug you never published → NotFound, no kind:5 emitted
buzz notes rm --name does-not-exist   # exits non-zero
```

### 6.13 Workstreams (Hive kinds 35000-35003 / 47001-47030)

The Workstream family is the non-git-centric work model: a workstream head
(35000) owns tasks (35001), artifacts (35002), and decision records (35003);
append-only events carry status changes (47001), artifact versions (47002),
reviews (47010-47012), experiment logs (47020), measurements (47021), and
handoffs (47030). Every one of these events is `h`-scoped to a channel, and
every addressable head is referenced by a coordinate `<kind>:<pubkey>:<d-tag>`
in an `a` tag.

Two argument conventions run through the whole family:

- Anywhere a flag takes an id (`--workstream`, `--target`, `--subject`,
  `--id`), you may pass either a bare `d`-tag (owner defaults to you, or to
  the matching `--*-owner` flag) **or** a full `35000:<pubkey>:<id>`
  coordinate. A value containing two `:` is always read as a coordinate.
- `--channel` is repeatable and **required on every write** — an event with no
  `h` tag is rejected client-side before any relay call.

Set up a channel and capture your own pubkey first:

```bash
CH=$(buzz channels create --name hive-runbook --type stream --visibility open \
  | jq -r .channel_id)
ME=$(buzz users get | jq -r '.[0].pubkey')
```

#### Workstream head (35000)

```bash
# create (d-tag = --id; republishing the same id replaces the head, NIP-33 LWW)
buzz workstream create --id thermal-v2 --type hardware \
  --name "Thermal chamber v2" --channel "$CH" \
  --content "Bring-up of the second thermal chamber." | jq .
# → {"event_id":"...","accepted":true,"message":"...","id":"thermal-v2"}

# ws-type vocabulary is closed — clap rejects anything else (exit 1)
buzz workstream create --id x --type firmware --name X --channel "$CH"; echo "exit: $?"

# list (always sends an explicit kinds filter; --channel adds #h scoping)
buzz workstream list --channel "$CH" | jq .
buzz workstream list --channel "$CH" --type hardware --status active | jq .
buzz --format compact workstream list --channel "$CH" | jq .
# compact rows: {id, kind, created_at, d, name, status, a}

# show resolves the current head by coordinate (newest created_at, lowest id
# on a tie) and 404s (exit 1) when nothing is published
buzz workstream show --id thermal-v2 | jq .
buzz workstream show --id 35000:"$ME":thermal-v2 | jq .
buzz workstream show --id never-published; echo "exit: $?"   # exit: 1

# set-status is a read-modify-write of the head: every other tag survives and
# created_at advances by exactly one second past the head we read
buzz workstream set-status --id thermal-v2 --status paused | jq .
buzz workstream show --id thermal-v2 | jq '.[0].tags'
```

#### Tasks (35001) and status history (47001)

```bash
buzz task create --id calibrate-probe --workstream thermal-v2 \
  --name "Calibrate the thermocouple probe" --channel "$CH" \
  --assignee "$ME" --due 2026-08-01 | jq .

# malformed due dates are refused before any relay call (exit 1)
buzz task create --id bad-due --workstream thermal-v2 --name X \
  --channel "$CH" --due "2026-8-1"; echo "exit: $?"

# list by parent workstream (#a), by channel (#h), by status or assignee
buzz task list --workstream thermal-v2 | jq .
buzz task list --channel "$CH" --status todo | jq .
buzz task list --workstream thermal-v2 --assignee "$ME" | jq .

# show returns the head first, then its 47001 history oldest-first
buzz task show --id calibrate-probe | jq .
buzz task show --id calibrate-probe --head-only | jq .

# edit is a NIP-33 replace of the head — only the flags you pass change
buzz task edit --id calibrate-probe --name "Calibrate probe (rev B)" | jq .
buzz task edit --id calibrate-probe --clear-due | jq .
buzz task edit --id calibrate-probe; echo "exit: $?"   # exit: 1, nothing to edit

# status emits the 47001 history event AND bumps the head (two write responses)
buzz task status --id calibrate-probe --status in-progress --note "on the bench"
# → two JSON lines: the 47001 event, then the replaced head

# --no-bump records history only
buzz task status --id calibrate-probe --status blocked --no-bump | jq .

# a no-op transition is refused before writing anything (exit 1)
buzz task status --id calibrate-probe --status blocked; echo "exit: $?"
```

**Conflict check (exit 5).** Race two head replacements against the same read
to confirm the NIP-33 LWW conflict path. From two shells, run
`buzz task edit --id calibrate-probe --name A` and `... --name B` at the same
time; the loser exits 5 with
`{"error":"conflict","message":"conflict: the head was replaced concurrently; re-read it and retry"}`.

#### Artifacts (35002) and versions (47002)

```bash
buzz artifact create --id chamber-bom --type bom \
  --name "Thermal chamber BOM" --workstream thermal-v2 \
  --channel "$CH" --version v1 | jq .

# blob refs must be full 64-char SHA-256 digests (exit 1 otherwise)
buzz artifact create --id bad-blob --type doc --name X --channel "$CH" \
  --blob deadbeef; echo "exit: $?"

# publish an immutable version; --hash is the SHA-256 of the payload.
# channels default to the artifact head's own h tags.
DIGEST=$(printf 'payload' | shasum -a 256 | cut -d' ' -f1)
buzz artifact version --id chamber-bom --version v2 --hash "$DIGEST" \
  --changelog "swapped in PT100 probes" --bump-head | jq .
# --bump-head additionally replaces the head's version pointer (exit 5 on race)

buzz artifact list --channel "$CH" | jq .
buzz artifact list --workstream thermal-v2 --type bom | jq .
buzz artifact show --id chamber-bom | jq .            # head + versions
buzz artifact show --id chamber-bom --head-only | jq .
```

#### Reviews (47010 / 47011 / 47012)

Reviews target *any* addressable workstream entity — this is the "non-code
user completes a review cycle without git" path.

```bash
REQ=$(buzz review request --target chamber-bom --target-kind artifact \
  --channel "$CH" --reviewer "$ME" \
  --content "Please sanity-check the connector choices." | jq -r .event_id)

# --target-kind is required unless --target is a full coordinate (exit 1)
buzz review request --target chamber-bom --channel "$CH"; echo "exit: $?"

# comments thread with NIP-10 markers; --parent nests, default replies to root
buzz review comment --request "$REQ" --channel "$CH" \
  --content "connector J4 pinout looks wrong" | jq .
buzz review comment --request "$REQ" --parent "$REQ" --channel "$CH" \
  --content "agreed" | jq .

# verdicts: approve | request-changes | reject
buzz review decide --request "$REQ" --decision request-changes \
  --target chamber-bom --target-kind artifact --channel "$CH" \
  --content "fix J4 then re-request" | jq .

# a non-approve verdict with no rationale is refused (exit 1)
buzz review decide --request "$REQ" --decision reject \
  --target chamber-bom --target-kind artifact --channel "$CH"; echo "exit: $?"

buzz review list --target chamber-bom --target-kind artifact | jq .
buzz review list --channel "$CH" --requests-only | jq .
buzz review list; echo "exit: $?"   # exit: 1 — must scope by target or channel
```

#### Decision records (35003)

```bash
buzz decision create --id adr-0001 --workstream thermal-v2 \
  --name "Use thermocouples" --status accepted --channel "$CH" \
  --content "## Context ..." | jq .

# supersede publishes the successor and retires the predecessor in one command;
# workstream and channel scope are inherited from the predecessor head
buzz decision supersede --id adr-0002 --supersedes adr-0001 \
  --name "Use PT100 probes" --content "## Context: thermocouples drift." | jq .
# → two JSON lines: the new record, then adr-0001 replaced with status=superseded

buzz decision list --workstream thermal-v2 | jq .
buzz decision list --channel "$CH" --status superseded | jq .
buzz decision supersede --id adr-0003 --supersedes adr-0003 --name X --content Y
# exit: 1 — a record cannot supersede itself
```

#### Handoffs, experiments, measurements

```bash
# handoff (47030): sender is always your own identity
buzz handoff create --workstream thermal-v2 --to "$PEER_PUBKEY" \
  --channel "$CH" --content "Chamber is calibrated; over to data." \
  --item "probe cal sheet attached" --item "raw logs uploaded" | jq .
buzz handoff list --workstream thermal-v2 | jq .

# experiment log (47020): --label values become t tags
buzz experiment log --workstream thermal-v2 --id run-14 --channel "$CH" \
  --content "Soak at 85C for 6h; no drift observed." \
  --label soak --label thermal | jq .
buzz experiment list --workstream thermal-v2 --label soak | jq .

# measurement (47021): subject is a workstream by default, or an artifact
buzz measure add --subject thermal-v2 --series chamber-temp \
  --value 84.7 --unit celsius --channel "$CH" | jq .
buzz measure add --subject chamber-bom --subject-kind artifact \
  --series mass --value 12.4 --unit kg --channel "$CH" | jq .
buzz measure list --subject thermal-v2 --series chamber-temp | jq .

# units and series are whitespace-free labels (exit 1 otherwise)
buzz measure add --subject thermal-v2 --series s --value 1 \
  --unit "deg C" --channel "$CH"; echo "exit: $?"
```

#### End-to-end acceptance walk (Hive plan §8 Phase 3 exit criterion)

A non-code user creates a workstream, attaches an artifact, and runs a full
review cycle without touching git:

```bash
buzz workstream create --id demo --type design --name "Demo" --channel "$CH"
buzz artifact create --id spec --type doc --name Spec --workstream demo --channel "$CH"
REQ=$(buzz review request --target spec --target-kind artifact --channel "$CH" \
  --content "ready for review" | jq -r .event_id)
buzz review comment --request "$REQ" --channel "$CH" --content "one nit"
buzz review decide --request "$REQ" --decision approve --target spec \
  --target-kind artifact --channel "$CH"
buzz review list --target spec --target-kind artifact | jq 'length'   # → 3
```

**Relay assumption:** these commands need only generic Nostr handling — kind
registration, `h`-tag channel scoping, NIP-33 addressable replacement for
35000-35003, and `#a`/`#d`/`#h`/`#t` tag filters on `POST /query`. No
workstream-specific HTTP endpoint exists or is needed. If a relay build has
not yet registered kinds 35000-35003 / 47001-47030, writes come back
`accepted: false` and the CLI reports
`{"error":"error","message":"relay rejected event: ..."}` with exit 4.

---

---

## 7. Error Path Testing

Verify the CLI produces correct JSON on stderr and correct exit codes.

```bash
# Exit 1: Invalid UUID
buzz channels get --channel "not-a-uuid" 2>&1; echo "exit: $?"
# stderr: {"error":"user_error","message":"invalid UUID: not-a-uuid"}
# exit: 1

# Exit 1: Invalid hex64
buzz messages delete --event "not-hex" 2>&1; echo "exit: $?"
# stderr: {"error":"user_error","message":"must be a 64-character hex string: not-hex"}
# exit: 1

# Exit 1: Invalid --type value (clap validates the enum — multi-line error)
buzz channels create --name x --type invalid --visibility open 2>&1; echo "exit: $?"
# stderr: {"error":"user_error","message":"error: invalid value 'invalid' for '--type <CHANNEL_TYPE>'\n  [possible values: stream, forum]\n..."}
# exit: 1

# Exit 1: Invalid --direction value
buzz messages vote --event "$(printf '0%.0s' {1..64})" \
  --direction sideways 2>&1; echo "exit: $?"
# exit: 1

# Exit 1: Empty body guard
buzz users set-profile 2>&1; echo "exit: $?"
# exit: 1 (at least one field required)

# Exit 3: No auth configured
env -u BUZZ_PRIVATE_KEY \
  cargo run -p buzz-cli -- channels list 2>&1; echo "exit: $?"
# stderr: {"error":"auth_error","message":"auth error: BUZZ_PRIVATE_KEY is required (use --private-key or set env var)"}
# exit: 3

# Not-found returns null, not an error (exit 0)
buzz channels get --channel "00000000-0000-0000-0000-000000000000"
# stdout: null
# exit: 0
```

---

## 8. Auth Testing

Test authentication.

```bash
# Private key (BUZZ_PRIVATE_KEY)
BUZZ_PRIVATE_KEY="nsec1..." buzz channels list | jq .
# Should succeed

# No auth → exit 3
env -u BUZZ_PRIVATE_KEY \
  cargo run -p buzz-cli -- channels list 2>&1; echo "exit: $?"
# stderr: {"error":"auth_error","message":"auth error: BUZZ_PRIVATE_KEY is required (use --private-key or set env var)"}
# exit: 3
```

---

## 9. Cleanup

```bash
# Delete test channels
buzz channels delete --channel "$CHANNEL_ID" | jq .
buzz channels delete --channel "$FORUM_ID" | jq .
```

---

## 10. Checklist

| # | Command | Tested | Notes |
|---|---------|:------:|-------|
| 1 | `messages send` | ☐ | Basic, reply, broadcast, mentions, stdin |
| 2 | `messages send-diff` | ☐ | Stdin, metadata, branch/PR |
| 3 | `messages edit` | ☐ | |
| 4 | `messages delete` | ☐ | |
| 5 | `messages get` | ☐ | With limit |
| 6 | `messages thread` | ☐ | |
| 7 | `messages search` | ☐ | With limit |
| 8 | `messages vote` | ☐ | Up and down |
| 9 | `channels list` | ☐ | With visibility, member |
| 10 | `channels get` | ☐ | |
| 11 | `channels create` | ☐ | Stream and forum |
| 12 | `channels update` | ☐ | |
| 13 | `channels topic` | ☐ | |
| 14 | `channels purpose` | ☐ | |
| 15 | `channels join` | ☐ | |
| 16 | `channels leave` | ☐ | |
| 17 | `channels archive` | ☐ | Needs admin:channels |
| 18 | `channels unarchive` | ☐ | Needs admin:channels |
| 19 | `channels delete` | ☐ | Needs admin:channels |
| 20 | `channels members` | ☐ | |
| 21 | `channels add-member` | ☐ | Needs admin:channels |
| 22 | `channels remove-member` | ☐ | Needs admin:channels |
| 23 | `canvas get` | ☐ | |
| 24 | `canvas set` | ☐ | Direct and stdin |
| 25 | `reactions add` | ☐ | |
| 26 | `reactions remove` | ☐ | |
| 27 | `reactions get` | ☐ | |
| 28 | `dms list` | ☐ | |
| 29 | `dms open` | ☐ | |
| 30 | `dms add-member` | ☐ | Needs messages:write |
| 31 | `users get` | ☐ | Self, single, batch |
| 32 | `users set-profile` | ☐ | |
| 33 | `users presence` | ☐ | |
| 34 | `users set-presence` | ☐ | online, away, offline |
| 35 | `workflows list` | ☐ | |
| 36 | `workflows create` | ☐ | |
| 37 | `workflows update` | ☐ | |
| 38 | `workflows delete` | ☐ | |
| 39 | `workflows trigger` | ☐ | |
| 40 | `workflows runs` | ☐ | |
| 41 | `workflows get` | ☐ | |
| 42 | `workflows approve` | ☐ | Validation only (needs approval gate); bare = approve, `--approved false` = deny |
| 43 | `feed get` | ☐ | |
| 44 | `social publish` | ☐ | |
| 45 | `social set-contacts` | ☐ | |
| 46 | `social event` | ☐ | |
| 47 | `social notes` | ☐ | |
| 48 | `social contacts` | ☐ | |
| 49 | `repos create` | ☐ | |
| 50 | `repos get` | ☐ | |
| 51 | `repos list` | ☐ | |
| 52 | `repos protect list` | ☐ | Empty/populated rules; unknown rules visible; malformed rule reported in validation_error |
| 53 | `repos protect set` | ☐ | Create and replace complete exact-ref rule; verify metadata is preserved |
| 54 | `repos protect remove` | ☐ | Remove exact ref; missing rule → NotFound |
| 55 | `upload file` | ☐ | |
| 56 | `pack validate` | ☐ | Local, no relay |
| 57 | `pack inspect` | ☐ | Local, no relay |
| 58 | `notes set` | ☐ | First publish, edit/carry, --clear-tags, ambiguity, empty-stdin guard |
| 59 | `notes get` | ☐ | By name, by naddr, --content-only, cross-author, ambiguous → exit 1 |
| 60 | `notes ls` | ☐ | Own, --author all, --tag, --limit |
| 61 | `notes rm` | ☐ | Delete→get 404, double-delete idempotent, missing slug → NotFound |
| 62 | `workstream create` | ☐ | Repeated --channel, --member; closed ws-type vocabulary |
| 63 | `workstream list` | ☐ | --channel/#h scoping, --type/--status filters, --format compact |
| 64 | `workstream show` | ☐ | Bare id and full coordinate; missing head → exit 1 |
| 65 | `workstream set-status` | ☐ | Other tags preserved; created_at advances by 1 |
| 66 | `task create` | ☐ | Parent #a coordinate, --assignee, ISO --due validation |
| 67 | `task list` | ☐ | By --workstream (#a), --channel (#h), --status, --assignee |
| 68 | `task show` | ☐ | Head + 47001 history; --head-only |
| 69 | `task edit` | ☐ | Partial edit preserves tags; no flags → exit 1; race → exit 5 |
| 70 | `task status` | ☐ | Emits 47001 + bumps head; --no-bump; no-op transition → exit 1 |
| 71 | `artifact create` | ☐ | --type, --version, --blob sha-256 validation |
| 72 | `artifact version` | ☐ | --hash required; channels inherited; --bump-head → exit 5 on race |
| 73 | `artifact list` | ☐ | --channel, --workstream, --type |
| 74 | `artifact show` | ☐ | Head + 47002 versions; --head-only |
| 75 | `review request` | ☐ | All four target kinds; --target-kind required for bare ids |
| 76 | `review comment` | ☐ | NIP-10 root vs nested markers |
| 77 | `review decide` | ☐ | approve/request-changes/reject; rationale required for non-approve |
| 78 | `review list` | ☐ | By --target and --channel; unscoped → exit 1 |
| 79 | `decision create` | ☐ | Parent workstream #a, status vocabulary |
| 80 | `decision list` | ☐ | --workstream, --channel, --status |
| 81 | `decision supersede` | ☐ | Successor + predecessor retired; self-supersede → exit 1 |
| 82 | `handoff create` | ☐ | from/to p markers, repeated --item checklist |
| 83 | `handoff list` | ☐ | --workstream/--channel; unscoped → exit 1 |
| 84 | `experiment log` | ☐ | --label → t tags |
| 85 | `experiment list` | ☐ | --label filter; unscoped → exit 1 |
| 86 | `measure add` | ☐ | Workstream and artifact subjects; label validation |
| 87 | `measure list` | ☐ | --series filter; unscoped → exit 1 |

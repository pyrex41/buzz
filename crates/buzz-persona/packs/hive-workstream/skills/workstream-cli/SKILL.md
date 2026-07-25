---
name: workstream-cli
description: "Drive Buzz workstreams, tasks, artifacts, reviews, decisions, handoffs, experiments, and measurements with the `buzz` CLI."
---

# Workstream CLI

Every workstream operation is a signed Nostr event published with `buzz`. There
is no HTTP API to learn and no git repository involved.

## The model

| Kind | What it is | Shape |
|------|-----------|-------|
| 35000 | workstream | head — replaced in place |
| 35001 | task | head — replaced in place |
| 35002 | artifact | head — replaced in place |
| 35003 | decision record | head — replaced in place |
| 47001 | task status change | append-only history |
| 47002 | artifact version | append-only history |
| 47010 / 47011 / 47012 | review request / comment / decision | append-only history |
| 47020 / 47021 | experiment log / measurement | append-only history |
| 47030 | handoff | append-only history |

**Heads vs. history.** A head is the current truth for one identifier and is
replaced last-write-wins: republishing the same `--id` overwrites it, and the
old value is gone. History events are never replaced — they accumulate, and the
sequence *is* the record. Never rewrite history to correct a mistake; append the
correction.

**Coordinates.** Every head is addressed as `<kind>:<pubkey>:<id>`. Anywhere a
flag takes an id (`--workstream`, `--target`, `--subject`, `--id`) you may pass
either a bare identifier (owner defaults to you, or to the matching `--*-owner`
flag) or a full coordinate. A value containing two `:` is read as a coordinate.

**Channel scope.** `--channel` is repeatable and required on every write.

**Conflicts.** Exit code 5 means someone replaced the head between your read and
your write. Re-read it, re-apply your change, retry. Never retry blindly.

## Setup

```bash
CH=$(buzz channels create --name hive --type stream --visibility open | jq -r .channel_id)
ME=$(buzz users get | jq -r '.[0].pubkey')
```

## Workstreams and tasks

```bash
buzz workstream create --id thermal-v2 --type hardware \
  --name "Thermal chamber v2" --channel "$CH" --content "Bring-up notes."
buzz workstream list --channel "$CH" --type hardware --status active | jq .
buzz workstream show --id thermal-v2 | jq .
buzz workstream set-status --id thermal-v2 --status paused

buzz task create --id calibrate-probe --workstream thermal-v2 \
  --name "Calibrate the thermocouple probe" --channel "$CH" \
  --assignee "$PEER" --due 2026-08-01
buzz task list --workstream thermal-v2 --status todo | jq .
buzz task show --id calibrate-probe | jq .          # head, then 47001 history
buzz task edit --id calibrate-probe --name "Calibrate probe (rev B)"
buzz task status --id calibrate-probe --status in-progress --note "on the bench"
buzz task status --id calibrate-probe --status blocked --no-bump
```

`--type` is a closed vocabulary: `code`, `systems`, `hardware`, `data`,
`design`, `process`, `docs`, `general`. Task status flows through
`todo | in-progress | blocked | review | done`. `buzz task status` writes the
47001 history event *and* bumps the head; `--no-bump` records history only.

## Artifacts and versions

```bash
buzz artifact create --id chamber-bom --type bom --name "Thermal chamber BOM" \
  --workstream thermal-v2 --channel "$CH" --version v1
DIGEST=$(shasum -a 256 bom.csv | cut -d' ' -f1)
buzz artifact version --id chamber-bom --version v2 --hash "$DIGEST" \
  --changelog "swapped in PT100 probes" --bump-head
buzz artifact show --id chamber-bom | jq .          # head, then 47002 versions
buzz artifact list --workstream thermal-v2 --type bom | jq .
```

`--type` is a free label: `doc`, `design`, `dataset`, `measurement`, `bom`,
`sim-result`. `--hash` is the SHA-256 of the payload you versioned. Every
revision gets its own 47002 event — that is how a reviewer sees what changed
between the version they rejected and the version you are re-submitting.

## Reviews

```bash
REQ=$(buzz review request --target chamber-bom --target-kind artifact \
  --channel "$CH" --reviewer "$REVIEWER_PUBKEY" \
  --content "v2 ready — connector choices are the risk." | jq -r .event_id)

buzz review comment --request "$REQ" --channel "$CH" --content "J4 pinout is inverted"
buzz review comment --request "$REQ" --parent "$COMMENT_ID" --channel "$CH" --content "agreed"

buzz review decide --request "$REQ" --decision request-changes \
  --target chamber-bom --target-kind artifact --channel "$CH" \
  --content "fix J4, then re-request"

buzz review list --target chamber-bom --target-kind artifact | jq .
buzz review list --channel "$CH" --requests-only | jq .
```

`--target-kind` is one of `workstream`, `task`, `artifact`, `decision`, and is
required unless `--target` is a full coordinate. `--reviewer` mentions a
reviewer so their agent wakes. Verdicts are `approve`, `request-changes`,
`reject`; the last two require a non-empty `--content` rationale.

One request is one round. A `request-changes` verdict closes that round — the
next revision opens a **new** request against the same target.

## Decisions, handoffs, experiments, measurements

```bash
buzz decision create --id adr-0001 --workstream thermal-v2 --channel "$CH" \
  --name "Use PT100 probes" --status accepted --content "## Context ..."
buzz decision supersede --id adr-0002 --supersedes adr-0001 \
  --name "Use thermistors" --content "## Context: PT100 lead resistance."
buzz decision list --workstream thermal-v2 --status accepted | jq .

buzz handoff create --workstream thermal-v2 --to "$PEER" --channel "$CH" \
  --content "Chamber is calibrated; over to data." \
  --item "probe cal sheet attached" --item "raw logs uploaded"
buzz handoff list --workstream thermal-v2 | jq .

buzz experiment log --workstream thermal-v2 --id run-14 --channel "$CH" \
  --content "Soak at 85C for 6h; no drift." --label soak --label thermal
buzz experiment list --workstream thermal-v2 --label soak | jq .

buzz measure add --subject thermal-v2 --series chamber-temp \
  --value 84.7 --unit celsius --channel "$CH"
buzz measure add --subject chamber-bom --subject-kind artifact \
  --series mass --value 12.4 --unit kg --channel "$CH"
buzz measure list --subject thermal-v2 --series chamber-temp | jq .
```

Decision status is `proposed | accepted | rejected | superseded`. `supersede`
publishes the successor and retires the predecessor in one command, inheriting
its workstream and channels. Series and unit labels must be whitespace-free.

## Exit codes

`0` ok · `1` input error · `2` network/relay · `3` auth · `4` other ·
`5` write conflict. Read the JSON on stderr before retrying anything.

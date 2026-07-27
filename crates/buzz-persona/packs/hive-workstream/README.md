# Hive Workstream

A six-agent persona pack for non-code work — hardware, data, design, process,
and docs. The team drives the Workstream kind family
(35000–35003 heads, 47001–47030 history) entirely through the `buzz` CLI. No
git repository is involved anywhere.

| Agent | Role |
|-------|------|
| **Bramble** (`coordinator`) | Cross-functional coordinator — workstream head, task board, handoffs |
| **Quill** (`spec-writer`) | Spec writer — artifacts, versions, review requests |
| **Vex** (`critic`) | Reviewer — threaded comments and verdicts |
| **Ledger** (`decision-log`) | Decision-log maintainer — decision records and supersede chains |
| **Sprocket** (`bringup`) | Hardware bring-up — experiment logs and measurements |
| **Sluice** (`pipeline-reviewer`) | Data-pipeline reviewer — datasets, transforms, claims |

All six share the `workstream-cli` skill, which is the runnable reference for
every verb they use.

## Usage

```bash
# Validate the pack
buzz pack validate ./crates/buzz-persona/packs/hive-workstream

# Inspect resolved config
buzz pack inspect ./crates/buzz-persona/packs/hive-workstream
```

Deploy the agents with `buzz-acp`, one identity per persona, all added as
members of the channel the workstream lives in. The harness subscribes managed
agents to task heads (35001), review requests (47010), and handoffs (47030), so
an agent wakes when a task is assigned to it, a review names it as reviewer, or
a handoff is addressed to it.

## Structure

```
hive-workstream/
├── .plugin/
│   └── plugin.json               # Pack manifest (OPS-compatible)
├── agents/
│   ├── coordinator.persona.md
│   ├── spec-writer.persona.md
│   ├── critic.persona.md
│   ├── decision-log.persona.md
│   ├── bringup.persona.md
│   └── pipeline-reviewer.persona.md
├── skills/
│   └── workstream-cli/
│       └── SKILL.md              # Workstream CLI reference (shared)
├── instructions.md               # Team-wide protocol
└── README.md
```

## The Protocol Under Test

The review cycle these personas describe is pinned by
`crates/buzz-test-client/tests/e2e_workstream_review.rs`: a human opens a
`hardware` workstream and a task, the spec writer publishes an artifact plus a
version and requests review, the critic comments and requests changes, the spec
writer publishes v2 and re-requests, the critic approves, and the human records
the decision, completes the task, and hands off. See `TESTING.md` §
*Multi-agent Workstream review cycle*.

## Customizing

Edit any `.persona.md` file. The YAML frontmatter controls config (model,
triggers, channels, skills); the markdown body is the system prompt. Change
`subscribe` to the channel your workstreams actually live in.

See `crates/buzz-persona/PERSONA_PACK_SPEC.md` for the full format reference.

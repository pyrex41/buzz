# Buzz/Hive — System & Network Design Specification

Stack-agnostic description of the system as built: what it does, the wire
contracts, the data model, and the invariants any reimplementation must
honor. Language, framework, and database choices are deliberately treated as
replaceable; everything here is expressed as interfaces and semantics. Where
an invariant was learned the hard way, it is marked **[INVARIANT]** — these
are normative, not stylistic.

---

## 1. What the system is

A self-hostable, multi-tenant, event-sourced collaboration platform: channels
and DMs (chat), threaded conversations, media, full-text search, git hosting,
voice huddles, AI-agent participation, and a **Workstream** layer (tasks,
artifacts, reviews, decisions) that lets non-code teams run structured work
without touching git. One logical server ("relay") serves everything over a
single WebSocket protocol plus a narrow HTTP surface.

Two deployment profiles, one binary, config-only switch:

- **Solo** — zero external services. Embedded single-file database,
  in-process pub/sub and shared state, local-filesystem media. Boots from an
  empty directory to a ready server with one flag.
- **Full** — clustered. Client-server RDBMS (with optional read replicas),
  shared cache/pub-sub service (Redis-class), object storage (S3-class),
  optional multi-node fan-out mesh.

Success metrics (verified by the standing E2E gates): a non-code user
completes a review cycle without git; the binary is usable with zero external
services; the storage switch is config-only, proven by the same E2E suite
passing on both profiles.

## 2. Core model: signed events

Everything is a **signed, immutable event** (Nostr NIP-01 shape): `{id,
pubkey, created_at, kind, tags, content, sig}` where `id` is the SHA-256 of
the canonical serialization and `sig` a BIP-340 Schnorr signature. The server
verifies signatures; state is derived from the event log, never authored by
the server on a user's behalf.

Two persistence classes by kind number:

- **Regular** — append-only history. Stored forever (subject to deletion
  events / retention).
- **Addressable (parameterized-replaceable)** — kinds 30000–39999 carry a
  `d` tag; the tuple `(kind, pubkey, d)` is a **coordinate** and only the
  latest event per coordinate is the live "head".

**[INVARIANT] Last-write-wins resolution** is: highest `created_at` wins;
ties broken by lexicographically **smallest** `id`. A stale write is
acknowledged as accepted with the machine-readable message prefix
`duplicate:` (clients map this to a distinct "write conflict" outcome — the
CLI reserves exit code 5 for it). Head resolution must be a pure function of
the event **set** — same events in any arrival order produce the same heads
(clients and server both implement this reduce).

**Reference tags**: `e` (event id, 64-char lowercase hex), `p` (pubkey),
`a` (coordinate, `<kind>:<pubkey-hex>:<d>`), `h` (channel id — the scoping
tag, see §4), `x` (SHA-256 of a media blob). **[INVARIANT]** All hex in tags
is lowercase; tag matching is byte-exact, so an uppercase spelling is
invisible to filters.

## 3. Identity, authentication, authorization

- **Identity is a keypair.** Users and agents are secp256k1 public keys; no
  server-issued accounts. Display profiles are themselves events.
- **WebSocket auth (NIP-42)**: server issues a challenge; client signs an
  ephemeral auth event naming the relay URL and challenge. Mandatory —
  unauthenticated sockets get a bounded window then are refused.
- **HTTP auth (NIP-98)**: each request carries a signed, single-use event
  binding method+URL+body-hash, with a replay guard (see SharedState §9).
- **Authorization** layers, all fail-closed:
  - **Community membership** (relay membership roster, owner/admin/member
    roles, allowlists, bans/timeouts).
  - **Channel membership** for private channels and DMs.
  - **Scope registry**: every accepted kind maps to a permission scope;
    **[INVARIANT]** unknown kinds are rejected (`restricted: unknown event
    kind`), so adding a kind is an explicit act.
  - **The p-gate**: queries that could enumerate private material (DM gift
    wraps, notifications, viewer-private snapshots) must be constrained to
    the requester's own pubkey; open-ended queries without explicit `kinds`
    are refused outright.
  - **Result-level guards**: viewer-private kinds are filtered from results
    even when addressed by id.

## 4. Multi-tenancy and the network boundary

One deployment hosts many **communities**. **[INVARIANT] The community is
derived from the request's Host (host:port), row zero of every request** —
WebSocket door, every HTTP route, git smart-HTTP, media, webhooks. There is
no default tenant and the server never echoes an unknown host into a
community. All storage rows carry the community id; all queries are
community-scoped before any other predicate.

Inside a community, **channels** scope conversation. **[INVARIANT]
Channel-scoped kinds must carry an `h` tag** and readers only see events in
channels they can access. DMs are channels of type `dm` with a canonical
participant-set hash for idempotent open (2–9 participants), per-participant
hide/unhide, and viewer-private visibility snapshots.

Practical consequence for multi-node topologies: two nodes serving the same
community **must present the same public host** (shared `RELAY_URL`), even
though they listen on different addresses — otherwise they operate disjoint
tenants and no cross-node traffic exists.

## 5. Wire protocol

### 5.1 WebSocket (primary)

Nostr relay grammar: client→server `["EVENT", ev]`, `["REQ", sub_id,
filter...]`, `["COUNT", ...]`, `["CLOSE", sub_id]`, `["AUTH", ev]`;
server→client `["OK", id, accepted, message]`, `["EVENT", sub_id, ev]`,
`["EOSE", sub_id]`, `["AUTH", challenge]`, `["CLOSED", ...]`. Filters:
`ids`, `authors`, `kinds`, `#e/#p/#a/#d/#h/#t...`, `since/until`, `limit`,
plus NIP-50 `search`. A REQ answers history then stays live; live events are
fanned out to matching, authorized subscriptions across the whole
deployment (via the messaging layer, §9). Per-connection subscription count
is capped (1024); admission is rate-limited per principal class.

`OK` message conventions are machine-readable prefixes:
`invalid:` (validation), `restricted:` (authorization/capability),
`duplicate:` (idempotent replay or LWW-stale), `error:` (server fault).

### 5.2 HTTP (narrow, enumerated)

The rule is **prefer new event kinds over new endpoints**. The HTTP surface
is: NIP-11 metadata; NIP-05; `POST /events` (same admission pipeline as WS);
`POST /query` — body is a **JSON array of filters** (NIP-50 `search`
filters route to the search service); `POST /count`; Blossom media
upload/get; git smart-HTTP + policy hooks; workflow webhooks `/hooks/{id}`;
health (`/_readiness`, DB-touching) and metrics endpoints on separate ports.
All of it host-scoped per §4 and NIP-98-authenticated except health/metadata.

### 5.3 Event admission pipeline

Signature verify → host/tenant bind → scope registry → rate limits →
kind-specific validation → channel derivation (reactions/deletions derive
their channel from their target) → membership/archival checks → persist →
side effects (counters, notifications, discovery events) → audit append →
fan-out → deep-link/preview enrichment. Rejections are `OK false` with the
prefixes above; **[INVARIANT]** validation failures never surface as
transport errors (no 500s for bad input).

## 6. Kind registry (functional domains)

A single authoritative registry of integer kinds (uniqueness-tested, scoped,
documented). Domains as built:

- **Chat**: channel message (9), thread replies, reactions (7), deletions
  (5), pins, read-state, typing/presence (ephemeral), broadcast flags.
- **Channel/community management (NIP-29-style)**: create/edit/join/leave,
  channel metadata (39000-range addressable), membership admin events,
  moderation actions and reports, invites, join policies.
- **DMs**: NIP-17 gift-wrapped payloads (sealed sender), DM discovery
  events, visibility snapshots (viewer-private addressable kind 30622).
- **Media**: file metadata events referencing content-addressed blobs.
- **Git (capability-gated)**: NIP-34 (30617/30618 repo state, 1617–1633
  patches/issues), plus CI/status kinds.
- **Workflows**: YAML-as-code automation definitions + run/approval events
  (46xxx), webhook-triggered.
- **Agents**: agent profile/config kinds, drafts requiring owner review,
  observer frames, memory ("engram") kinds with strict envelope validation.
- **Audit**: hash-chain checkpoint kind (48001).
- **Workstreams (35000–35003, 47001–47030)** — see §7.

## 7. Workstream domain model

Purpose: structured work for any discipline (`ws-type` ∈ code, systems,
hardware, data, design, process, docs, general) with the same machinery chat
uses — no new endpoints, full realtime fan-out, search, audit for free.

**Addressable heads** (each `d`-identified, LWW-edited):

| Kind | Entity | Required | Notable tags |
|---|---|---|---|
| 35000 | Workstream | `d`, `h` | `ws-type` (closed vocab), `name`, `status` (active/paused/done/archived), `p` members |
| 35001 | Task | `d`, `h`, `a`→35000 | `name`, `status` (todo/in-progress/blocked/in-review/done/cancelled), `assignee`, `due` |
| 35002 | Artifact | `d`, `h` | `artifact-type` (open vocab), `name`, `x` sha-256 blob refs, current-version pointer |
| 35003 | Decision record | `d`, `h`, `a`→35000 | `status` (proposed/accepted/rejected/superseded), `supersedes` (event id) |

**Append-only history** (immutable):

| Kind | Meaning | Required |
|---|---|---|
| 47001 | Task status change | `a`→35001, `status`, `previous-status` |
| 47002 | Artifact version | `a`→35002, `version`, `content-hash`; changelog in content |
| 47010 | Review request | `a`→35001/35002/35003, `p` reviewers |
| 47011 | Review comment | `e` thread ref (NIP-10 marks) |
| 47012 | Review decision | subject ref + `decision` ∈ approve/request-changes/reject |
| 47020 | Experiment log | `a`→35000 |
| 47021 | Measurement | `a`→35000, `series`/`value`/`unit` |
| 47030 | Handoff | `p` with from/to markers, checklist in content |

**[INVARIANT] Reference validation is format-only** (offline-first: a task
may reference a workstream the relay hasn't seen). Existence is a client
concern. Convention: mutate-a-head operations that also record history
publish the durable history event **first**, then the head replacement, so a
LWW conflict on the head never loses the recorded transition.

The review cycle (request → threaded comments → decision → new version →
approval → decision record → task completion) is exercised end-to-end by a
three-party E2E (author, reviewer, coordinator) asserting **cross-party
observation** — each step must be *visible to the other parties via
standard filters*, not merely accepted.

## 8. Storage layer

One storage interface, two backends. The serving path is
backend-dispatched (~200 operations across events, channels, membership,
DMs, threads/counters, reactions, moderation, workflows, users, search,
audit); subsystems inherently tied to the Full profile (push-notification
outbox, mesh session fencing, read replicas, partition maintenance) stay
concrete behind config gates and are **off** in Solo.

Shared semantic contracts (conformance-tested against every backend):
event lifecycle including addressable LWW and `duplicate:` conflicts;
`h`-scoping and tenant isolation; filter semantics; **materialized thread
counters** (`reply_count`, `descendant_count` maintained transactionally
with reply insert); audit chain verification; search behaviors; media
round-trip.

### 8.1 Full profile (client-server RDBMS)

Advisory locks serialize NIP-33 coordinate writers; time-partitioned event
tables with partition maintenance; optional read replicas behind a
**freshness fence** (reads route to a replica only when it has provably
caught up to the writer's high-water mark). Command events use an
open-transaction guard: insert the command event, run domain mutations on
separate connections, commit the guard last (drop = rollback).

### 8.2 Solo profile (embedded single-file DB)

Learned invariants — these are requirements for ANY embedded/single-writer
storage engine, not artifacts of the one used:

- **[INVARIANT] One writer, fair queue.** All server-side statements are
  serialized through a single connection whose acquire queue is FIFO-fair.
  Racing N connections against an embedded store's lock via a polling busy
  handler starves unlucky waiters unboundedly under sustained write
  pressure (observed: multi-minute stalls). The pool/queue IS the write
  serializer; the store's own lock only arbitrates against *other
  processes*.
- **[INVARIANT] Never hold the store's write lock (or the sole connection)
  across a foreign await.** Two concrete bans: (a) partially-consumed
  concurrent query pipelines must be fully drained before post-processing
  that itself touches the store (an un-polled in-flight query future holding
  a connection while the consumer waits on the pool is a deadlock); (b) the
  command-event guard must hold **nothing** while domain mutations run —
  on the embedded backend it is a *deferred insert*: validate + dominance
  pre-check up front (no transaction), buffer the row, and at commit re-run
  the dominance/duplicate checks and insert atomically in one short
  immediate transaction. Losing the commit race = idempotent success,
  matching LWW.
- Write transactions that do open must take the write lock **up front**
  (immediate/exclusive begin) — a deferred read→write upgrade can deadlock
  unresolvably against snapshot isolation.
- Timestamps that participate in content hashes (audit §8.4) must be stored
  at exactly the precision they were hashed at (truncate before hashing).
- WAL-style journaling with a second-writer allowance (external tooling may
  write the same file), `NORMAL` durability, generous busy timeout for
  cross-process contention only.

### 8.3 Search

Interface: community+channel-scoped full-text query with kinds/authors/time
filters, pagination, and relevance order; hits are re-fetched and
**re-authorized** downstream (search never bypasses access control — it
returns ids). Full profile: RDBMS FTS. Solo: embedded FTS index maintained
by triggers on the event table. **[INVARIANT]** User input is never
interpolated into index query syntax — queries are built from sanitized
per-term constructions. Cross-backend parity contract is top-K overlap, not
identical ranking. NIP-50 `search` filters cannot be mixed with non-search
filters in one request.

### 8.4 Audit chain

Per-community append-only log: `seq` (dense), `hash`, `prev_hash`,
actor/action/subject, timestamp. `hash = H(seq ‖ prev ‖ community ‖ actor ‖
action ‖ subject ‖ rfc3339(created_at))`. Appends are strictly serialized
per community; verification refetches and recomputes the whole chain,
reporting first divergence (hash mismatch or chain break). Byte-identical
across backends (see the truncation invariant above).

### 8.5 Media

Content-addressed blobs (SHA-256 key), Blossom-compatible HTTP: upload
requires auth + declared hash; server verifies hash (400 on mismatch),
dedupes idempotently; GET supports ranges; optional auth-gated reads.
Backends: object storage (Full) and local filesystem (Solo) with the **same
key layout**, so migration is a file copy. Upload admission is bounded
(global + per-pubkey concurrency, per-minute rate, all community-scoped).

## 9. Messaging & shared state

Two orthogonal interfaces behind config:

- **PubSub** — topic-based fan-out of accepted events to every node's live
  subscriptions. Topics are community/channel-scoped. Delivery contract is
  **at-most-once** (a disconnected node misses live frames; durable reads
  backfill — clients reconcile via REQ history).
- **SharedState** — presence, typing, per-principal rate-limit windows,
  NIP-98 replay guard, cross-request fences. Contract includes atomic
  check-and-set with TTL.

Backends: shared cache service (Full), in-process (Solo), and a
static-peer mesh transport (each node publishes to all configured peers)
for small clusters — **pub/sub only**. **[INVARIANT] Config validation
rejects in-process SharedState in any multi-node topology** (replay/rate
fences must be shared), so the mesh transport still requires the shared
state service. A `multinode_fanout` counter (events received *from peers*)
is the observable proof the mesh carried traffic; soak-tested: steady-state
bidirectional delivery, peer kill/restart recovery, durability of
outage-window events.

## 10. Capabilities

Optional subsystems are uniform on/off blocks: **off = no state
constructed, no routes mounted, no background queries, kinds refused at
ingest with `restricted:`** and truthful NIP-11. As built: `git`
(smart-HTTP, object store, NIP-34 kinds, repo browser, conformance probe;
default on, off under Solo) and `huddle_audio` (single-node voice relay;
must be off when horizontally scaled). Boolean parsing is strict — an
unrecognized value fails boot rather than silently defaulting.

## 11. Configuration & profiles

Environment-driven; every setting has a canonical name and accepts a
branded alias (`BUZZ_*` / `HIVE_*`; canonical wins when both set).
`--profile solo` / `PROFILE=solo` flips the **defaults** of the backend trio
(storage=embedded, messaging=in-process, media=local-FS), enables
auto-migration, and disables the git capability — every explicit variable
still overrides its profile default. Explicit cross-checks reject
invalid combinations (replicas with embedded storage; mesh without the
shared-state service; in-process state with peers). Boot order: config →
storage (migrate) → messaging/state → capability construction → routers →
background loops (reaper, reminders, usage metrics — DB-derived metrics are
skipped entirely on backends that don't support them, in-memory gauges
always emitted).

## 12. Deployment topologies

1. **Solo**: one binary, `--profile solo`, data under `./data` (DB file +
   media tree). Backup = stop-and-copy or online DB backup API.
2. **Compose Solo**: same, containerized, one volume, healthcheck on
   `/_readiness`.
3. **Full**: relay(s) + RDBMS + shared cache + object storage; horizontal
   scale with huddle off; optional replicas.
4. **Small cluster (mesh)**: N relays + shared RDBMS + shared cache, peered
   fan-out transport; same public host on all nodes (§4).

Ports: one serving port (WS + HTTP), separate health and metrics ports.
Scope cuts on Solo (functional on Full only): mobile push outbox, mesh
session tunneling, read replicas.

## 13. Agent surface

Agents are first-class members: same keypairs, same events, agent-class rate
limits. An **ACP harness** bridges relay events to agent runtimes: managed
subprocesses get credentials injected; default subscriptions wake agents on
mentions, DMs, task assignments, review requests, and handoffs (p-tag
targeting). **Persona packs** are content (workflow instructions per role);
the shipped team covers coordinator, spec-writer, critic, decision-log
maintainer, hardware bring-up, and data-pipeline review — all operating
purely through the CLI.

**CLI contract** (the agent-facing API): reads return sig-stripped JSON
arrays; writes return `{event_id, accepted, message}`; creates add the
entity id; global `--format compact`; exit codes 0 ok / 1 input / 2
network / 3 auth / 4 other / **5 write-conflict (LWW)**. Queries always
pass explicit `kinds` (the p-gate refuses open-ended queries). Verb families:
channels, messages, threads, DMs, uploads, feed, reactions, canvas, repos,
memory, and the workstream family (workstream/task/artifact/review/
decision/handoff/experiment/measure).

## 14. Clients

- **Desktop** (the reference UI): community switching remounts the entire
  scoped subtree via a key change; **[INVARIANT]** every module-level cache
  holding community data must register a reset hook, or stale-tenant data
  leaks. Text sizing must use rem tokens (zoom = root font-size scaling).
  Workstream UI: per-channel list, LWW head-resolution reduce (client-side,
  same tiebreak as server), task board by status, artifact/review flow,
  decision log; same-second edits bump `created_at` monotonically
  (`max(now, prev+1)`) to avoid self-conflicts.
- **Mobile**: same event model, kinds mirrored from the registry.
- **Web**: repo browser served by the relay (capability-gated).

## 15. Quality gates (the operational spec)

- **Solo E2E gate**: boot the Solo profile from nothing; run the full
  protocol suites (relay protocol, interop [search/threads/gift-wraps/
  DM-visibility], media, workstream lifecycle, multi-party review) against
  it. This is the standing proof of the zero-service claim and of
  storage-parity.
- Same suites run against the Full profile in CI (config-only switch, both
  green).
- Backend conformance suite at the interface level for every
  storage/messaging/state backend.
- ZMQ soak harness for the mesh transport (steady-state, kill/restart,
  durability).
- Lint/format/no-panic discipline: no unchecked panics in serve paths;
  every rejection is a typed, prefixed message.

## 16. Notes for a re-implementation

What must survive a stack change: the event model and LWW rules (§2), the
host-derived tenant boundary (§4), the wire grammar and error prefixes (§5),
the kind registry semantics incl. fail-closed unknown kinds (§3, §6), the
counter/audit/search contracts (§8), the messaging/state split and its
multi-node validation rules (§9), the CLI contract (§13), and every
**[INVARIANT]** — especially the embedded-storage concurrency rules (§8.2),
which are properties of single-writer stores generally, not of any one
library. What is freely replaceable: language, web framework, ORM/driver,
the specific RDBMS/FTS/cache/object-store products, the UI toolkit, and the
process supervision story. An embedded client-server RDBMS (managed as a
child process) is a viable alternative Solo storage backend that would
un-gate the Full-only subsystems at the cost of heavier packaging — the
dispatch seam is the designed extension point for that third profile.

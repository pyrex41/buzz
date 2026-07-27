# ADR 0001 — Pluggable storage & messaging: trait rules

**Status:** Accepted
**Context:** [Hive implementation plan](../hive-implementation-plan.md) §3–§4.

Buzz's relay is hard-wired to Postgres (`buzz_db::Db`, 216 public methods),
Redis (`PubSubManager` plus rate-limit/replay/presence key structures), and
S3 (`MediaStorage`). Hive requires backend pluggability with a zero-external-
service default (SQLite + local FS + in-process messaging). This ADR fixes
the design rules the refactor must follow.

## Decision 1 — Trait methods are whole operations that own their transactions

Storage traits (`EventStore`, `ChannelStore`, `AuditStore`, …) expose
domain operations, never storage primitives. No trait method returns or
accepts a database transaction, connection, or backend handle.
`Db::begin_transaction()` and every raw-SQL call site outside the backend
crates are eliminated (`command_executor.rs` was the first, Phase 0.4).
Where a caller must interleave a domain mutation inside an event-persist
window, the backend returns an **opaque guard** (e.g. `CommandEventTx`) whose
only public operation is `commit()` — the transaction type never escapes.

Cross-store invariants that today ride one Postgres transaction become
explicit compound methods on the owning store.

**Consequence:** backends are free to implement atomicity their own way
(Postgres transactions + advisory locks; SQLite single-writer `IMMEDIATE`
transactions + in-process mutexes), and the relay cannot re-couple itself.

## Decision 2 — Backend-internal machinery is invisible

Partition management, replica fencing, GUC floor triggers, advisory locks,
tsvector generated columns, and Lua scripts are Postgres/Redis
implementation details. They are reachable only through neutral lifecycle
hooks (`StoreMaintenance::{migrate, maintenance_tick, try_acquire_singleton}`)
or not at all. A backend that doesn't need them no-ops them.

## Decision 3 — `SharedState` is a separate abstraction from `PubSub`

Redis serves five concerns today: event fan-out, presence TTL keys, atomic
rate-limit windows, the fail-closed NIP-98 replay guard, and the cross-pod
control plane. Fan-out and control plane are message-*transport* concerns
(`PubSub`); presence, rate limiting, and replay are shared-*state* concerns
and get their own trait (`SharedState`: TTL KV, atomic `set_nx_with_ttl`,
`incr_window`). ZeroMQ implements only `PubSub`; it is never asked to hold
state. Single-node deployments use in-process implementations of both,
which is what makes the zero-dependency profile real. Multi-node keeps
Redis for `SharedState` even when transport is ZMQ. Config validation
rejects in-process state on multi-node topologies — the replay guard and
rate limiter fail closed and MUST be shared across nodes.

Existing `buzz-auth` traits (`RateLimiter`, `Nip98ReplayGuard`) keep their
signatures; their impls become thin layers over `SharedState`, preserving
today's Redis key layouts byte-for-byte for rolling-deploy compatibility.

## Decision 4 — Separate migration lineage per storage backend

No `sqlx::Any`, no lowest-common-denominator SQL. Postgres keeps its
existing `migrations/` verbatim; SQLite gets a fresh consolidated schema in
its backend crate. Semantic equivalence between backends is enforced by a
shared behavioral conformance suite (`buzz-backend-conformance`) run against
every backend in CI — not by sharing DDL.

## Decision 5 — Evolve in place; rename last

The refactor is strangler-fig over the existing crates. New abstraction
crates carry neutral `-api` names (`buzz-storage-api`, `buzz-state-api`,
`buzz-messaging-api`); existing crates become backend implementations
without renaming. The Hive branding sweep (crate names, `hive` binary,
`HIVE_*` env aliases) is a single mechanical change at the end (plan
Phase 6), never interleaved with behavioral work.

## Decision 6 — Workstream kind allocations

The Workstream model uses reserved kind blocks (constants landed in
`buzz-core/src/kind.rs`, handlers in Phase 3):

- **35000–35199** (parameterized replaceable): 35000 workstream,
  35001 task head, 35002 artifact head, 35003 decision record.
- **47000–47099** (regular): 47001 task status change, 47002 artifact
  version, 47010–47012 review request/comment/decision, 47020 experiment
  log, 47021 measurement, 47030 handoff.

These ranges collide with nothing in `ALL_KINDS`; 35xxx inherits NIP-33
`d`-tag replace semantics from the existing range predicates unchanged.

# Hive — Detailed Implementation Plan

**Status:** Draft for review
**Scope:** Turning the Buzz codebase into Hive — a self-hostable human + agent
workspace with pluggable storage, ZeroMQ-first messaging, and a general
Workstream model that is not git-centric.

This plan is grounded in a survey of the codebase as it exists today (file and
line references throughout refer to the current tree). It maps the Hive
specification onto the real code, states what must change where, and sequences
the work into phases with concrete exit criteria.

---

## 1. Strategy: evolve, don't rewrite

The specification frames Hive as "fork / new repo." This plan recommends the
**strangler-fig variant**: evolve this codebase in place behind trait seams,
rather than a greenfield rewrite.

Rationale:

- The workspace is ~139k lines of working, tested Rust across 26 crates, with
  a relay (~60k lines) that already implements NIP-01/29/42/98 semantics,
  multi-tenancy, moderation, audit chains, media, git hosting, huddle audio,
  workflows, and an agent harness (`buzz-acp`, ~32k lines). A rewrite discards
  years of protocol edge-case handling that the spec explicitly wants to keep.
- The spec's core demand — "the relay never imports a concrete Postgres or
  Redis client" — is a *seam-insertion* problem, not a rewrite problem. Today
  there are **zero trait abstractions** in `buzz-db`, `buzz-search`,
  `buzz-media`, and `buzz-pubsub` (verified by survey), but the coupling
  points are enumerable and localized (§3).
- Two existing patterns prove the target shape already works here:
  `buzz-auth` defines `RateLimiter` and `Nip98ReplayGuard` as traits whose
  Redis implementations live in `buzz-pubsub`
  (`crates/buzz-auth/src/rate_limit.rs:168`,
  `crates/buzz-auth/src/nip98_replay.rs:63`). That trait-in-core /
  impl-in-backend pattern is exactly what we generalize.
- `buzz-conformance` (a tracer/conformance crate already wired into
  `AppState`) gives us the skeleton of a backend-conformance test suite —
  the thing that makes "switching SQLite ↔ Postgres is config, not code"
  verifiable rather than aspirational.

**Branding:** keep `buzz-*` crate names during the refactor. Renaming 26
crates mid-refactor doubles every diff for zero functional gain. New
abstraction crates get neutral names (`buzz-storage-api`, `buzz-state`,
`buzz-messaging-api`). A dedicated rename/branding sweep (crate names, binary
name `hive`, env prefix `HIVE_` with `BUZZ_` aliases) is Phase 6 — one
mechanical PR at the end, when the churn is over.

---

## 2. Current-state assessment (survey findings)

### 2.1 Where the coupling actually lives

| Concern | Concrete type today | Coupling severity |
|---|---|---|
| Event store | `buzz_db::Db` — one struct, **216 public methods**, holds `PgPool` + optional read-replica pool + `ReplicaFence` (`crates/buzz-db/src/lib.rs:169-188`) | Severe — see below |
| Search | `SearchService { pool: PgPool }` (`crates/buzz-search/src/lib.rs:35-54`); *indexing is not in this crate at all* — it is the `events.search_tsv` generated tsvector column owned by buzz-db's insert SQL | Moderate |
| Media | `MediaStorage { bucket: Box<s3::Bucket> }` (`crates/buzz-media/src/storage.rs:19-21`), ~12 methods; **no local-FS backend exists** (MinIO is the "local" story) | Low — well-bounded surface |
| Pub/sub + shared state | `PubSubManager` (`crates/buzz-pubsub/src/lib.rs:99-367`), Redis hard dependency, no feature flag | Moderate — but it's five concerns in one (§2.3) |
| Audit | `AuditService::new(pool: PgPool)` (`crates/buzz-audit/src/service.rs:34`), per-community hash chain serialized by a Postgres advisory lock | Moderate |
| Auth | `AuthService` — stateless crypto (NIP-42/NIP-98), no storage. Depends on Redis only via the two trait impls above | None (already clean) |
| Config | Env vars read directly via `std::env::var` in `crates/buzz-relay/src/config.rs` (1,307 lines); no config file | Low |

### 2.2 Postgres-specific machinery inside `buzz-db`

These are the things a second backend must reimplement, replace, or no-op —
each gets an explicit strategy in §4:

- **Advisory locks** — `pg_try_advisory_lock` for usage-metrics leader
  election (`lib.rs:522`), `pg_advisory_xact_lock` for per-owner community
  creation (`lib.rs:872`) and event replacement, shared locks in migrations.
- **Monthly range partitioning** of `events` on `created_at`
  (`partition.rs`, `ensure_future_partitions` at `lib.rs:2802`).
- **Replica fencing** — writer→replica LSN probing (`replica_fence.rs`) plus
  a session GUC (`SET buzz.created_at_floor`, `lib.rs:391`) enforced by a
  deferred constraint trigger (migration 0021).
- **Generated tsvector column + GIN index** — FTS indexing *is* the row
  insert; `buzz-search` only queries.
- **`jsonb`, `= ANY($1)`, `ON CONFLICT` with `xmax = 0` insert-detection.**
- **Leaked transaction type** — `Db::begin_transaction()` returns
  `sqlx::Transaction<'static, sqlx::Postgres>` publicly (`lib.rs:648`), and
  the relay's `handlers/command_executor.rs:163-224` writes **raw SQL**
  (including `pg_advisory_xact_lock`) against that transaction;
  `PersistResult::Inserted` carries the live transaction
  (`command_executor.rs:84`). This is the single worst abstraction leak in
  the codebase and the first thing Phase 2 fixes.
- **24 Postgres-only migrations** at `migrations/` (embedded via
  `sqlx::migrate!`, count asserted in tests), full of PL/pgSQL, triggers,
  partition DDL. A second push-gateway migration set lives in
  `crates/buzz-push-gateway/migrations/`.

### 2.3 Redis is five concerns wearing one trench coat

`buzz-pubsub` uses Redis for five semantically distinct jobs. This distinction
drives the whole messaging design, because **ZeroMQ is a pure transport — it
can replace (1) and (5), and cannot replace (2)–(4)**:

1. **Event fan-out** — `PUBLISH`/`SUBSCRIBE` on
   `buzz:{community}:channel:{uuid}` / `buzz:{community}:global`
   (`topic.rs:43-50`); relay consumes via a `broadcast::Receiver` stream loop
   (`crates/buzz-relay/src/main.rs:817-843`).
2. **Presence** — `SET … EX 90` TTL keys (`presence.rs`).
3. **Rate limiting** — atomic Lua `INCR`+`EXPIRE` fixed windows
   (`rate_limiter.rs:24-31`).
4. **NIP-98 replay guard** — `SET … NX EX` set-if-absent
   (`nip98_replay.rs:66-72`), **fail-closed** by contract.
5. **Cross-pod control plane** — cache-invalidation and conn-control
   pub/sub (`cache_invalidation.rs`, `conn_control.rs`).

Additionally, **outside** buzz-pubsub, the mesh/tunnel layer
(`crates/buzz-relay/src/tunnel/directory.rs`, `tunnel/reliable.rs`,
`mesh_boot.rs`) runs raw `redis::cmd`/`redis::Script` for Redis-fenced
cross-relay session ownership, and `AppState` holds the raw
`deadpool_redis::Pool` directly (`state.rs`). The relay also creates two
direct `PgPool`s outside `Db` (audit at `main.rs:322-334`, search at
`main.rs:374-386`).

Redis is effectively **mandatory today**: the pool, `PubSubManager`,
`RedisRateLimiter`, and `RedisNip98ReplayGuard` are non-optional `AppState`
fields, and replay/rate-limit paths fail closed when Redis is down.

### 2.4 What is already in good shape

- `buzz-core` is genuinely zero-I/O: ~125 kind constants in `kind.rs` with
  range predicates, `verify_event`, `filters_match`, tenant types. This is
  the spec's `hive-core`, already built.
- `EventQuery` / `SearchQuery` / `ChannelScope` / `ByteStream` /
  `BlobMeta` / `bucket_index` are already backend-neutral DTOs and pure
  functions — natural trait vocabulary.
- Search results are **re-fetched and re-authorized** through buzz-db
  (`api/bridge.rs:1594`) — search is never the access boundary. This means a
  `SearchIndex` backend only has to be *approximately* right; authz doesn't
  depend on it. Great property for pluggability.
- Media is content-addressed (SHA-256 at `upload.rs:83`) with per-community
  sidecar read gates — the storage layout ports to a local filesystem
  directly.
- Git hosting (~8.6k lines under `crates/buzz-relay/src/api/git/`) is
  already route-isolated (`git_router`) and kind-isolated (NIP-34 kinds
  30617/30618, 1617–1633 at `kind.rs:468-487`) — making it an optional
  capability is a gating problem, not a surgery problem.

---

## 3. Target architecture

```
                 ┌───────────────────────────────────────────────┐
                 │                buzz-relay                     │
                 │   (axum, handlers, orchestration — imports    │
                 │    ONLY the *-api trait crates below)         │
                 └──┬──────────┬──────────┬──────────┬──────────┘
                    │          │          │          │
        ┌───────────▼──┐ ┌─────▼─────┐ ┌──▼───────┐ ┌▼──────────────┐
        │ buzz-storage-api│ buzz-messaging-api│ buzz-state-api │ buzz-media-api │
        │ EventStore   │ │ PubSub    │ │ SharedState│ │ MediaStore   │
        │ SearchIndex  │ │           │ │ (KV+TTL)  │ │              │
        │ AuditStore   │ │           │ │           │ │              │
        └───┬──────┬───┘ └──┬────┬───┘ └──┬────┬──┘ └──┬───────┬───┘
            │      │        │    │        │    │       │       │
        postgres sqlite   inproc zmq   inproc redis  local-fs  s3
        (existing) (new)  (new) (new)  (new) (adapter) (new) (existing)
```

Three deployment profiles fall out of backend selection:

| Profile | EventStore | Search | Media | PubSub | SharedState | External deps |
|---|---|---|---|---|---|---|
| **Solo** (default) | SQLite | SQLite FTS5 | Local FS | In-process | In-process | **none** |
| **Small cluster** | Postgres | Postgres FTS | S3/MinIO | ZeroMQ | Redis (or single "state leader" ZMQ REQ/REP — deferred) | PG (+ S3, Redis) |
| **Production (today's)** | Postgres | Postgres FTS | S3 | Redis (adapter) | Redis | PG, Redis, S3 |

Key point the spec's §4.1 sketch missed and the survey surfaced: pub/sub alone
is not enough. A fourth trait, **`SharedState`** (KV + TTL + atomic
set-if-absent + windowed counters), is required because presence, replay
guards, and rate limits are shared-*state* problems, not message-*transport*
problems. Single-node gets an in-process implementation (making the "zero
external services" goal real); multi-node keeps Redis for state even when
transport is ZMQ. This is honest about ZMQ's limitations rather than
pretending PUB/SUB covers everything.

### 3.1 Trait definitions (concrete, derived from real call sites)

New crate `buzz-messaging-api` (trait + DTOs only; today's `EventTopic`,
`EventTopicKey`, `ChannelEvent`, `CacheInvalidation`, `ConnControl` move
here from buzz-pubsub):

```rust
#[async_trait]
pub trait PubSub: Send + Sync {
    /// Fan out an event to a community-scoped topic. Fire-and-forget
    /// durability: events are already durable in the EventStore.
    async fn publish_event(&self, ctx: &TenantContext, topic: EventTopic,
                           event: &nostr::Event) -> Result<()>;
    /// Local delivery stream (replaces PubSubManager::subscribe_local).
    fn subscribe_local(&self) -> broadcast::Receiver<ChannelEvent>;
    /// Dynamic topic interest refcounting (replaces retain/release_topic).
    async fn retain_topic(&self, ctx: &TenantContext, topic: EventTopic) -> Result<()>;
    async fn release_topic(&self, ctx: &TenantContext, topic: EventTopic) -> Result<()>;
    // Control plane (cache invalidation + conn control), same shapes as today.
    async fn publish_cache_invalidation(&self, ctx: &TenantContext,
                                        msg: CacheInvalidation) -> Result<()>;
    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<(CommunityId, CacheInvalidation)>;
    async fn publish_conn_control(&self, ctx: &TenantContext, msg: ConnControl) -> Result<()>;
    fn subscribe_conn_control(&self) -> broadcast::Receiver<(CommunityId, ConnControl)>;
    /// Background driver (replaces the three run_* loops). Implementations
    /// that need no socket pump (in-process) return a future that never resolves.
    async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()>;
}
```

New crate `buzz-state-api`:

```rust
#[async_trait]
pub trait SharedState: Send + Sync {
    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()>;
    /// Atomic set-if-absent with TTL. Backs the NIP-98 replay guard —
    /// implementations MUST be atomic; callers fail closed on Err.
    async fn set_nx_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Fixed-window counter: INCR + set-expiry-on-first-increment, atomic.
    /// Backs rate limiting. Returns (count, remaining_window).
    async fn incr_window(&self, key: &str, window: Duration) -> Result<(u64, Duration)>;
}
```

Presence, the `Nip98ReplayGuard` impl, and the `RateLimiter` impl become thin
libraries **over** `SharedState` (they keep today's key layouts so the Redis
backend is wire-compatible with running deployments). The existing
`buzz-auth` traits stay; only their impls move.

New crate `buzz-storage-api` — this is where restraint matters. We do **not**
define a 216-method mega-trait. `Db` is split along its existing module seams
(`event.rs`, `channel.rs`, `moderation.rs`, `push.rs`, `workflow.rs`, …, each
already a submodule of buzz-db) into **domain repository traits**:

```rust
pub trait EventStore: Send + Sync {      // core NIP-01/29/33 event lifecycle
    async fn persist_event(&self, ctx: &TenantContext, cmd: PersistEventCmd)
        -> Result<PersistOutcome>;       // encapsulates today's command_executor
                                         // transaction + locking + counters
    async fn query_events(&self, ctx: &TenantContext, q: &EventQuery) -> Result<Vec<StoredEvent>>;
    async fn count_events(&self, ctx: &TenantContext, q: &EventQuery) -> Result<u64>;
    async fn get_event_by_id(&self, ctx: &TenantContext, id: &EventId) -> Result<Option<StoredEvent>>;
    async fn soft_delete_event(&self, ...) -> Result<...>;
    // replaceable/addressable semantics (NIP-16/33) as whole operations,
    // never as exposed transactions
}
pub trait ChannelStore: Send + Sync { /* channels, membership, roles */ }
pub trait CommunityStore: Send + Sync { /* tenancy, allowlist, NIP-43 */ }
pub trait ModerationStore: Send + Sync { ... }
pub trait WorkflowStore: Send + Sync { ... }
pub trait PushStore: Send + Sync { ... }        // Postgres-only in v1 (see §5.4)
pub trait AuditStore: Send + Sync { /* append(NewAuditEntry), verify_chain, get_entries */ }
pub trait SearchIndex: Send + Sync {
    /// Explicit indexing hook. Postgres impl is a no-op (generated column
    /// does it in the insert); SQLite impl writes the FTS5 shadow table.
    async fn index_event(&self, ctx: &TenantContext, ev: &StoredEvent) -> Result<()>;
    async fn remove_event(&self, ctx: &TenantContext, id: &EventId) -> Result<()>;
    async fn search(&self, q: &SearchQuery) -> Result<SearchResult>;
}
pub trait MediaStore: Send + Sync {
    // The ~12 methods of today's MediaStorage: put, put_file, get, get_range,
    // get_stream, head, head_with_metadata, delete, list_page + sidecar ops.
}
```

**Transaction rule (the load-bearing design decision):** trait methods are
*whole operations*, transactional inside the backend. `begin_transaction()`
disappears from the public surface. The relay's `command_executor.rs` raw SQL
(advisory lock + insert + counter updates) moves into the Postgres backend as
the body of `persist_event`; the SQLite backend implements the same contract
with its own mechanics (§4.1). Cross-store invariants that today ride one
Postgres transaction get explicit compound methods on the owning store rather
than cross-trait transactions.

**Backend-internal concerns get lifecycle hooks, not trait methods:**

```rust
pub trait StoreMaintenance: Send + Sync {
    async fn migrate(&self) -> Result<()>;
    async fn maintenance_tick(&self) -> Result<()>;  // PG: ensure_future_partitions,
                                                     // fence probes; SQLite: WAL checkpoint,
                                                     // PRAGMA optimize / incremental vacuum
    async fn try_acquire_singleton(&self, role: &str) -> Result<Option<SingletonLease>>;
                                                     // PG: advisory lock (usage-metrics leader);
                                                     // SQLite: always Some (single node)
}
```

Replica fencing, partitioning, GUC triggers, and advisory locks thereby become
invisible Postgres implementation details — exactly where they belong.

### 3.2 Crate layout after the refactor

```
crates/
  buzz-core             # unchanged role: types, kinds, verification (zero I/O)
  buzz-storage-api      # NEW: EventStore/ChannelStore/.../SearchIndex/AuditStore traits + DTOs
  buzz-state-api        # NEW: SharedState trait
  buzz-messaging-api    # NEW: PubSub trait + topic/control DTOs
  buzz-media-api        # NEW (or folded into storage-api): MediaStore trait
  buzz-db               # BECOMES: Postgres impl of storage traits (keeps name until Phase 6)
  buzz-store-sqlite     # NEW: SQLite impl (rusqlite or sqlx-sqlite) + FTS5 + sqlite audit chain
  buzz-pubsub           # BECOMES: Redis impls of PubSub + SharedState (wire-compatible)
  buzz-messaging-zmq    # NEW: ZeroMQ PubSub impl
  buzz-messaging-inproc # NEW: in-process PubSub + SharedState (tokio broadcast + DashMap/TTL)
  buzz-media            # media pipeline (validation/thumbnail/upload) over MediaStore;
                        #   s3 backend stays here, local-fs backend added
  buzz-backend-conformance  # NEW: shared test suite run against every backend pair
  ... (all other crates unchanged)
```

### 3.3 Relay wiring changes

`AppState` (`crates/buzz-relay/src/state.rs:432-574`) changes from concrete
types to trait objects:

- `db: Db` → `stores: Stores` (a small struct of `Arc<dyn EventStore>`,
  `Arc<dyn ChannelStore>`, … so handlers name only what they use).
- `pubsub: Arc<PubSubManager>` → `pubsub: Arc<dyn PubSub>`.
- `redis_pool: deadpool_redis::Pool` → **removed from AppState.** The only
  consumers besides pubsub are the mesh/tunnel layer and readiness checks;
  mesh keeps its own pool behind a `mesh` feature/config gate (§5.4),
  readiness delegates to backend `ping()`s.
- `media_storage: Arc<MediaStorage>` → `Arc<dyn MediaStore>`.
- `search: Arc<SearchService>` → `Arc<dyn SearchIndex>`.
- `audit: Option<Arc<AuditService>>` → `Option<Arc<dyn AuditStore>>`.
- The two stray `PgPool`s built in `main.rs` (audit, search) disappear —
  backends own their connections.

Composition root: `main.rs` grows a single `build_backends(&config) ->
Backends` function — the **only** place in the relay that names concrete
backend types, selected by config (§3.4). Static dispatch purists note:
`Arc<dyn Trait>` costs one vtable hop on paths that then do network/disk I/O;
this is noise. We do not generic-parameterize `AppState` — the compile-time
and code-churn cost is not worth it.

### 3.4 Configuration

Adopt layered config: **TOML file + env override** (figment), preserving every
existing `BUZZ_*`/`DATABASE_URL` env var as an override for deployment
compatibility. New file, matching the spec's shape:

```toml
# hive.toml (all sections optional; defaults = Solo profile)
[storage]
backend = "sqlite"              # "sqlite" | "postgres"
path = "./data/hive.db"        # sqlite
# database_url / read_database_url   # postgres

[search]
backend = "auto"                # follows storage backend by default

[media]
backend = "local"               # "local" | "s3"
path = "./data/media"

[messaging]
backend = "inproc"              # "inproc" | "zmq" | "redis"
# zmq_bind = "tcp://0.0.0.0:5555"   # or ipc:///run/hive/pubsub.ipc
# zmq_peers = ["tcp://host2:5555"]  # static peer list for small clusters

[state]
backend = "inproc"              # "inproc" | "redis"
```

Startup validation enforces coherent combinations (e.g. `messaging = "zmq"`
with `state = "inproc"` is rejected on multi-node configs; sqlite + multiple
relay processes is rejected).

---

## 4. Backend designs

### 4.1 SQLite EventStore (`buzz-store-sqlite`)

Design decisions, mapped one-to-one against the Postgres machinery in §2.2:

| Postgres mechanism | SQLite strategy |
|---|---|
| Monthly partitions on `events` | Single `events` table + composite indexes `(community_id, created_at)`, `(community_id, kind, created_at)`. Partitioning exists for PG-scale retention; SQLite targets solo/small usage. TTL reaping = indexed `DELETE` in `maintenance_tick`. |
| Advisory locks (event replacement, community create, audit chain) | Single-process by definition ⇒ in-process `tokio::sync::Mutex` keyed maps + SQLite's single-writer semantics. `try_acquire_singleton` always succeeds. |
| Replica fence / LSN probes / GUC floor trigger | Not applicable — no replicas. `created_at` floor enforced in Rust in the insert path (shared validation helper so both backends run identical checks). |
| Generated tsvector + GIN | FTS5 **external-content** table (`content=events`) maintained through the `SearchIndex::index_event`/`remove_event` hooks called inside `persist_event`/delete. Query via `bm25()` ranking; prefix mode via FTS5 `prefix=` indexes — mirroring today's FullText/Prefix `SearchMode`. |
| `jsonb` columns (`scopes`, `channel_ids`, tags) | `TEXT` + JSON1 functions; tag-query hot path gets an explicit `event_tags(event_id, name, value)` side table (replacing the GIN-on-jsonb trick) — this is also what makes `#h`/`#p` filters indexable. |
| `= ANY($1)` | rusqlite `carray` / dynamic `IN` lists. |
| `ON CONFLICT … xmax = 0` insert-detection | `INSERT … ON CONFLICT DO UPDATE … RETURNING` + `changes()` bookkeeping (SQLite supports upsert; the "was it an insert" bit is derivable). |
| PL/pgSQL functions, triggers | Logic moves into the Rust operation bodies (preferred over SQLite triggers — keeps both backends' logic reviewable in one language). |
| 24 PG migrations | **Separate migration lineage per backend.** No `sqlx::Any` and no lowest-common-denominator SQL: PG keeps its migrations verbatim; SQLite gets a fresh consolidated `0001` schema. The conformance suite (§7) is what keeps them semantically aligned, not shared DDL. |

Concurrency: WAL mode, one writer connection behind an async mutex, a small
read pool, `busy_timeout` set. Every trait operation is a single `IMMEDIATE`
transaction — which is precisely why the trait must own transactions (§3.1).

Driver: `rusqlite` (via `tokio::task::spawn_blocking` workers) rather than
sqlx-sqlite — FTS5, `carray`, and fine PRAGMA control matter more than
keeping one driver family, and buzz-db already uses runtime queries (no
compile-time query macros to preserve).

**Scope control:** the SQLite backend implements the traits needed by the
relay's serve path (events, channels, communities, membership, DMs, threads,
reactions, moderation, workflows, tokens, audit, search). It does **not**
implement `PushStore` or mesh-fencing in v1 (§5.4) — those subsystems are
config-gated to the Postgres/Redis profile.

### 4.2 ZeroMQ PubSub (`buzz-messaging-zmq`)

- Crate: `zeromq` (pure-Rust, async-native). Fallback if conformance/soak
  tests find gaps: `async_zmq` over libzmq — the trait insulates this choice.
- **Single node:** the in-process backend is the default; ZMQ enters for
  multi-process single-host (`ipc://`) and small clusters (`tcp://`).
- **Topology (v1): static full mesh.** Each relay node runs one `PUB` socket
  bound at `zmq_bind` and one `SUB` socket connecting to every entry in
  `zmq_peers`; a node also loops its own publishes back locally (in-process
  short-circuit, so single-node ZMQ needs no self-connection). Topic frames
  reuse today's Redis channel-name strings verbatim
  (`buzz:{community}:channel:{uuid}`, `…:global`, `…:cache-invalidate`,
  `…:conn-control`) — ZMQ prefix subscription matches them for free, and
  the control plane rides the same socket with no extra machinery.
- Subscription management: ZMQ SUB-side filtering maps directly onto
  today's `retain_topic`/`release_topic` refcounting; the local
  `DesiredTopics` map logic moves into the backend unchanged.
- Delivery semantics: at-most-once, ephemeral — same contract as Redis
  pub/sub today (events are durable in the EventStore; a lagged consumer
  already handles `Lagged` on the broadcast channel). No durability work
  needed, and none promised.
- Reconnect/backoff: ZMQ sockets auto-reconnect; the `run()` driver adds
  liveness logging and a peer-health gauge mirroring today's
  reconnect-with-backoff loops (`lib.rs:148-180`).
- Explicit non-goals v1 (documented in the crate): dynamic peer discovery,
  broker mode (XPUB/XSUB proxy), CURVE encryption. Small-cluster operators
  list peers in config; clusters that outgrow static lists use the Redis
  backend until a discovery helper ships (Phase 5).

### 4.3 In-process backends (`buzz-messaging-inproc`)

- `PubSub`: `tokio::sync::broadcast` — this is nearly free, because the
  relay already consumes fan-out via `broadcast::Receiver`
  (`subscribe_local`); the in-process impl just skips the network round-trip.
- `SharedState`: sharded `DashMap` with expiry wheel for TTL, atomic
  entry-API for `set_nx_with_ttl` and `incr_window`. Semantics tested by the
  same conformance suite as the Redis impl (fail-closed contract, window
  boundaries, TTL clamps `[120s, 3600s]` for replay keys).

### 4.4 Local-FS MediaStore

Layout mirrors today's S3 key scheme exactly (`bucket_index.rs:15-19` is
already a pure fold over `(key, size)` pairs and works unchanged):

```
data/media/
  blobs/{sha256}.{ext}
  blobs/{sha256}.thumb.jpg
  _meta/{community}/{sha256}.json      # sidecar read-gate
  _uploads/{community}/{sha256}/{event_id}.json
```

Writes are tmp-file + atomic rename; range reads via seek; `list_page` via
ordered directory walk. The existing upload pipeline (validation, thumbnail,
blurhash) is backend-agnostic already and moves unmodified onto the trait.

### 4.5 Audit chain portability

The hash chain itself (`compute_hash`, canonical JSON, genesis hash —
`crates/buzz-audit/src/hash.rs:19-46`) is pure and backend-neutral; only two
things are Postgres-specific: the `audit_log` DDL (owned by relay migration
0001) and the per-community `pg_advisory_lock` serializing appends
(`service.rs:44`). `AuditStore::append` moves serialization behind the trait:
PG keeps the advisory lock; SQLite uses the single-writer mutex. `verify_chain`
is shared code over `get_entries`.

---

## 5. Workstreams, kinds, and de-centering git

### 5.1 Kind allocation

Following the repo convention (flat `pub const KIND_*: u32` in
`buzz-core/src/kind.rs`, registered in `ALL_KINDS`, uniqueness-tested).
Chosen to avoid every occupied range (9xxx, 13xxx, 2xxxx, 30xxx-in-use,
4000x–46xxx, 48xxx, 49001):

**Addressable (NIP-33 parameterized-replaceable, `d`-tag identified) —
block 35000–35199 (currently unused):**

| Kind | Name | Notes |
|---|---|---|
| 35000 | `KIND_WORKSTREAM` | The container. `d` = workstream id; tags: `ws-type` (see §5.2), `h` links to its channels, `p` members/agents, status |
| 35001 | `KIND_WORKSTREAM_TASK` | `d` = task id; `a`-tag → parent workstream; status/assignee/due tags; LWW edits via NIP-33 replace (relay already implements `replace_parameterized_event`, `lib.rs:3628`) |
| 35002 | `KIND_ARTIFACT` | Artifact *head*: name, type (doc/design/dataset/measurement/BOM/sim-result), current-version pointer, media `x` (sha-256) refs into MediaStore |
| 35003 | `KIND_DECISION_RECORD` | Lightweight ADR; `a` → workstream; supersedes-tag chain |

**Regular (immutable, append-only) — block 47000–47099 (currently unused;
sits beside workflow 46xxx and below audit 48001):**

| Kind | Name | Notes |
|---|---|---|
| 47001 | `KIND_TASK_STATUS_CHANGE` | `a` → task; the task head is LWW, changes are the history |
| 47002 | `KIND_ARTIFACT_VERSION` | `a` → artifact head; content hash, changelog; media blob refs |
| 47010 | `KIND_REVIEW_REQUEST` | `a` → any artifact/task/decision — **generic review, not git-specific** |
| 47011 | `KIND_REVIEW_COMMENT` | threads via NIP-10 marks, like existing comments |
| 47012 | `KIND_REVIEW_DECISION` | approve / request-changes / reject |
| 47020 | `KIND_EXPERIMENT_LOG` | free-form structured log entry; `a` → workstream |
| 47021 | `KIND_MEASUREMENT` | unit/value/series tags for hardware & data work |
| 47030 | `KIND_HANDOFF` | cross-functional baton pass: from-`p`, to-`p`, checklist payload |

All of these ride the existing machinery for free: NIP-29 `h` scoping,
tenant isolation, filter matching, FTS (content indexed like any event),
audit, fan-out, and the HTTP bridge (`POST /events|/query|/count`) — the
"prefer Nostr events over new HTTP endpoints" rule holds; **zero new HTTP
endpoints** are needed for the entire Workstream model.

### 5.2 Workstream types

`ws-type` tag vocabulary (validated relay-side, extensible):
`code`, `systems`, `hardware`, `data`, `design`, `process`, `docs`,
`general`. Type selects client presentation and default agent-persona pack,
**not** relay behavior — the relay treats all workstreams uniformly. A `code`
workstream may additionally carry `repo` tags binding NIP-34 git kinds and
repo routes to it.

### 5.3 Git as an optional capability

Already isolated; make it configurable:

- `[capabilities] git = false` (default **on** for compatibility; the Solo
  quickstart template ships it off) gates: mounting `git_router` +
  `git_policy_router` (`router.rs:135-141`), `GitStore`/`GitPackCache`
  construction in `AppState::new` (`state.rs:640-647`), acceptance of NIP-34
  kinds (rejected with an OK-false machine-readable reason when off), and the
  git usage-metrics queries.
- `AppState` git fields become `Option<GitCapability>` — one struct so a
  single `if let` gates everything.
- Same pattern, same phase, for `huddle_audio` (already has
  `BUZZ_HUDDLE_AUDIO_AVAILABLE`) — capabilities become a uniform config block.

### 5.4 Explicit v1 scope cuts (Solo profile)

These subsystems remain functional on the Postgres/Redis profile but are
**config-gated off** in Solo, keeping Phase 2 tractable:

- **Mesh / tunnel / pair-relay** (`buzz-relay-mesh`, `tunnel/*`,
  `mesh_boot.rs`): inherently multi-relay, Redis-fenced session ownership
  (raw `redis::Script`). Solo = single relay ⇒ nothing to mesh. Stays
  Redis-only, behind config.
- **Push gateway** (`buzz-push-gateway`, `PushStore`, push lease/wake-outbox
  machinery — the heaviest PG-specific module in buzz-db): mobile push for a
  solo localhost deployment is a non-goal. Postgres-profile only in v1;
  revisit in Phase 5.
- **Read replicas / replica fence**: Postgres-only by nature.

---

## 6. Agent surface

Minimal changes — the survey confirms the agent stack is already
storage-agnostic (buzz-acp talks WebSocket + CLI, never the database):

- `buzz-acp`, `buzz-agent`, `buzz-dev-mcp`, `sprig` work unchanged once the
  relay serves the new kinds.
- `buzz-sdk` + `buzz-cli` gain typed builders/subcommands for §5.1 kinds
  (`buzz workstream create|list`, `buzz task …`, `buzz artifact …`,
  `buzz review …`, `buzz decision …`) — per repo convention, CLI first, then
  `client.rs` wiring.
- `buzz-persona` gains non-code persona packs (spec-writer/critic,
  decision-log maintainer, cross-functional coordinator, hardware bring-up,
  data-pipeline reviewer). Personas are content, not code — cheap to add,
  high demo value.
- Desktop: Workstream navigation UI (list/create, task board, artifact list
  with review flow, decision log). This is the largest client work item and
  the core of "non-software engineers feel at home."

---

## 7. Testing strategy

1. **Backend conformance suite** (`buzz-backend-conformance`, new) — the
   centerpiece. One suite of trait-level behavioral tests (event lifecycle
   incl. replaceable/addressable LWW + conflict codes, `h`-tag scoping,
   tenant isolation, filter semantics, counter materialization
   (`reply_count`/`descendant_count`), audit chain verification, search
   modes, media round-trip + range reads + sidecar gating, pub/sub fan-out
   ordering-per-topic, `SharedState` atomicity/TTL) executed against every
   backend: `postgres`, `sqlite`, `inproc`, `zmq`, `redis`, `local-fs`, `s3`.
   Seeded from buzz-db's existing integration tests, which already encode
   the semantics — the work is porting assertions from SQL-level to
   trait-level.
2. **Existing integration/E2E suites parameterized by profile** — CI matrix
   runs `buzz-test-client` E2E against a Solo-profile relay (no services!)
   and the Postgres/Redis-profile relay. Solo E2E in CI is the standing
   proof of the zero-dependency claim.
3. **Property tests** where semantics are subtle: filter matching (exists in
   buzz-core, extend), FTS parity (same corpus, same query ⇒ overlapping
   top-K between FTS5 and PG FTS), `incr_window` boundary behavior.
4. **ZMQ soak/chaos** (Phase 5, when multi-node lands): peer kill/restart,
   slow-subscriber backpressure, partition during fan-out — asserting the
   documented at-most-once contract and EventStore-backed recovery.
5. Quality gates unchanged: `just ci` (fmt + clippy + tests + builds); no
   `unsafe`; no new `unwrap()/expect()` in production paths — trait
   signatures are `Result`-first specifically to honor this.

---

## 8. Phased roadmap

Sizing assumes 1–2 experienced engineers plus agent assistance; ranges are
calendar, not effort. Every phase ends green on `just ci` + conformance.

### Phase 0 — Seams & scaffolding (1–2 weeks)
1. Land this plan + an ADR for the trait/transaction rules (§3.1) and the
   SharedState split (§2.3) — `docs/` + `KIND` allocations reserved in
   `kind.rs` as consts (no handlers yet).
2. Create `buzz-storage-api`, `buzz-state-api`, `buzz-messaging-api`,
   `buzz-backend-conformance` crates (traits + DTO moves, no behavior change).
3. Config loader: figment TOML + env-alias layer; relay boots from
   `hive.toml` or pure env exactly as today.
4. **Kill the worst leak first:** move `command_executor.rs` raw SQL into
   buzz-db as `persist_event` (still concrete `Db`, no trait yet). This is
   independently valuable and de-risks everything after.
   - Exit: no `sqlx` import in `buzz-relay` outside `#[cfg(test)]`; no
     behavior change; CI green.

### Phase 1 — Messaging + state pluggability, Redis optional (3–4 weeks)
1. Extract `PubSub` trait; adapt `PubSubManager` into the Redis impl
   (wire-compatible topics — rolling deploys unaffected).
2. `SharedState` trait; re-home presence/rate-limit/replay impls over it
   (Redis impl keeps exact key layouts and Lua/NX semantics).
3. In-process backends for both; `build_backends()` composition root;
   `redis_pool` leaves `AppState`; mesh + push gated by config (§5.4).
4. ZMQ PubSub backend (single-node ipc/tcp first; static peer mesh works but
   is labeled experimental until Phase 5 soak).
5. Conformance suite covers PubSub + SharedState across all three backends.
   - Exit: **relay runs with Redis absent** (inproc profile) passing the
     full E2E suite; Redis profile passes unchanged; ZMQ profile passes
     single-node E2E.

### Phase 2 — Storage pluggability + SQLite Solo profile (6–9 weeks; the long pole)
1. Split `Db` behind the domain-store traits (§3.1), Postgres impl first —
   mechanical but wide (216 methods to classify: core-path traits vs.
   PG-profile-only traits vs. internal). Relay compiles against traits only.
2. `SearchIndex` with explicit `index_event` hook (PG no-op); `AuditStore`;
   `MediaStore` trait + local-FS backend.
3. `buzz-store-sqlite`: consolidated schema, event core, channels/membership,
   DMs/threads/reactions, moderation, workflows, tokens, audit chain, FTS5.
4. Conformance suite extended to every storage trait; CI matrix gains the
   Solo profile end-to-end.
5. Single-binary polish: `buzz-relay --profile solo` (or bare `hive` after
   Phase 6) boots with zero services, auto-creating `./data`.
   - Exit: spec §14 metrics 2 & 3 — zero-service startup; SQLite⇄Postgres
     switch is config-only, proven by the same E2E suite passing on both.

### Phase 3 — Workstream model (3–4 weeks)
1. Kinds from §5.1 wired: validation, `h`/`a` scoping rules, LWW conflict
   codes (exit-code-5 convention), counters where relevant.
2. `buzz-sdk` builders + `buzz-cli` subcommands + docs.
3. Git & huddle behind the `[capabilities]` block (§5.3).
4. Desktop: Workstream list/detail, task board, artifact + review flow,
   decision log (largest client item; can overlap Phase 2 once kinds land
   in Phase 3.1 since clients only need a running relay).
   - Exit: spec §14 metric 1 — a non-code user creates a workstream,
     attaches artifacts, runs a review cycle, never touches git; git-off
     relay passes E2E.

### Phase 4 — Agents for non-code domains (2–3 weeks)
1. Persona packs (§6); ACP harness subscription defaults for workstream
   kinds; agent membership surfaced in Workstream UI.
2. Multi-agent E2E: human + spec-writer + critic complete a review cycle in
   a `hardware` workstream (extends `TESTING.md` scenarios).

### Phase 5 — Production hardening & multi-node ZMQ (3–5 weeks)
1. ZMQ static-mesh soak/chaos (§7.4); peer-health metrics; discovery helper
   if warranted.
2. Decide small-cluster SharedState story (Redis vs. state-leader) from
   real demand; revisit push-gateway on Solo.
3. Packaging: single-binary release, Docker Compose profiles, example
   deployments (solo laptop / homelab ZMQ pair / k8s Postgres+Redis),
   operator docs.

### Phase 6 — Branding sweep (1 week, mechanical)
1. Crate renames `buzz-*` → `hive-*` where desired, binary `hive`,
   `HIVE_*` env with `BUZZ_*` aliases retained one release, docs pass.
   Deliberately last: churn-free window, single reviewable PR.

Dependency notes: Phase 1 and Phase 2.1–2.2 can proceed in parallel (different
crates); Phase 3 desktop work overlaps Phase 2 tail; Phases 4–5 are
independent of each other.

---

## 9. Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| `Db` split (216 methods) balloons | High | Phase-2 classification step first; PG-profile-only subsystems (push, mesh, replicas) stay concrete behind config gates — only the serve path gets traits in v1 |
| Hidden cross-domain transaction couplings surface during split | High | Phase 0.4 removes the known one (command_executor); compound trait methods rule (§3.1); conformance tests for counter/LWW invariants catch the rest |
| SQLite semantics drift from PG (FTS ranking, upsert edge cases, LWW ties) | Medium | Conformance suite is the contract; FTS parity is top-K overlap, not identical ranking (search is re-authorized downstream anyway, §2.4) |
| `zeromq` crate maturity gaps | Medium | Trait insulation; `async_zmq`/libzmq fallback pre-identified; in-process default means ZMQ is never on the Solo critical path |
| In-process SharedState weakens replay/rate-limit guarantees if misdeployed multi-node | Medium | Config validation rejects inproc-state + multi-node (§3.4); fail-closed contract tested per backend |
| Wire/schema compat breaks running deployments mid-refactor | Medium | Redis topic names + key layouts preserved byte-for-byte; PG migrations untouched; profile "Production (today's)" is CI-tested every phase |
| Desktop Workstream UI underestimated | Medium | Started early (overlaps Phase 2); CLI-first means the model is exercisable and demo-able before UI completes |
| Scope creep from spec's "agents/skills/clients" breadth | Low | §5.4 cuts are explicit and config-gated, not deleted — reversible later |

---

## 10. Success metrics → verification hooks

| Spec §14 metric | Verified by |
|---|---|
| Non-code user completes a review cycle without git | Phase 3/4 multi-agent E2E scenario, git capability off |
| Binary usable with zero external services | Solo-profile E2E job in CI (no service containers in that job) |
| SQLite→Postgres switch is config-only | Same E2E suite, two profiles, one binary |
| ZMQ fan-out reliable single-node & small-cluster | Phase 1 E2E (single-node) + Phase 5 soak (cluster) |

---

## 11. Immediate next steps

1. Review/approve this plan (esp. §3.1 transaction rule, §5.1 kind numbers,
   §5.4 scope cuts, §8 sequencing).
2. Phase 0.1–0.2: ADRs + API crate scaffolding.
3. Phase 0.4: `command_executor` SQL relocation — first code PR, zero
   behavior change, immediately reduces risk for everything that follows.

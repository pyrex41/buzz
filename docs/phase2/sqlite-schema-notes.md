# SQLite serve-path schema — translation notes

Companion to [`sqlite-schema.sql`](sqlite-schema.sql), the embedded migration
0001 for the `buzz-db` SQLite backend (solo profile, relay serve path only).
Sources: `migrations/0001`–`0024` (Postgres) cross-checked against the SQL
actually issued by `crates/buzz-db/src/*.rs`, `crates/buzz-audit/src/service.rs`,
and `crates/buzz-search/src/query.rs`.

---

## Global conventions (apply to every table)

### Timestamps: INTEGER unix seconds — the ONE convention

**Every Postgres `TIMESTAMPTZ` column becomes `INTEGER` unix seconds (UTC).**
Chosen over TEXT RFC3339 because range predicates (`created_at >= ?`,
keyset cursors) compare integers directly and index compactly, and Nostr
`created_at` is already second-resolution on the wire. Conversion happens at
the Rust boundary: `chrono::DateTime<Utc> -> .timestamp()` on bind,
`DateTime::from_timestamp(secs, 0)` on read. `events.not_before` and
`events.delivered_at` were already `BIGINT` unix seconds in Postgres —
unchanged, which is a consistency win.

Consequences to accept knowingly:

- Server-stamped columns (`received_at`, `ttl_deadline`, `updated_at`, …) lose
  sub-second precision. No serve-path ordering depends on it — every
  pagination path tiebreaks on `id ASC`, and TTL deadlines are minutes-scale.
- SQL that generated times in Postgres translates mechanically:
  `NOW()` / `now()` / `clock_timestamp()` → `unixepoch()`;
  `clock_timestamp() + make_interval(secs => ttl_seconds)` →
  `unixepoch() + ttl_seconds`; `to_timestamp($n)` → plain integer bind;
  `EXTRACT(EPOCH FROM created_at)::bigint` (buzz-search) → just `created_at`.

### UUIDs: TEXT, lowercase hyphenated

`UUID` → `TEXT` in canonical lowercase-hyphenated form. Matches how sqlx
binds `uuid::Uuid` on SQLite (workspace already enables `sqlx = 0.9`
features `["sqlite", "uuid"]`) and keeps rows greppable in the `sqlite3`
CLI. **Verification item:** add a round-trip unit test asserting the stored
form is lowercase hyphenated TEXT, so a future sqlx encoding change (e.g. to
BLOB(16)) fails loudly instead of silently bifurcating key formats.

Columns with `DEFAULT gen_random_uuid()` get the standard `randomblob()`
v4-UUID default expression so INSERTs that rely on the DB default
(`Db::ensure_configured_community` inserts only `host`) keep working. The
Rust arm may instead pass `Uuid::new_v4()` explicitly; both produce the same
format.

### Other type mappings

| Postgres | SQLite | Notes |
|---|---|---|
| `BYTEA` | `BLOB` | `length(blob)` returns bytes, so `CHECK (length(pubkey) = 32)` ports verbatim |
| `JSONB` | `TEXT` (JSON) | JSON1 (`json_each`, `json_extract`, `json_type`) is built into modern SQLite |
| enum types | `TEXT` + `CHECK (… IN (…))` | the 10 `CREATE TYPE`s from 0001 are inlined per column; Rust arm drops all `::text` casts (`channel_type::text`, `role::text`, `status::text`, …) |
| `BOOLEAN` | `INTEGER` 0/1 | sqlx maps `bool` ↔ INTEGER; partial index `WHERE enabled` becomes `WHERE enabled = 1` |
| `VARCHAR(n)` | `TEXT` | SQLite ignores length; caps stay app-level (e.g. `D_TAG_MAX_LEN` in event.rs) |
| `BIGINT GENERATED ALWAYS AS IDENTITY` | `INTEGER PRIMARY KEY AUTOINCREMENT` | rate_limit_violations, delivery_log |

### PRAGMAs (required, per-connection)

Set via `SqliteConnectOptions`, **not** in the migration (sqlx runs migrations
inside a transaction, where `PRAGMA foreign_keys` is a silent no-op):

```
journal_mode = WAL          -- persistent once set; concurrent readers + 1 writer
synchronous  = NORMAL       -- safe with WAL; fsync at checkpoint
busy_timeout = 5000         -- writers wait instead of failing SQLITE_BUSY
foreign_keys = ON           -- REQUIRED: FKs are declared but dead without it
```

Writers should open transactions with `BEGIN IMMEDIATE` to take the write
lock up front — this, plus SQLite's single-writer model, is the replacement
for every `pg_advisory_xact_lock` / `pg_advisory_lock` in the Postgres arm
(call sites: `event.rs::persist_command_event`, `lib.rs::replace_addressable_event`
/ `replace_parameterized_event` / `publish_nip43_membership_locked` /
`create_community_with_owner`, `channel.rs:1114`, `relay_members.rs:423`,
`buzz-audit/src/service.rs::log`). No Rust-side mutex needed: the write lock
serializes each transaction end-to-end.

### Tenant-isolation invariant — PRESERVED

The `migration.rs` lint contract carries over verbatim and the DDL honors it:

1. Every tenant-scoped table has `community_id TEXT NOT NULL`.
2. Every PK/UNIQUE/FK on a scoped table **leads with `community_id`** (child
   rows join on the community tuple, e.g. `channel_members → channels
   (community_id, id)`).
3. `channels.community_id` is immutable — enforced by a real SQLite trigger
   (`trg_channels_community_id_immutable`), not just convention.
4. Operator-global tables are named explicitly in `_operator_global_tables`:
   `communities`, `rate_limit_violations`, `product_feedback`,
   `_operator_global_tables`. The sqlite backend should grow the equivalent
   lint test over this DDL.

### `xmax = 0` and other insert-detection tricks

- `lib.rs:850` `ensure_configured_community`:
  `INSERT … ON CONFLICT (lower(host)) DO UPDATE SET host = communities.host
  RETURNING id, host, (xmax = 0) AS created`. SQLite has no `xmax`. Replacement:
  `INSERT INTO communities (host) VALUES (?) ON CONFLICT (host) DO NOTHING
  RETURNING id, host` — a returned row means `created = true`; empty result
  means conflict, so follow with `SELECT id, host FROM communities WHERE host = ?`
  (`created = false`). Alternatively `INSERT OR IGNORE` + `changes()`.
- Everywhere else the code already uses `rows_affected()` over
  `ON CONFLICT DO NOTHING` / conditional `DO UPDATE … WHERE` — SQLite reports
  `changes()` identically (insert-ignored → 0, update-skipped-by-WHERE → 0),
  so `insert_event`, `add_reaction` (three-state semantics), channel-member
  re-activation, and the cron-fire claim port without semantic change.
- All `ON CONFLICT (col, …)` targets in the codebase are column lists over
  PKs/unique indexes — valid SQLite conflict targets. The one expression
  target, `ON CONFLICT (lower(host))`, is eliminated by making
  `communities.host` `COLLATE NOCASE` + `UNIQUE` (host is stored
  pre-normalized; NOCASE is the ASCII belt-and-suspenders, same coverage as
  Postgres `lower()` for the ASCII-normalized host contract).

---

## Per-table notes

### `communities` (operator-global)
Includes `icon` (0003) and `archived_at` (0016). `UNIQUE INDEX` on NOCASE
`host` replaces `UNIQUE ON lower(host)`; upsert target becomes
`ON CONFLICT (host)`. v4-UUID default keeps the `INSERT (host) RETURNING id`
seeding path working. Nil-UUID CHECK ported as a TEXT comparison.

### `users`
Straight port. `capabilities` JSONB → TEXT. Partial unique indexes on
`(community_id, lower(nip05_handle))` and `(community_id, okta_user_id)`
port directly (SQLite supports expression + partial indexes; `lower()` is
ASCII-only, acceptable for validated handles). Composite self-FK
`(community_id, agent_owner_pubkey) → users` with `ON DELETE SET NULL` is
supported and kept.

### `channels`
Straight port; enums inlined as CHECKs. The plpgsql
`channels_community_id_immutable()` trigger becomes a native SQLite
`BEFORE UPDATE OF community_id … RAISE(ABORT)` trigger — invariant kept in
the database, not moved to Rust. TTL columns unchanged (`ttl_deadline` now
integer seconds).

### `channel_members`
Straight port. Partial index `(community_id, pubkey) WHERE removed_at IS NULL`
kept. The `ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE`
re-activation upserts (channel.rs:130/224/408, dm.rs:185) work unchanged.

### `events` — single table, no partitions
- **Partitioning → one table.** The monthly `PARTITION BY RANGE (created_at)`
  and `partition.rs` manager (`ensure_future_partitions`) do not exist in the
  sqlite backend; the backend dispatch should no-op that call.
- **PK re-keyed to `(community_id, id)`.** Postgres used
  `(community_id, created_at, id)` only because the partition key must be in
  the PK. A Nostr id commits to `created_at`, so `(community_id, id)` gives
  identical dedup semantics for valid signed events, still allows the same
  event in two communities, and serves `WHERE community_id = ? AND id = ?`
  directly (replacing `idx_events_community_id`).
- **Composite indexes** (all community-leading, matching the query builder in
  `event.rs`): `(community_id, created_at DESC, id)`,
  `(community_id, channel_id, created_at DESC, id)` (channel windows,
  `get_last_message_at[_bulk]`), `(community_id, pubkey, kind, created_at DESC, id)`,
  `(community_id, kind, created_at DESC, id)`,
  `(community_id, kind, pubkey, channel_id, deleted_at)` (addressable
  replacement), partial `(community_id, kind, pubkey, d_tag, created_at DESC, id)`
  (NIP-33 coordinate: `replace_parameterized_event`, `persist_command_event`,
  `soft_delete_by_coordinate`), partial `(community_id, not_before)`
  (due reminders), `(community_id, deleted_at)`. SQLite honors `DESC` index
  columns and partial (`WHERE`) indexes.
- **`tags JSONB` → TEXT; GIN containment queries must be rewritten.** The
  Postgres arm relies on `idx_events_tags_gin` (0004, jsonb_path_ops) for
  `tags @> '[["e","<hex>"]]'`. The queries that use containment are exactly:
  - `event.rs::query_events` e-tag pushdown (~lines 347–366)
  - `event.rs::count_events` e-tag pushdown (~lines 570–583)

  **SQLite arm uses `json_each()` or the side-table path**: per e-tag OR-arm,
  `EXISTS (SELECT 1 FROM json_each(events.tags) t
  WHERE t.value ->> 0 = 'e' AND t.value ->> 1 = ?)`. This is an
  unindexed scan of each candidate row's tags — fine at solo-profile scale
  because the surrounding predicates (community/channel/kind/created_at) are
  index-served first. If it ever shows up in profiles, add an `event_e_tags
  (community_id, e_tag_hex, event_id)` side table maintained on insert,
  mirroring the `event_mentions` pattern (which already covers the `#p` path
  — feed.rs and the `p_tag_hex` join need **no** JSON work).
  Non-serve-path `@>`/`jsonb_array_elements` users (migrations 0007/0009–0011,
  0019 triggers; `lib.rs::backfill_d_tags`, which is a brownfield-Postgres
  backfill a fresh sqlite DB never needs) are omitted entirely.
- **`search_tsv` generated column + GIN → GONE**; see `events_fts` below.
- **`DISTINCT ON` (query_due_reminders, event.rs:1217)**: not SQLite syntax.
  Rewrite with a window function:
  `ROW_NUMBER() OVER (PARTITION BY e.community_id, e.pubkey, e.d_tag
  ORDER BY e.created_at DESC, e.id ASC) = 1` in a subquery.
- **`ILIKE`/`octet_length` (huddle link lookup, event.rs:124–137)**: SQLite
  `LIKE` is already ASCII-case-insensitive → use `LIKE`; `octet_length(content)`
  → `length(CAST(content AS BLOB))`.
- **`GREATEST(x - 1, 0)`** (thread counter decrements) → SQLite scalar
  `max(x - 1, 0)`.
- **TTL refresh trigger (0022/0024)**: ported as a plain synchronous
  `AFTER INSERT` trigger (`trg_events_refresh_channel_ttl`). The deferred
  constraint trigger + `FOR UPDATE`/advisory-lock choreography existed only
  for Postgres commit-time race/contention reasons; SQLite's single writer
  makes the simple trigger correct. Insert paths need no Rust changes.
- **created_at floor guard (0021)**: OMITTED — it exists solely for the
  replica-fence proof (read replicas), which is out of solo-profile scope.
  The ingest-time `|created_at − now| ≤ 900 s` check in the relay handler
  remains the only enforcement.

### `events_fts` (FTS5 external-content) — replaces tsvector/GIN
DDL and maintenance are spelled out in the schema file:

```sql
CREATE VIRTUAL TABLE events_fts USING fts5(
    content, content='events', content_rowid='rowid', tokenize='unicode61');
-- AFTER INSERT  (kind allowlisted): INSERT INTO events_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
-- AFTER DELETE  (kind allowlisted): INSERT INTO events_fts(events_fts, rowid, content) VALUES ('delete', OLD.rowid, OLD.content);
-- AFTER UPDATE OF content (kind allowlisted): 'delete' old + insert new.
```

- **Positive kind allowlist `(0, 9, 40002, 45001, 45003)`** — a sqlite DB is
  by definition a fresh install, so migration 0008's allowlist (not the
  0001/0005/0014 exclusion-list lineage) is the correct semantic. Privacy
  kinds (1059, 30300, 30350, 30622, 44100, 44101, 44200, all ephemeral and
  ciphertext kinds) are storage-level unsearchable because they are never
  indexed. If a new searchable kind is added, update the three trigger WHEN
  clauses together and add a regression test mirroring
  `buzz-search/tests/fts_integration.rs`.
- **Soft deletes**: `deleted_at` is filtered at query time by joining back to
  `events` (`events.rowid = events_fts.rowid`), same as the Postgres arm
  filters `deleted_at IS NULL` next to the `@@` probe. Hard deletes (NIP-RS /
  mesh replacement) fire the DELETE trigger and drop index entries.
- **Query shape** (buzz-search sqlite arm):
  `SELECT e.id, e.kind, e.pubkey, e.channel_id, e.created_at, bm25(events_fts) AS rank
  FROM events_fts JOIN events e ON e.rowid = events_fts.rowid
  WHERE events_fts MATCH ? AND e.community_id = ? AND e.deleted_at IS NULL …
  ORDER BY rank, e.created_at DESC, e.id`. `plainto_tsquery` /
  `websearch_to_tsquery` modes map onto FTS5 MATCH syntax (implicit AND;
  quote phrases; `bm25()` is ascending-better whereas `ts_rank_cd` is
  descending-better — flip the sort). `community_id = ?` remains the
  non-negotiable first predicate.
- **`'rebuild'` is FORBIDDEN**: `INSERT INTO events_fts(events_fts)
  VALUES('rebuild')` would re-index every row unconditionally, bypassing the
  kind allowlist — a privacy regression. Never expose it in tooling.
- **BUILD-TIME VERIFICATION ITEM**: sqlx's bundled libsqlite3 must be compiled
  with FTS5. Verify with `SELECT count(*) FROM pragma_compile_options WHERE
  compile_options = 'ENABLE_FTS5'` in a backend startup self-check plus a CI
  unit test that creates an FTS5 table. If the bundled build lacks it, switch
  to `sqlite-unbundled` against a system SQLite with FTS5, or enable the
  appropriate libsqlite3-sys build flag.

### `event_mentions`
Straight port (`pubkey_hex` stays hex TEXT). All three indexes kept,
including 0007's `(community_id, event_id)` cleanup index. The
0009 `guard_event_mention_live` trigger (FOR KEY SHARE fencing of concurrent
hard deletes) is OMITTED: it defends a cross-connection race that a
single-writer SQLite cannot exhibit; mention insertion remains the
post-commit best-effort path in `lib.rs::insert_mentions`.

### `parameterized_event_watermarks` (NIP-RS, 0007)
Table ported as-is. The plpgsql guard triggers (0009 `guard_nip_rs_watermark`,
0010/0011 revisions, `guard_nip_rs_hard_delete` + the
`buzz.nip_rs_hard_delete` GUC opt-in) are **OMITTED — the check moves to
(and already lives in) Rust**: `lib.rs::replace_parameterized_event` performs
the watermark read, domination check, hard-delete of superseded NIP-RS/mesh
payloads, and watermark upsert inside one transaction. The DB triggers only
protected against *older relay binaries* writing during rolling deploys —
impossible in the solo profile. The sqlite arm must simply skip the
`set_config('buzz.nip_rs_hard_delete', …)` and `current_setting` calls.
Mesh-status head-only retention (0019) is likewise already handled by the
same Rust path (`is_buzz_mesh_status` branch); its purge trigger is excluded
with the mesh scope.

### `thread_metadata` (reply_count / descendant_count materialization)
Re-keyed from `(community_id, event_created_at, event_id)` (partition
artifact) to **`(community_id, event_id)`** — every counter UPDATE in
`event.rs`/`thread.rs` already targets exactly that tuple, and the PK now
serves it (Postgres needed the extra `idx_thread_metadata_event_id`, dropped
here). Stub-row inserts for parent/root and the `ON CONFLICT DO NOTHING`
guard before counter bumps port unchanged; the counter increments/decrements
(`reply_count + 1`, `GREATEST(… − 1, 0)` → `max(… − 1, 0)`,
`last_reply_at = NOW()` → `unixepoch()`) stay in the same Rust transaction as
the event insert. Parent/root/channel-depth indexes kept.

### `reactions`
PK deliberately kept **identical to Postgres** —
`(community_id, event_created_at, event_id, pubkey, emoji)` — because
`reaction.rs::ADD_REACTION_SQL` names it as the upsert conflict target; the
three-state semantics (insert / re-activate via `DO UPDATE … WHERE removed_at
IS NOT NULL` / active-duplicate → 0 rows) work identically in SQLite. Only
`created_at = NOW()` → `unixepoch()`. Partial unique index on
`(community_id, reaction_event_id)` kept.

### `subscriptions`, `delivery_log`, `rate_limit_violations` — parity only
**No Rust code queries these tables today** (verified by grep across
`crates/`). Carried so the sqlite schema is 1:1 with Postgres minus
push/mesh. `delivery_log` loses its monthly partitions and its odd
`(delivered_at, id)` PK (a partition artifact) in favor of a rowid PK +
`delivered_at` index. Safe to drop from the migration if parity is not wanted.

### `workflows`, `workflow_runs`, `workflow_approvals`
Straight ports; enums → CHECKs, `definition`/`execution_trace`/
`trigger_context` JSONB → TEXT, `definition_hash`/`token` BYTEA → BLOB.
`workflow.rs:328`'s `ON CONFLICT (community_id, id) DO UPDATE` upsert and the
approval token-hash lookups port unchanged (`updated_at = NOW()` →
`unixepoch()`). Partial index `WHERE enabled` → `WHERE enabled = 1`; the
sqlite arm's scheduler query must write `enabled = 1` for the partial index
to be usable.

### `scheduled_workflow_fires`
Straight port. The at-most-once cron claim (`INSERT … ON CONFLICT
(community_id, workflow_id, scheduled_for) DO NOTHING RETURNING …`,
workflow.rs:508) is natively supported by SQLite ≥ 3.35 (RETURNING). The
`ON DELETE NO ACTION` FK to `workflow_runs` is SQLite's default and kept.

### `api_tokens`
Straight port. `scopes`/`channel_ids` JSONB → TEXT; token-hash length CHECK
kept; unique `(community_id, token_hash)` kept.

### `pubkey_allowlist`, `relay_members`, `join_policy_acceptances`
Straight ports. `relay_members.pubkey` stays hex TEXT (wire form). The
owner-count guard (`create_community_with_owner`) drops its advisory lock —
`BEGIN IMMEDIATE` serializes the count-then-insert. `join_policy_acceptances`
keeps its cascade FK onto `relay_members`.

### `archived_identities`
Straight port (all-TEXT identity columns preserved).

### `audit_log`
Straight port; `detail` JSONB → TEXT, hashes BLOB. The per-community
`pg_advisory_lock` in `buzz-audit::AuditService::log` becomes unnecessary:
run the head-read + insert inside one `BEGIN IMMEDIATE` transaction (the code
already wraps both in a transaction). Unique `(community_id, hash)` kept;
`(community_id, seq)` PK preserves one hash chain per tenant.

### `git_repo_names`
Straight port. Name-claim `INSERT … ON CONFLICT` atomicity and the per-owner
COUNT quota work unchanged.

### `moderation_actions`, `moderation_reports`, `community_bans`
Ports of migration 0006 with one structural change: **`moderation_actions` is
declared before `moderation_reports`** so the `reports.action_id` FK can be
inline — Postgres added it with `ALTER TABLE ADD FOREIGN KEY`, which SQLite
does not support. The exactly-one-target-class CHECK, all partial indexes,
and the idempotency unique `(community_id, report_event_id)` port verbatim.
Ban expiry comparisons (`ban_expires_at`, `muted_until`) become integer
comparisons against `unixepoch()`.

### `product_feedback` (operator-global)
Straight port. `btrim(body)` → `trim(body)`; `jsonb_typeof(tags) = 'array'` →
`json_type(tags) = 'array'`.

### `_operator_global_tables`
Ported with all four registry rows (0001's three + 0017's `product_feedback`)
inserted in this single migration.

---

## Excluded tables and machinery (deliberate)

| Excluded | Source | Reason |
|---|---|---|
| `push_leases`, `push_wake_outbox` | 0012 | push leases / wake outbox — out of solo scope |
| `push_gateway_challenges`, `push_gateway_installations`, `push_gateway_delegations`, `push_gateway_endpoint_quotas`, `push_gateway_delivery_auth_replays`, `push_gateway_delivery_request_replays` | 0015 | push gateway authority |
| `push_match_queue` | 0018 | push match queue |
| push-lease FTS exclusion rewrite | 0014 | subsumed by the positive allowlist (30350 is simply never indexed) |
| events partitions + `partition.rs` | 0001 | single `events` table; `ensure_future_partitions` no-ops on sqlite |
| replica-fence floor trigger + `buzz.created_at_floor` GUC + `replica_fence.rs` catalog check | 0021 | replica fencing out of scope |
| mesh-status purge trigger | 0019 | head-only retention already enforced by `replace_parameterized_event` in Rust |
| NIP-RS guard triggers + `buzz.nip_rs_hard_delete` GUC | 0009–0011 | mixed-version rolling-deploy defense; single-writer solo profile enforces in Rust |
| advisory-lock TTL trigger revision | 0024 | replaced by a plain synchronous trigger (single writer) |
| `pg_advisory_*`, GUCs, `xmax`, `pg_locks` introspection | various | see "Global conventions" |

## Rust-side obligations checklist (backend dispatch work this schema implies)

1. Connection options: WAL / NORMAL / busy_timeout / foreign_keys, `BEGIN
   IMMEDIATE` for write transactions; drop all `pg_advisory_*` statements.
2. Bind timestamps as `i64` seconds; UUIDs as `uuid::Uuid` (TEXT); drop
   `::text` enum casts, `to_timestamp`, `make_interval`, `EXTRACT(EPOCH …)`.
3. Rewrite the two e-tag containment filters (`query_events`, `count_events`)
   with `json_each()`.
4. Rewrite `query_due_reminders`'s `DISTINCT ON` with `ROW_NUMBER()`.
5. Rewrite `ensure_configured_community`'s `xmax = 0` with
   `ON CONFLICT DO NOTHING RETURNING` + fallback SELECT.
6. Point buzz-search's sqlite arm at `events_fts MATCH` + `bm25()`.
7. Skip `ensure_future_partitions`, `backfill_d_tags`, replica-fence probes,
   and the NIP-RS GUC calls on the sqlite backend.
8. Startup self-check + CI test: FTS5 compiled into sqlx's bundled
   libsqlite3; Uuid encodes as lowercase hyphenated TEXT.

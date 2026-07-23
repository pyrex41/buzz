# buzz-db Method Classification Worksheet (SQLite "Solo" Backend, Phase 2)

Classification of every public method on `Db` (`crates/buzz-db/src/lib.rs`) for the
internal backend-dispatch seam. `Db` keeps its public surface; each method gains a
SQLite arm module-by-module. Methods classified **pg-only** return a typed
`UnsupportedBackend` error on the SQLite backend.

**Method count reconciliation.** `grep -c "pub async fn\|pub fn" crates/buzz-db/src/lib.rs`
returns **218**. Two of those are NOT `Db` methods:

1. `insert_mentions` (free function, line 97) — the p-tag mention indexer called by the
   insert paths. It is part of the events work packet (multi-row `INSERT … VALUES … ON
   CONFLICT DO NOTHING` via `QueryBuilder`), but is not dispatched through `Db`.
2. `UsageMetricsLeader::is_live` (line 215) — a method on the advisory-lock guard struct,
   pg-only by construction (owned detached `PgConnection`).

That leaves **216 `Db` methods**, all classified below. `Db::persist_command_event` is
included (line 662).

Column legend:

- **classification**: `serve-core` (needed by the relay's live serve path on Solo),
  `pg-only` (inherently Postgres/multi-pod; SQLite arm returns `UnsupportedBackend`),
  `infra` (pool plumbing the dispatch seam itself handles).
- **PG-specific features**: Postgres constructs the SQLite implementer must translate
  (or that justify pg-only). "—" = plain portable SQL (bind params, SELECT/INSERT/
  UPDATE/DELETE, LIMIT/ORDER BY).

Cross-cutting PG features (apply to many rows; abbreviated in tables):

- **NOW()/now()/clock_timestamp()** — server-side clock used for `deleted_at`,
  `removed_at`, `updated_at`, etc. SQLite arm must standardize on one clock source
  (application `Utc::now()` or `unixepoch()`/`strftime`); mixed clocks would break
  fence/ordering comparisons.
- **ON CONFLICT** — SQLite supports upsert syntax (`ON CONFLICT … DO NOTHING/DO UPDATE`)
  natively; portable except the `xmax = 0` inserted-vs-updated trick (see
  `ensure_configured_community`).
- **= ANY($n)** with array binds (`uuid[]`, `bytea[]`, `text[]`) — SQLite has no array
  binds; rewrite as dynamically-built `IN (…)` lists or `json_each` over a JSON array.
- **advisory locks** (`pg_advisory_xact_lock`, `pg_try_advisory_lock`,
  `hashtextextended`) — no SQLite equivalent, and none needed: SQLite serializes writers
  globally (single writer). SQLite arm replaces each advisory-lock+tx pattern with a
  plain immediate-mode write transaction.
- **RETURNING** — supported by SQLite ≥ 3.35; portable.
- **IS NOT DISTINCT FROM** — SQLite spelling is `IS`.
- **UPDATE … FROM** join — supported by SQLite ≥ 3.33; portable with syntax care.
- **jsonb** — `events.tags` is `jsonb`; `tags @> '[["e","…"]]'` containment (GIN
  jsonb_path_ops index, migration 0004) must be rewritten (e.g. `EXISTS (SELECT 1 FROM
  json_each(tags) …)`) with an eye on query cost.
- **PG enum types** — `channels.channel_type` / `channels.visibility` are PG enums read
  via `::text` casts; SQLite schema should store TEXT + CHECK constraints and drop the
  casts.
- **`(… || ' seconds')::interval` / `make_interval` / `NOW() + INTERVAL`** — interval
  arithmetic for TTL deadlines; SQLite arm uses `datetime(…, '+N seconds')` or
  application-computed timestamps.

---

## 1. Infra — pool plumbing (dispatch seam handles these)

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `new` | lib.rs (constructor) | infra | `PgPoolOptions`; `after_connect` hook running `set_config('buzz.created_at_floor', …)` GUC to arm the deferred floor-guard trigger (migration 0021); optional read-replica pool | SQLite arm gets its own constructor/config path; floor-guard GUC is replica-fence machinery and does not exist on SQLite |
| `from_pool` | lib.rs | infra | `PgPool` in signature | test constructor; seam needs a SQLite equivalent |
| `from_pools` | lib.rs | infra | writer + replica `PgPool`s | replica variant is meaningless on SQLite (single file) |
| `migrate` | lib.rs → migration.rs | infra | delegates to `migration::run_migrations` (PG migration set: partitions, triggers, tsvector, enum types) | SQLite backend needs its **own** migration set/schema; do not attempt to port PG migration SQL 1:1 |
| `ping` | lib.rs | infra | `SELECT 1` | trivially portable |
| `pool_stats` | lib.rs | infra | sqlx pool introspection | report SQLite pool/connection stats or static values |
| `begin_transaction` | lib.rs | infra | **returns `sqlx::Transaction<'static, sqlx::Postgres>` — a PG type leaks through the public signature** | biggest seam problem in this bucket: callers in buzz-relay compose multi-statement transactions against this handle. The dispatch seam must either enum-wrap the transaction type or migrate callers off raw transactions |

## 2. Replica fence / read-pool plumbing — pg-only

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `fence` | lib.rs → replica_fence.rs | pg-only | LSN handshake fence (`pg_current_wal_lsn`, `pg_last_wal_replay_lsn`, `pg_stat_activity`, `pg_prepared_xacts`) | accessor for the fence Arc; on SQLite there is no replica — a permanently "closed"/inert fence object is fine (callers of `fence()` exist in tests/relay wiring) |
| `spawn_fence_probe` | lib.rs → replica_fence.rs | pg-only | catalog + behavior verification of the deferred floor-guard trigger; background LSN probe | SQLite arm: return `Ok(false)` ("no replica configured") rather than erroring — matches the existing no-replica path |
| `read` | lib.rs | pg-only | read-replica pool routing | on SQLite always the single connection/pool; degenerate implementation acceptable instead of `UnsupportedBackend` since `read()` falls back to writer already |
| `has_read_pool` | lib.rs | pg-only | — | SQLite: constant `false` |
| `read_pool_stats` | lib.rs | pg-only | — | SQLite: constant `None` |

Note: `fence`/`read`/`has_read_pool`/`read_pool_stats`/`spawn_fence_probe` are
classified pg-only per the worksheet buckets, but each has a natural no-op/degenerate
Solo behavior (`false`/`None`/writer pool) that keeps the relay serve path working
without `UnsupportedBackend`; recommend degenerate impls, reserving the typed error for
methods with no sensible single-node semantics.

## 3. Usage metrics (leader election + gauges) — pg-only

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `try_lock_usage_metrics` | lib.rs | pg-only | `pg_try_advisory_lock` on a **detached session connection** (`connection.detach()`); guard struct `UsageMetricsLeader` owns a `PgConnection` | multi-pod leader election; meaningless single-node |
| `usage_community_count` | usage.rs | pg-only | — | metrics-poller gauge |
| `usage_user_counts` | usage.rs | pg-only | `COUNT(*) FILTER (WHERE …)` | SQLite lacks `FILTER` on aggregates pre-3.30-ish semantics; but classified pg-only regardless (poller) |
| `usage_channel_counts` | usage.rs | pg-only | — | |
| `usage_message_counts` | usage.rs | pg-only | partition-scan cost note in module docs | |
| `usage_relay_member_counts` | usage.rs | pg-only | — | |
| `usage_workflow_counts` | usage.rs | pg-only | — | |
| `usage_git_repo_counts` | usage.rs | pg-only | — | |
| `usage_active_user_counts` | usage.rs | pg-only | `NOW() - INTERVAL '{literal}'` — **interval SQL string interpolated with `format!`**, trusted `&'static str`; `FILTER (WHERE …)` | |
| `usage_active_channel_counts` | usage.rs | pg-only | same interval interpolation | |
| `usage_community_hosts` | usage.rs | pg-only | — | |

## 4. Deployment admin plane (read-only) — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `admin_list_reports` | admin_moderation.rs | serve-core | dynamic filter/cursor builder; no PG-only constructs found | deployment-global (cross-community) reads; judgment call: kept serve-core because a Solo operator still gets the admin plane |
| `admin_get_report` | admin_moderation.rs | serve-core | — | |
| `admin_list_feedback` | admin_moderation.rs | serve-core | — | |
| `admin_get_feedback` | admin_moderation.rs | serve-core | — | |

## 5. Communities / tenancy — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `lookup_community_by_host` | lib.rs (inline SQL) | serve-core | `lower(host)` predicate backed by a functional unique index (case-insensitive host map) | SQLite supports expression indexes — portable |
| `is_community_active` | lib.rs (inline) | serve-core | `EXISTS(…)` | portable |
| `lookup_community_by_host_for_management` | lib.rs (inline) | serve-core | `lower(host)` | operator plane, ignores `archived_at` |
| `list_communities_owned_by` | lib.rs (inline) | serve-core | — | join `communities` × `relay_members` |
| `lookup_community_host` | lib.rs (inline) | serve-core | — | reverse host lookup for tenant context |
| `get_community_icon` | lib.rs (inline) | serve-core | — | |
| `set_community_icon` | lib.rs (inline) | serve-core | — | |
| `ensure_configured_community` | lib.rs (inline) | serve-core | `ON CONFLICT (lower(host)) DO UPDATE SET host = communities.host RETURNING …, (xmax = 0) AS created` — **xmax system-column trick** to detect insert-vs-existing | SQLite rewrite: `INSERT … ON CONFLICT DO NOTHING` + `changes()`/second SELECT, or RETURNING with a sentinel; the no-op DO UPDATE exists solely to make RETURNING yield the row |
| `create_community_with_owner` | lib.rs (inline) | serve-core | transaction + `pg_advisory_xact_lock(owner_count_advisory_lock_key)`; `ON CONFLICT (lower(host)) DO NOTHING RETURNING` | advisory lock serializes per-owner community-limit check; on SQLite the global write lock subsumes it — plain tx suffices |
| `archive_community_owned_by` | lib.rs (inline) | serve-core | `UPDATE … FROM` join; `COALESCE(archived_at, now())` (idempotent first-archive stamp); `RETURNING` | `UPDATE…FROM` needs SQLite ≥ 3.33 or subquery rewrite |
| `unarchive_community_owned_by` | lib.rs (inline) | serve-core | `UPDATE … FROM`; `RETURNING` | |
| `community_of_channel` | lib.rs (inline) | serve-core | — | |
| `communities_of_channels` | lib.rs (inline) | serve-core | `id = ANY($1)` uuid[] | conformance read-seam helper; contract: missing channel ⇒ absent from map (pinned by tests) |

## 6. Events — serve-core (except where noted)

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `persist_command_event` | event.rs | serve-core | **returns open tx handle** (`CommandEventTx` wraps `sqlx::Transaction<Postgres>`); `pg_advisory_xact_lock` for NIP-33 coordinate serialization; `ON CONFLICT DO NOTHING` idempotency | same PG-type leak as `begin_transaction`; SQLite arm needs a backend-neutral tx guard type on the seam |
| `insert_event` | event.rs | serve-core | `ON CONFLICT DO NOTHING`; calls free fn `insert_mentions` (multi-row `QueryBuilder` VALUES + `ON CONFLICT DO NOTHING`) | events table is **partitioned** on PG (`community_id, created_at, id` key); SQLite schema is a plain table |
| `query_events` | event.rs | serve-core | dynamic `QueryBuilder`; **`tags @> '[["e","…"]]'` jsonb containment** (GIN jsonb_path_ops); optional join on `event_mentions`; kind/author/id `IN` pushdowns; `channel_id IS NULL` global scoping | the central read path; jsonb containment must become `json_each`-based EXISTS or an e-tag side table on SQLite. NIP-50 `search` is routed to buzz-search **before** reaching here — no tsvector in this method |
| `count_events` | event.rs | serve-core | same builder as `query_events` | |
| `huddle_started_link_exists` | event.rs | serve-core | `content ILIKE $6` | SQLite `LIKE` is case-insensitive for ASCII by default; verify pattern semantics match |
| `get_latest_global_replaceable` | event.rs | serve-core | `ORDER BY created_at DESC, id ASC LIMIT 1` (NIP-16 ordering) | portable |
| `get_event_by_id` | event.rs | serve-core | — | |
| `get_event_by_id_including_deleted` | event.rs | serve-core | — | |
| `soft_delete_event` | event.rs | serve-core | `deleted_at = NOW()` | clock convention |
| `soft_delete_by_coordinate` | event.rs | serve-core | `deleted_at = NOW()`; d_tag coordinate predicate | |
| `soft_delete_event_and_update_thread` | event.rs | serve-core | transaction: soft-delete + reply-counter decrement | |
| `get_last_message_at` | event.rs | serve-core | — | |
| `get_last_message_at_bulk` | event.rs | serve-core | `= ANY` uuid[] | |
| `get_events_by_ids` | event.rs | serve-core | id `IN` pushdown (per-value binds) | |
| `insert_event_with_thread_metadata` | event.rs | serve-core | transaction; `ON CONFLICT DO NOTHING`; thread-counter `reply_count = reply_count + 1, last_reply_at = NOW()` | reply-counter materialization contract (CLAUDE.md) |
| `insert_reaction_event_with_thread_metadata` | event.rs | serve-core | transaction; `ON CONFLICT` upsert on reactions row | |
| `query_due_reminders` | event.rs | serve-core | **`SELECT DISTINCT ON (community_id, pubkey, d_tag)`** + matching ORDER BY; join `communities` for host | `DISTINCT ON` has no SQLite equivalent — rewrite with `ROW_NUMBER() OVER (PARTITION BY …)` window or `GROUP BY` + `MAX` |
| `claim_due_reminder` | event.rs | serve-core | compare-and-set `delivered_at` (cross-pod dedup) | single-node still correct; portable |
| `claim_due_reminder_with_stamp` | event.rs | serve-core | CAS on `delivered_at IS NULL` | portable |
| `release_due_reminder` | event.rs | serve-core | CAS on `delivered_at = $stamp` | portable |
| `soft_delete_discovery_events` | lib.rs (inline) | serve-core | `deleted_at = NOW()`; `kind IN (39000,39001,39002)` | |
| `replace_addressable_event` | lib.rs (inline) | serve-core | transaction + `pg_advisory_xact_lock(fnv1a key)`; NIP-16 dominance check `ORDER BY created_at DESC, id ASC LIMIT 1`; `IS NOT DISTINCT FROM` on channel_id; `deleted_at = NOW()`; `ON CONFLICT DO NOTHING` insert; rollback-on-duplicate | SQLite: advisory lock unnecessary (single writer); `IS NOT DISTINCT FROM` → `IS` |
| `nip43_membership_snapshot_needs_reconciliation` | lib.rs (composed) | serve-core | none — pure composition of `query_events` + `list_relay_members`, comparison in Rust | **not** PG-specific; works as soon as the two callees have SQLite arms |
| `publish_nip43_membership_locked` | lib.rs (inline) | serve-core | transaction + `pg_advisory_xact_lock`; reads `relay_members` inside locked tx; signs event in-tx; `deleted_at = NOW()` retire + `ON CONFLICT DO NOTHING` insert | read-build-write cycle must stay atomic; SQLite immediate tx gives the same serialization |
| `replace_parameterized_event` | lib.rs (inline) | serve-core | transaction + `pg_advisory_xact_lock`; **`set_config('buzz.nip_rs_hard_delete','on', true)` transaction-local GUC** authorizing hard delete against migration-0011 guard trigger; NIP-RS watermark table upsert `ON CONFLICT … DO UPDATE`; hard `DELETE` vs soft-delete branch; `not_before` column; dominance check incl. watermark | most intricate event method. The GUC + guard-trigger fence is PG trigger machinery; the SQLite schema will not have migration-0011's trigger, so the SQLite arm enforces the same invariants in application code (watermark row still required for replay protection) |

## 7. Push leases / wake outbox — pg-only (entire cluster)

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `claim_due_push_match_batch` | push.rs | pg-only | **`FOR UPDATE OF q SKIP LOCKED`**; CTE claim; `now()` lease windows; `RETURNING` | multi-pod matcher queue |
| `active_push_match_leases` | push.rs | pg-only | `EXTRACT(EPOCH FROM now())::bigint` expiry | |
| `complete_push_match_batch` | push.rs | pg-only | `event_id = ANY($3)` bytea[] | fenced by claim_id |
| `retry_push_match_batch` | push.rs | pg-only | `= ANY` | |
| `reap_exhausted_push_matches` | push.rs | pg-only | — | sweep |
| `enqueue_push_wake` | push.rs | pg-only | `ON CONFLICT (community_id, endpoint_hash, event_id) DO NOTHING RETURNING` | idempotent outbox |
| `enqueue_push_wakes` | push.rs | pg-only | **`UNNEST($…::bytea[], $…::text[])` set-wise join**; multi-row insert with `ON CONFLICT … RETURNING` | |
| `claim_due_push_wakes` | push.rs | pg-only | `FOR UPDATE OF o SKIP LOCKED`; joins lease liveness; `EXTRACT(EPOCH FROM now())` | |
| `revalidate_push_wake` | push.rs | pg-only | `FOR UPDATE` row locks; `lease_until >= now()` | |
| `complete_push_wake` | push.rs | pg-only | claim-fenced CAS | |
| `retry_push_wake` | push.rs | pg-only | claim-fenced CAS | |
| `fail_push_wake` | push.rs | pg-only | claim-fenced CAS | |
| `disable_push_endpoint` | push.rs | pg-only | generation-fenced `UPDATE … now()` | |
| `accept_push_lease_event` | push.rs | pg-only | transaction; **two `pg_advisory_xact_lock`s** (per-community push gate via `hashtextextended`, per-lease); `FOR UPDATE` on lease row; kind:30350 event replace inline (`deleted_at=now()`, insert); `ON CONFLICT … DO UPDATE … RETURNING generation`; migration-side shared advisory lock in trigger | also writes to `events` (kind 30350); if Solo ever wants push, this whole module gets revisited — for now typed `UnsupportedBackend` |

## 8. Channels & membership — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `create_channel` | channel.rs | serve-core | `NOW() + ($8 \|\| ' seconds')::interval` TTL deadline; membership bootstrap `ON CONFLICT … DO UPDATE` (owner upsert); PG enum columns read via `::text` | |
| `create_channel_with_id` | channel.rs | serve-core | `ON CONFLICT (community_id, id) DO NOTHING` idempotent create; same TTL interval cast; returns created flag | |
| `get_channel` | channel.rs | serve-core | enum `::text` casts | |
| `get_canvas` | channel.rs | serve-core | — | |
| `set_canvas` | channel.rs | serve-core | — | |
| `add_member` | channel.rs | serve-core | `ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE` re-activation (clears `removed_at`) | |
| `remove_member` | channel.rs | serve-core | `removed_at = NOW(), removed_by = $1` | |
| `is_member` | channel.rs | serve-core | — | |
| `membership_pairs` | channel.rs | serve-core | `channel_id = ANY($2) AND pubkey = ANY($3)` (uuid[] + bytea[]) | one-statement pair intersection; SQLite: two `IN` lists or json_each cross-filter |
| `get_members` | channel.rs | serve-core | — | |
| `get_members_bulk` | channel.rs | serve-core | `= ANY($2)` uuid[] | |
| `get_accessible_channel_ids` | channel.rs | serve-core | `UNION` of membership + open visibility | portable |
| `list_channels` | channel.rs | serve-core | enum `::text` casts | |
| `get_accessible_channels` | channel.rs | serve-core | membership/visibility union with filters | |
| `get_bot_members` | channel.rs | serve-core | **`json_agg(DISTINCT jsonb_build_object('name', …, 'id', …))`** aggregation | SQLite: `json_group_array(json_object(…))` (no DISTINCT inside — dedupe in SQL subquery or Rust) |
| `get_users_bulk` | channel.rs | serve-core | `= ANY` bytea[] | |
| `update_channel` | channel.rs | serve-core | dynamic SET clause; `ttl_deadline = NOW() + (… \|\| ' seconds')::interval`; **`pg_advisory_xact_lock(hashtextextended($1,0))`** taken EXCLUSIVE to coordinate with the migration-side TTL trigger's shared lock | the shared/exclusive advisory pair guards TTL transitions against the huddle-TTL trigger (migration-defined); SQLite arm re-implements TTL invariants without triggers |
| `set_topic` | channel.rs | serve-core | `topic_set_at = NOW()` | |
| `set_purpose` | channel.rs | serve-core | `purpose_set_at = NOW()` | |
| `archive_channel` | channel.rs | serve-core | `archived_at = NOW()` | |
| `unarchive_channel` | channel.rs | serve-core | recomputes `ttl_deadline = NOW() + (ttl_seconds \|\| ' seconds')::interval` | |
| `soft_delete_channel` | channel.rs | serve-core | `deleted_at = NOW()` | |
| `get_member_count` | channel.rs | serve-core | — | |
| `get_member_counts_bulk` | channel.rs | serve-core | `= ANY` uuid[] | |
| `get_member_role` | channel.rs | serve-core | — | |
| `reap_expired_ephemeral_channels` | channel.rs | serve-core | `UPDATE channels AS ch SET archived_at = NOW() … FROM communities c … WHERE ch.ttl_deadline < NOW() … RETURNING` (UPDATE…FROM join); interacts with migration-side TTL trigger + shared advisory lock (`clock_timestamp()` in trigger path) | huddle TTL janitor — needed on Solo for huddles to expire. Trigger/advisory-lock interplay lives in migration SQL; SQLite arm must enforce the TTL state machine in the query itself |

## 9. Users — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `ensure_user` | user.rs | serve-core | `ON CONFLICT DO NOTHING`; rows_affected → created flag | |
| `get_user` | user.rs | serve-core | — | |
| `update_user_profile` | user.rs | serve-core | — | |
| `get_user_by_nip05` | user.rs | serve-core | — | |
| `search_users` | user.rs | serve-core | `LOWER(…) LIKE $2 ESCAPE '\'`; **`encode(pubkey, 'hex')`** for pubkey-prefix match; CASE-ranked ORDER BY | `encode(bytea,'hex')` → SQLite `lower(hex(pubkey))`; LIKE/ESCAPE portable |
| `set_agent_owner` | user.rs | serve-core | conditional UPDATE (`WHERE agent_owner_pubkey IS NULL`) — atomic claim | portable CAS |
| `get_agent_channel_policy` | user.rs | serve-core | — | |
| `is_agent_owner` | user.rs | serve-core | — | |
| `set_channel_add_policy` | user.rs | serve-core | — | |

## 10. DMs — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `find_dm_by_participants` | dm.rs | serve-core | — | keyed on app-computed `participant_hash` (SHA-256 in Rust — portable) |
| `create_dm` | dm.rs | serve-core | transaction: find-or-create channel + member rows; membership `ON CONFLICT … DO UPDATE` | race window resolved by tx; SQLite single-writer makes it trivial |
| `list_dms_for_user` | dm.rs | serve-core | — | keyset cursor |
| `open_dm` | dm.rs | serve-core | delegates to find/create; participant merge in Rust | |
| `hide_dm` | dm.rs | serve-core | `hidden_at = NOW()` | per-user hide table |
| `unhide_dm` | dm.rs | serve-core | — | |
| `list_hidden_dms` | dm.rs | serve-core | — | |

## 11. Threads — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `insert_thread_metadata` | thread.rs | serve-core | `ON CONFLICT DO NOTHING`; counter update `reply_count + 1, last_reply_at = NOW()`; rows_affected gating so duplicate inserts don't double-count | thread-counter materialization contract |
| `get_thread_replies` | thread.rs | serve-core | keyset pagination over `thread_metadata` join `events`; **Db wrapper contains replica-fence routing** (`self.read()`, `fence.covers`, writer re-verification of terminal pages) | routing logic is pg-only but degrades naturally: with no read pool the branch never triggers. SQLite arm = single query |
| `get_thread_summary` | thread.rs | serve-core | aggregate + top-participants (10-cap) | |
| `get_channel_window` | thread.rs | serve-core | **`ROW_NUMBER() OVER (PARTITION BY tm.root_event_id ORDER BY MAX(e.created_at) DESC)`** window fn for per-root participant caps; `root_event_id = ANY($2)` bytea[]; replica-fence routing in Db wrapper | SQLite ≥ 3.25 supports window functions — portable; `= ANY` needs rewrite |
| `get_thread_metadata_by_event` | thread.rs | serve-core | — | |
| `decrement_reply_count` | thread.rs | serve-core | `GREATEST`-style floor on counters (verify exact SQL when porting) | pairs with `soft_delete_event_and_update_thread` |

## 12. Reactions — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `add_reaction` | reaction.rs | serve-core | `ON CONFLICT (community_id, event_created_at, event_id, pubkey, emoji) DO UPDATE SET created_at = NOW() … WHERE removed_at IS NOT NULL` — TOCTOU-free re-activation | SQLite upsert supports conditional DO UPDATE … WHERE — portable |
| `remove_reaction` | reaction.rs | serve-core | `removed_at = NOW()` | |
| `remove_reaction_by_source_event_id` | reaction.rs | serve-core | `removed_at = NOW()` | |
| `get_active_reaction_record` | reaction.rs | serve-core | — | |
| `set_reaction_event_id` | reaction.rs | serve-core | — | backfill of source event id |
| `get_reactions` | reaction.rs | serve-core | GROUP BY emoji + cursor | |
| `get_reactions_bulk` | reaction.rs | serve-core | none — loops one query per event in Rust (explicitly not a composite-key IN) | trivially portable |

## 13. Feed — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `query_feed_mentions` | feed.rs | serve-core | join `events` × `event_mentions`; dynamic channel-id `IN` list | depends on `insert_mentions` denormalized table |
| `query_feed_needs_action` | feed.rs | serve-core | same join shape | |
| `query_feed_activity` | feed.rs | serve-core | channel-id `IN` list | |

## 14. API tokens — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `create_api_token` | api_token.rs | serve-core | scopes/channel_ids stored as jsonb (serde_json values) | SQLite: TEXT JSON columns; parse in Rust identically |
| `create_api_token_if_under_limit` | api_token.rs | serve-core | `INSERT … SELECT … WHERE (SELECT COUNT(*) …) < 10` atomic conditional insert; `expires_at > NOW()` | atomicity relies on statement-level snapshot; on SQLite single-writer this is equally atomic |
| `get_api_token_by_hash` | lib.rs (inline SQL) | serve-core | jsonb scopes parse | note: implemented inline in lib.rs, not api_token.rs |
| `get_api_token_by_hash_including_revoked` | api_token.rs | serve-core | — | |
| `touch_api_token` | lib.rs (inline SQL) | serve-core | `last_used_at = NOW()` | inline in lib.rs |
| `update_token_last_used` | lib.rs | serve-core | — | pure alias of `touch_api_token`; dispatches automatically once touch has an arm |
| `list_active_tokens` | lib.rs (inline SQL) | serve-core | jsonb scopes parse | inline in lib.rs |
| `list_tokens_by_owner` | api_token.rs | serve-core | — | |
| `revoke_token` | api_token.rs | serve-core | `revoked_at = NOW()` | |
| `revoke_all_tokens` | api_token.rs | serve-core | `revoked_at = NOW()` | |

## 15. Workflows + approvals — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `create_workflow` | workflow.rs | serve-core | `$6::jsonb` cast on definition | SQLite: store definition as TEXT |
| `upsert_workflow` | workflow.rs | serve-core | `ON CONFLICT (community_id, id) DO UPDATE … WHERE workflows.channel_id IS NOT DISTINCT FROM EXCLUDED.channel_id RETURNING id` | conditional upsert guard; `IS NOT DISTINCT FROM` → `IS` |
| `get_workflow` | workflow.rs | serve-core | — | |
| `list_channel_workflows` | workflow.rs | serve-core | — | |
| `list_enabled_channel_workflows` | workflow.rs | serve-core | — | |
| `list_all_enabled_workflows` | workflow.rs | serve-core | — | scheduler scan (cross-community by design) |
| `claim_scheduled_workflow_fire` | workflow.rs | serve-core | `ON CONFLICT (community_id, workflow_id, scheduled_for) DO NOTHING RETURNING` — first-pod-wins claim | single-node still needs the claim row (restart dedup + interval anchor); portable upsert |
| `latest_scheduled_workflow_fire` | workflow.rs | serve-core | — | DB-authoritative interval anchor |
| `attach_scheduled_workflow_run` | workflow.rs | serve-core | — | |
| `prune_scheduled_workflow_fires_before` | workflow.rs | serve-core | — | retention must exceed max interval (§5c test) |
| `update_workflow` | workflow.rs | serve-core | `$2::jsonb` cast | |
| `update_workflow_status` | workflow.rs | serve-core | — | |
| `set_workflow_enabled` | workflow.rs | serve-core | — | |
| `delete_workflow` | workflow.rs | serve-core | cascading delete of runs/approvals | FK cascade or explicit deletes |
| `delete_workflow_for_owner` | workflow.rs | serve-core | `RETURNING channel_id` | |
| `find_workflow_by_owner_and_name` | workflow.rs | serve-core | — | NIP-09 a-tag deletion path |
| `create_workflow_run` | workflow.rs | serve-core | jsonb trigger_context | |
| `get_workflow_run` | workflow.rs | serve-core | — | |
| `list_workflow_runs` | workflow.rs | serve-core | — | |
| `update_workflow_run` | workflow.rs | serve-core | `CASE WHEN … THEN NOW() ELSE started_at END` timestamp latching | |
| `create_approval` | workflow.rs | serve-core | — | |
| `get_approval` | workflow.rs | serve-core | token hashed in Rust before query | |
| `get_approval_by_stored_hash` | workflow.rs | serve-core | — | |
| `get_run_approvals` | workflow.rs | serve-core | — | |
| `update_approval` | workflow.rs | serve-core | `granted_at = CASE WHEN $4 = 'granted' THEN NOW() … END` | |
| `update_approval_by_stored_hash` | workflow.rs | serve-core | same CASE/NOW() | |

## 16. Partition management + PG data backfills — pg-only

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `ensure_future_partitions` | partition.rs | pg-only | `CREATE TABLE … PARTITION OF` DDL; `pg_catalog.pg_class`/`pg_namespace`/`relispartition` catalog checks; overlap-error handling for `*_p_future` catch-all | SQLite has no table partitioning and needs none |
| `backfill_d_tags` | lib.rs (inline) | pg-only | `jsonb_array_elements(tags)` subquery extracting d-tag | one-time backfill of legacy PG rows; a fresh SQLite schema writes `d_tag` at insert, nothing to backfill — classify pg-only (migration internals), SQLite arm may alternatively return `Ok(0)` |

## 17. Pubkey allowlist — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `is_pubkey_allowed` | lib.rs (inline SQL) | serve-core | — | |
| `has_allowlist_entries` | lib.rs (inline SQL) | serve-core | — | |
| `add_to_allowlist` | lib.rs (inline SQL) | serve-core | `ON CONFLICT DO NOTHING` | |
| `remove_from_allowlist` | lib.rs (inline SQL) | serve-core | — | |
| `list_allowlist` | lib.rs (inline SQL) | serve-core | — | |

## 18. Relay members — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `is_relay_member` | relay_members.rs | serve-core | — | |
| `get_relay_member` | relay_members.rs | serve-core | — | |
| `list_relay_members` | relay_members.rs | serve-core | — | |
| `add_relay_member` | relay_members.rs | serve-core | `ON CONFLICT (community_id, pubkey) DO NOTHING` | |
| `claim_relay_membership` | relay_members.rs | serve-core | transaction: member insert + `join_policy_acceptances` insert, both `ON CONFLICT DO NOTHING` | |
| `has_join_policy_acceptance` | relay_members.rs | serve-core | — | |
| `remove_relay_member` | relay_members.rs | serve-core | atomic conditional DELETE (refuses owner) | |
| `remove_relay_member_if_role` | relay_members.rs | serve-core | atomic conditional DELETE (role fence, no TOCTOU) | |
| `update_relay_member_role` | relay_members.rs | serve-core | `updated_at = now()` | |
| `bootstrap_owner` | relay_members.rs | serve-core | `ON CONFLICT (community_id, pubkey) DO UPDATE SET role = 'owner'`; demotes other admins | startup path |
| `transfer_ownership` | relay_members.rs | serve-core | transaction + `pg_advisory_xact_lock(owner_count_advisory_lock_key)` on transferee; **`SELECT … FOR UPDATE`** on current-owner row; expected-owner verification in-tx; `ON CONFLICT … DO UPDATE` promote + demote | `FOR UPDATE` is a no-op concept on SQLite (whole-db write lock); plain tx preserves semantics |
| `backfill_from_allowlist` | relay_members.rs | serve-core | `INSERT … SELECT` from `pubkey_allowlist` with `ON CONFLICT DO NOTHING`; tolerates missing source table | judgment call: startup migration helper kept serve-core because the relay calls it unconditionally at boot — SQLite arm can be a faithful port (both tables exist in the SQLite schema) or documented no-op returning 0 |

## 19. Product feedback — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `insert_product_feedback` | product_feedback.rs | serve-core | `ON CONFLICT (event_id) DO UPDATE … RETURNING id` (idempotent by event id) | |
| `list_product_feedback` | product_feedback.rs | serve-core | — | deployment-global list |

## 20. Moderation — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `insert_moderation_report` | moderation.rs | serve-core | `ON CONFLICT (community_id, report_event_id) DO UPDATE … RETURNING id` (idempotent re-ingest) | |
| `list_moderation_reports` | moderation.rs | serve-core | — | |
| `get_moderation_report` | moderation.rs | serve-core | — | |
| `get_moderation_report_by_event` | moderation.rs | serve-core | — | |
| `resolve_moderation_report` | moderation.rs | serve-core | `resolved_at = now()` | |
| `ban_community_member` | moderation.rs | serve-core | `ON CONFLICT (community_id, pubkey) DO UPDATE`; `updated_at = now()` | |
| `unban_community_member` | moderation.rs | serve-core | `updated_at = now()` | |
| `timeout_community_member` | moderation.rs | serve-core | `ON CONFLICT … DO UPDATE` | |
| `untimeout_community_member` | moderation.rs | serve-core | `WHERE … muted_until > now()` — active-timeout predicate uses DB clock | clock convention matters for expiry comparisons |
| `moderation_restriction_state` | moderation.rs | serve-core | `(banned AND (ban_expires_at IS NULL OR ban_expires_at > now()))`, `CASE WHEN muted_until > now()` — expiry evaluated in SQL against DB clock | enforcement hot path |
| `get_community_ban` | moderation.rs | serve-core | `now()` expiry projection | |
| `list_community_restrictions` | moderation.rs | serve-core | `now()` expiry predicates | |
| `insert_moderation_action` | moderation.rs | serve-core | `RETURNING id` | audit row |
| `list_moderation_actions` | moderation.rs | serve-core | — | |

## 21. Git repo name registry — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `repo_name_owner` | git_repo.rs | serve-core | — | |
| `reserve_repo_name` | git_repo.rs | serve-core | `ON CONFLICT (community_id, repo_id) DO NOTHING RETURNING owner_pubkey` — atomic claim with post-conflict re-read | portable upsert pattern |
| `count_repos_for_owner` | git_repo.rs | serve-core | — | quota check (caller-enforced) |
| `release_repo_name` | git_repo.rs | serve-core | — | |

## 22. Archived identities — serve-core

| method | submodule | classification | PG-specific features | notes |
|---|---|---|---|---|
| `is_archived` | archived_identities.rs | serve-core | — | |
| `archive` | archived_identities.rs | serve-core | `ON CONFLICT (community_id, pubkey) DO NOTHING` | |
| `unarchive` | archived_identities.rs | serve-core | — | |
| `list_archived` | archived_identities.rs | serve-core | — | |

---

## Counts per classification

| classification | count |
|---|---|
| serve-core | **177** |
| pg-only | **32** (replica fence/read pool 5, usage metrics 11, push 14, partitions+backfill 2) |
| infra | **7** (`new`, `from_pool`, `from_pools`, `migrate`, `ping`, `pool_stats`, `begin_transaction`) |
| **total `Db` methods** | **216** |

(218 grep hits − `insert_mentions` free fn − `UsageMetricsLeader::is_live` = 216. ✓)

---

## SQLite implementation order — 8 work packets (serve-core only)

Ordered by dependency. Each packet is independently landable behind the dispatch seam;
the seam itself (backend enum, `UnsupportedBackend` error type, backend-neutral
transaction guard replacing the leaked `sqlx::Transaction<Postgres>` in
`begin_transaction` / `CommandEventTx`) is a prerequisite for WP1.

**WP1 — Events + communities/tenancy (38 methods).** The storage core everything else
scopes through. Communities: `lookup_community_by_host`, `is_community_active`,
`lookup_community_by_host_for_management`, `list_communities_owned_by`,
`lookup_community_host`, `get_community_icon`, `set_community_icon`,
`ensure_configured_community`, `create_community_with_owner`,
`archive_community_owned_by`, `unarchive_community_owned_by`, `community_of_channel`,
`communities_of_channels`. Events: `persist_command_event`, `insert_event`,
`query_events`, `count_events`, `huddle_started_link_exists`,
`get_latest_global_replaceable`, `get_event_by_id`,
`get_event_by_id_including_deleted`, `soft_delete_event`, `soft_delete_by_coordinate`,
`soft_delete_event_and_update_thread`, `get_last_message_at`,
`get_last_message_at_bulk`, `get_events_by_ids`, `insert_event_with_thread_metadata`,
`insert_reaction_event_with_thread_metadata`, `query_due_reminders`,
`claim_due_reminder`, `claim_due_reminder_with_stamp`, `release_due_reminder`,
`soft_delete_discovery_events`, `replace_addressable_event`,
`nip43_membership_snapshot_needs_reconciliation`, `publish_nip43_membership_locked`,
`replace_parameterized_event`. Also ports the `insert_mentions` free function and the
NIP-RS watermark table. Hard parts: jsonb `tags @>` rewrite, `DISTINCT ON` rewrite,
advisory-lock removal, `CommandEventTx` seam.

**WP2 — Channels/membership + relay members + allowlist (43 methods).** Depends on
WP1 (communities, events for discovery kinds). All of section 8 (26), section 18 (12),
section 17 (5). Hard parts: TTL interval arithmetic, `json_agg` rewrite in
`get_bot_members`, TTL-trigger invariants moved into `reap_expired_ephemeral_channels`
query logic, `transfer_ownership` tx.

**WP3 — Users (9 methods).** Depends on WP1 (community scoping). Section 9. Hard part:
`encode(pubkey,'hex')` → `hex()` in `search_users`.

**WP4 — Threads + reactions (13 methods).** Depends on WP1 (events) and WP2 (channels).
Sections 11 (6) and 12 (7). Hard parts: window function in `get_channel_window`
(portable on SQLite ≥ 3.25), reply-counter materialization parity with WP1's
insert/delete paths, replica-routing branches in the `Db` wrappers must compile to the
single-pool path.

**WP5 — DMs (7 methods).** Depends on WP2 (channels/membership). Section 10. Low risk.

**WP6 — Moderation + admin plane (18 methods).** Depends on WP1 (events for report
event ids). Sections 20 (14) and 4 (4). Hard part: DB-clock expiry predicates
(`muted_until > now()`) — pick the clock convention consistently with WP1.

**WP7 — Workflows/approvals + API tokens (36 methods).** Workflows depend on WP2
(channel scoping); tokens depend only on WP1. Sections 15 (26) and 14 (10). Hard parts:
conditional upsert in `upsert_workflow`, scheduled-fire claim table, `CASE … NOW()`
timestamp latching, jsonb definition/scopes columns as TEXT.

**WP8 — The rest: feed, git repos, archived identities, product feedback (13 methods).**
Feed (3) depends on WP1's `event_mentions`; git repos (4), archived identities (4),
product feedback (2) are independent leaf tables. Sections 13, 21, 22, 19.

Packet totals: 38 + 43 + 9 + 13 + 7 + 18 + 36 + 13 = **177** serve-core methods. ✓

---

## Methods whose semantics could not be fully determined

No method's *classification* is in doubt, but the following carry residual uncertainty
that implementers should resolve against source before porting (flagged rather than
silently guessed):

1. **`reap_expired_ephemeral_channels` / `update_channel` TTL interplay** — the huddle
   TTL state machine is partly enforced by a migration-defined trigger holding
   `pg_advisory_xact_lock_shared(hashtextextended(...))` (asserted in
   `migration.rs` tests, lines ~858–880). I traced the crate-side EXCLUSIVE lock but
   not the full trigger body in `migrations/`. The SQLite arm must re-derive the
   complete invariant (no revive-after-expiry, 60-second grace transition) from the
   migration SQL, not just from `channel.rs`.
2. **`replace_parameterized_event` guard triggers** — behavior depends on migration
   0011's regex-coordinate hard-delete guard and the `buzz.nip_rs_hard_delete` GUC,
   plus migration 0009's event→mentions lock ordering. Classified and feature-noted
   above, but the exact guard-trigger conditions live in migration SQL I did not read
   line-by-line; the SQLite arm must encode equivalent checks in application code.
3. **`decrement_reply_count`** — I did not read the exact SQL (floor-at-zero vs
   unchecked decrement) in `thread.rs`; verify before porting.
4. **`begin_transaction` external callers** — classification (infra) is certain, but
   which buzz-relay call sites compose raw transactions (and therefore constrain the
   seam's transaction-abstraction design) was out of scope for this worksheet and must
   be inventoried in the seam-design task.

Judgment calls made explicitly (not uncertainty): `admin_*` (section 4) and
`backfill_from_allowlist` kept serve-core; `backfill_d_tags` put pg-only;
fence/read-pool accessors (section 2) marked pg-only but recommended as degenerate
no-ops rather than `UnsupportedBackend` since the relay serve path calls them
unconditionally.

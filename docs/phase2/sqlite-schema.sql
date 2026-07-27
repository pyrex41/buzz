-- ============================================================================
-- Buzz SQLite serve-path schema — embedded migration 0001 for the sqlite
-- backend of buzz-db (solo profile).
--
-- Scope: the relay SERVE PATH only. Translated from migrations/0001..0024
-- (Postgres), cross-checked against the SQL actually issued by
-- crates/buzz-db/src/*.rs and crates/buzz-audit/src/service.rs.
--
-- EXCLUDED (out of solo-profile scope, per phase-2 plan):
--   * push_leases, push_wake_outbox, push_gateway_challenges,
--     push_gateway_installations, push_gateway_delegations,
--     push_gateway_endpoint_quotas, push_gateway_delivery_auth_replays,
--     push_gateway_delivery_request_replays, push_match_queue
--     (migrations 0012, 0013, 0015, 0018, 0023 — push leases / wake outbox /
--     gateway authority)
--   * monthly partition management (buzz-db/src/partition.rs) — one events
--     table, no partitions
--   * replica-fence machinery (buzz-db/src/replica_fence.rs, migration 0021
--     created_at floor constraint trigger + buzz.created_at_floor GUC)
--   * mesh-status retention trigger (migration 0019) — head-only retention is
--     already enforced by the Rust replace path (see notes)
--
-- ── Global conventions (see sqlite-schema-notes.md for rationale) ───────────
--   TIMESTAMPTZ  -> INTEGER unix SECONDS (UTC). One convention, everywhere.
--                   chrono <-> i64 conversion happens at the Rust boundary.
--                   (events.not_before / events.delivered_at were already
--                   BIGINT unix seconds in Postgres — unchanged.)
--   UUID         -> TEXT, lowercase hyphenated (matches sqlx Uuid<->SQLite
--                   binding; debuggable with the sqlite3 CLI).
--   BYTEA        -> BLOB.
--   JSONB        -> TEXT holding JSON (JSON1 functions: json_each,
--                   json_extract, json_type).
--   enums        -> TEXT + CHECK (…IN (…)). Rust arm drops `::text` casts.
--   BOOLEAN      -> INTEGER 0/1 (sqlx maps bool <-> INTEGER).
--   VARCHAR(n)   -> TEXT (length caps enforced at the application layer).
--   tsvector+GIN -> no column; FTS5 external-content table events_fts below.
--   GENERATED IDENTITY -> INTEGER PRIMARY KEY AUTOINCREMENT.
--
-- ── Required connection PRAGMAs ─────────────────────────────────────────────
-- These are per-connection (except journal_mode, which is persistent) and
-- CANNOT be set from inside this migration (sqlx runs migrations inside a
-- transaction, where `PRAGMA foreign_keys` is a silent no-op). The sqlite
-- backend MUST configure them on every pooled connection via
-- SqliteConnectOptions:
--
--     PRAGMA journal_mode  = WAL;       -- .journal_mode(SqliteJournalMode::Wal)
--     PRAGMA synchronous   = NORMAL;    -- .synchronous(SqliteSynchronous::Normal)
--     PRAGMA busy_timeout  = 5000;      -- .busy_timeout(Duration::from_secs(5))
--     PRAGMA foreign_keys  = ON;        -- .foreign_keys(true)  ** REQUIRED **
--
-- Writers should use BEGIN IMMEDIATE transactions; that (plus SQLite's
-- single-writer model) replaces every pg_advisory_* lock in the Postgres arm.
--
-- ── Tenant-isolation invariant (migration.rs lint contract) ─────────────────
-- Preserved verbatim from the Postgres schema:
--   1. Every tenant-scoped table carries `community_id TEXT NOT NULL`.
--   2. Every PRIMARY KEY / UNIQUE / FK on a scoped table leads with
--      community_id (or the parent join carries the community tuple).
--   3. channels.community_id is immutable (trigger below).
--   4. Operator-global tables are named in _operator_global_tables, never
--      implied: communities, rate_limit_violations, product_feedback,
--      _operator_global_tables.
-- ============================================================================


-- ── Communities (OPERATOR-GLOBAL: the tenant registry) ──────────────────────
-- host is stored pre-normalized (ASCII-lowercase, no trailing dot, no default
-- port). COLLATE NOCASE UNIQUE replaces Postgres's UNIQUE ON lower(host)
-- belt-and-suspenders; upserts target ON CONFLICT (host).
-- The DEFAULT expression generates a v4 UUID so `INSERT (host) … RETURNING id`
-- (Db::ensure_configured_community) keeps working without a Rust-side id.

CREATE TABLE communities (
    id          TEXT PRIMARY KEY
                DEFAULT (lower(
                    hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                    substr(hex(randomblob(2)), 2) || '-' ||
                    substr('89ab', 1 + (abs(random()) % 4), 1) ||
                    substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                )),
    host        TEXT NOT NULL COLLATE NOCASE,
    signing_key BLOB,
    created_at  INTEGER NOT NULL DEFAULT (unixepoch()),
    icon        TEXT,                          -- migration 0003
    archived_at INTEGER,                       -- migration 0016
    CONSTRAINT chk_communities_id_not_nil
        CHECK (id <> '00000000-0000-0000-0000-000000000000')
);

CREATE UNIQUE INDEX idx_communities_host ON communities (host);


-- ── Users ───────────────────────────────────────────────────────────────────
-- One profile per (community, pubkey). Declared before channels/workflows so
-- their composite FKs can reference it.

CREATE TABLE users (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    pubkey              BLOB NOT NULL,
    nip05_handle        TEXT,
    display_name        TEXT,
    avatar_url          TEXT,
    about               TEXT,
    agent_type          TEXT,
    capabilities        TEXT,                  -- JSON
    okta_user_id        TEXT,
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    deactivated_at      INTEGER,
    metadata_event_id   BLOB,
    agent_owner_pubkey  BLOB,
    channel_add_policy  TEXT NOT NULL DEFAULT 'anyone'
                        CHECK (channel_add_policy IN ('anyone', 'owner_only', 'nobody')),
    PRIMARY KEY (community_id, pubkey),
    CONSTRAINT chk_users_pubkey_len CHECK (length(pubkey) = 32),
    FOREIGN KEY (community_id, agent_owner_pubkey)
        REFERENCES users (community_id, pubkey) ON DELETE SET NULL
);

-- lower() is ASCII-only in SQLite — acceptable: NIP-05 handles are validated
-- ASCII at the write path.
CREATE UNIQUE INDEX idx_users_nip05 ON users (community_id, lower(nip05_handle))
    WHERE nip05_handle IS NOT NULL;
CREATE UNIQUE INDEX idx_users_okta ON users (community_id, okta_user_id)
    WHERE okta_user_id IS NOT NULL;


-- ── Channels ────────────────────────────────────────────────────────────────
-- PK (community_id, id): the same channel UUID may exist in two communities.

CREATE TABLE channels (
    id              TEXT NOT NULL
                    DEFAULT (lower(
                        hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                        substr(hex(randomblob(2)), 2) || '-' ||
                        substr('89ab', 1 + (abs(random()) % 4), 1) ||
                        substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                    )),
    community_id    TEXT NOT NULL REFERENCES communities(id),
    name            TEXT NOT NULL,
    channel_type    TEXT NOT NULL DEFAULT 'stream'
                    CHECK (channel_type IN ('stream', 'forum', 'dm', 'workflow')),
    visibility      TEXT NOT NULL DEFAULT 'open'
                    CHECK (visibility IN ('open', 'private')),
    description     TEXT,
    canvas          TEXT,
    created_by      BLOB NOT NULL,
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    archived_at     INTEGER,
    deleted_at      INTEGER,
    nip29_group_id  TEXT,
    topic_required  INTEGER NOT NULL DEFAULT 0,
    max_members     INTEGER,
    topic           TEXT,
    topic_set_by    BLOB,
    topic_set_at    INTEGER,
    purpose         TEXT,
    purpose_set_by  BLOB,
    purpose_set_at  INTEGER,
    participant_hash BLOB,
    ttl_seconds     INTEGER,
    ttl_deadline    INTEGER,
    PRIMARY KEY (community_id, id),
    CONSTRAINT chk_channels_id_not_nil
        CHECK (id <> '00000000-0000-0000-0000-000000000000')
);

CREATE UNIQUE INDEX idx_channels_nip29_group ON channels (community_id, nip29_group_id)
    WHERE nip29_group_id IS NOT NULL;
CREATE UNIQUE INDEX idx_channels_dm_hash ON channels (community_id, participant_hash)
    WHERE participant_hash IS NOT NULL;
CREATE INDEX idx_channels_community_type ON channels (community_id, channel_type);
CREATE INDEX idx_channels_community_visibility ON channels (community_id, visibility);
CREATE INDEX idx_channels_created_by ON channels (community_id, created_by);
CREATE INDEX idx_channels_ttl_expiry ON channels (ttl_deadline)
    WHERE ttl_seconds IS NOT NULL AND archived_at IS NULL AND deleted_at IS NULL;

-- Lint invariant #3: a channel can never be re-tenanted. Replaces the
-- plpgsql channels_community_id_immutable() trigger.
CREATE TRIGGER trg_channels_community_id_immutable
BEFORE UPDATE OF community_id ON channels
WHEN NEW.community_id <> OLD.community_id
BEGIN
    SELECT RAISE(ABORT, 'channels.community_id is immutable');
END;


-- ── Channel members ─────────────────────────────────────────────────────────

CREATE TABLE channel_members (
    community_id TEXT NOT NULL REFERENCES communities(id),
    channel_id  TEXT NOT NULL,
    pubkey      BLOB NOT NULL,
    role        TEXT NOT NULL DEFAULT 'member'
                CHECK (role IN ('owner', 'admin', 'member', 'guest', 'bot')),
    joined_at   INTEGER NOT NULL DEFAULT (unixepoch()),
    invited_by  BLOB,
    removed_at  INTEGER,
    removed_by  BLOB,
    hidden_at   INTEGER,
    PRIMARY KEY (community_id, channel_id, pubkey),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_channel_members_pubkey ON channel_members (community_id, pubkey)
    WHERE removed_at IS NULL;


-- ── Events (single table — NO partitions) ───────────────────────────────────
-- Postgres partitions by month on created_at, which forces created_at into
-- the PK. Without partitions the natural dedup key is (community_id, id):
-- a Nostr event id commits to its created_at, so this is equivalent to the
-- Postgres (community_id, created_at, id) key for valid signed events, and
-- it directly serves the `WHERE community_id = ? AND id = ?` hot path.
-- Cross-community dedup semantics preserved: the same signed event may exist
-- once per community.
--
-- No search_tsv column — see events_fts (FTS5) below.
-- No tags GIN index — e-tag containment queries use json_each() in the
-- sqlite query arm (see notes).

CREATE TABLE events (
    community_id TEXT NOT NULL REFERENCES communities(id),
    id          BLOB NOT NULL,
    pubkey      BLOB NOT NULL,
    created_at  INTEGER NOT NULL,              -- event's signed unix seconds
    kind        INTEGER NOT NULL,
    tags        TEXT NOT NULL,                 -- JSON array of arrays
    content     TEXT NOT NULL,
    sig         BLOB NOT NULL,
    received_at INTEGER NOT NULL DEFAULT (unixepoch()),
    channel_id  TEXT,
    deleted_at  INTEGER,
    d_tag       TEXT,
    not_before  INTEGER,                       -- BIGINT in Postgres too
    delivered_at INTEGER,                      -- BIGINT in Postgres too
    PRIMARY KEY (community_id, id)
);

-- Hot-path composite indexes, all community-leading (query paths verified in
-- crates/buzz-db/src/event.rs / lib.rs / thread.rs / buzz-search).
CREATE INDEX idx_events_community_created
    ON events (community_id, created_at DESC, id);
CREATE INDEX idx_events_community_channel_created
    ON events (community_id, channel_id, created_at DESC, id);
CREATE INDEX idx_events_community_pubkey_kind_created
    ON events (community_id, pubkey, kind, created_at DESC, id);
CREATE INDEX idx_events_community_kind_created
    ON events (community_id, kind, created_at DESC, id);
CREATE INDEX idx_events_community_deleted
    ON events (community_id, deleted_at);
-- Addressable (relay-signed NIP-29 metadata) replacement key.
CREATE INDEX idx_events_addressable
    ON events (community_id, kind, pubkey, channel_id, deleted_at);
-- NIP-33 parameterized replacement key (replace_parameterized_event,
-- persist_command_event, soft_delete_by_coordinate).
CREATE INDEX idx_events_parameterized
    ON events (community_id, kind, pubkey, d_tag, created_at DESC, id)
    WHERE d_tag IS NOT NULL AND deleted_at IS NULL;
-- NIP-ER due-reminder scan (query_due_reminders).
CREATE INDEX idx_events_not_before
    ON events (community_id, not_before)
    WHERE not_before IS NOT NULL AND deleted_at IS NULL AND delivered_at IS NULL;

-- Ephemeral-channel TTL refresh. Replaces migrations 0022/0024's deferred
-- constraint trigger + per-channel advisory lock: SQLite is single-writer, so
-- a plain synchronous AFTER INSERT trigger has none of the Postgres races.
-- Kind 9007 creates the channel and initializes its own deadline.
CREATE TRIGGER trg_events_refresh_channel_ttl
AFTER INSERT ON events
WHEN NEW.channel_id IS NOT NULL AND NEW.kind <> 9007
BEGIN
    UPDATE channels
    SET ttl_deadline = unixepoch() + ttl_seconds
    WHERE community_id = NEW.community_id
      AND id = NEW.channel_id
      AND ttl_seconds IS NOT NULL
      AND archived_at IS NULL
      AND deleted_at IS NULL;
END;


-- ── Full-text search: FTS5 external-content table ───────────────────────────
-- Replaces `search_tsv TSVECTOR GENERATED … STORED` + GIN (migrations 0001 /
-- 0005 / 0008 / 0014). A SQLite deployment is definitionally a FRESH install,
-- so the migration-0008 positive allowlist applies: ONLY kinds
--   0     (profile metadata)
--   9     (stream message)
--   40002 (stream message v2)
--   45001 (forum post)
--   45003 (forum comment)
-- are indexed. Everything else (gift wrap 1059, reminders 30300, DM
-- visibility 30622, push leases 30350, membership notices 44100/44101, agent
-- turn metrics 44200, all ciphertext kinds) is storage-level unsearchable
-- because it is simply never inserted into the index.
--
-- tokenize='unicode61' ~ Postgres 'simple' config: case-folding + word
-- splitting, no stemming, no stopwords.
--
-- BUILD-TIME VERIFICATION ITEM: sqlx's bundled libsqlite3 must be compiled
-- with FTS5 (`SELECT 1 FROM pragma_compile_options WHERE
-- compile_options = 'ENABLE_FTS5'`). Assert this in a backend startup check
-- and a unit test before shipping.
--
-- WARNING: never run `INSERT INTO events_fts(events_fts) VALUES('rebuild')`.
-- Rebuild re-indexes EVERY events row unconditionally, bypassing the kind
-- allowlist below — a privacy regression (ciphertext becomes searchable).

CREATE VIRTUAL TABLE events_fts USING fts5(
    content,
    content='events',
    content_rowid='rowid',
    tokenize='unicode61'
);

-- Index maintenance. External-content FTS5 requires the application (here:
-- triggers) to mirror every row change; the 'delete' command must be passed
-- the exact previously-indexed text.
CREATE TRIGGER trg_events_fts_insert
AFTER INSERT ON events
WHEN NEW.kind IN (0, 9, 40002, 45001, 45003)
BEGIN
    INSERT INTO events_fts (rowid, content) VALUES (NEW.rowid, NEW.content);
END;

CREATE TRIGGER trg_events_fts_delete
AFTER DELETE ON events
WHEN OLD.kind IN (0, 9, 40002, 45001, 45003)
BEGIN
    INSERT INTO events_fts (events_fts, rowid, content)
    VALUES ('delete', OLD.rowid, OLD.content);
END;

CREATE TRIGGER trg_events_fts_update
AFTER UPDATE OF content ON events
WHEN NEW.kind IN (0, 9, 40002, 45001, 45003)
BEGIN
    INSERT INTO events_fts (events_fts, rowid, content)
    VALUES ('delete', OLD.rowid, OLD.content);
    INSERT INTO events_fts (rowid, content) VALUES (NEW.rowid, NEW.content);
END;


-- ── Event mentions (#p fan-out index) ───────────────────────────────────────
-- Joins to events MUST carry the community tuple
-- (e.community_id = m.community_id AND e.id = m.event_id).

CREATE TABLE event_mentions (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    pubkey_hex          TEXT NOT NULL,
    event_id            BLOB NOT NULL,
    event_created_at    INTEGER NOT NULL,
    channel_id          TEXT,
    event_kind          INTEGER,
    PRIMARY KEY (community_id, pubkey_hex, event_id)
);

CREATE INDEX idx_event_mentions_pubkey_created
    ON event_mentions (community_id, pubkey_hex, event_created_at DESC);
CREATE INDEX idx_event_mentions_pubkey_kind_created
    ON event_mentions (community_id, pubkey_hex, event_kind, event_created_at DESC);
-- Defensive mention cleanup on replacement (migration 0007).
CREATE INDEX idx_event_mentions_community_event
    ON event_mentions (community_id, event_id);


-- ── NIP-RS parameterized event watermarks (migration 0007) ──────────────────
-- Compact replay-ordering watermark for kind:30078 read-state coordinates.
-- The plpgsql guard triggers of migrations 0009/0010/0011 are OMITTED: their
-- only purpose was defending against pre-fix relay binaries during rolling
-- deploys. The solo-profile sqlite backend has exactly one writer — the
-- current binary — and the equivalent check already lives in Rust
-- (Db::replace_parameterized_event, crates/buzz-db/src/lib.rs).

CREATE TABLE parameterized_event_watermarks (
    community_id  TEXT NOT NULL REFERENCES communities(id),
    kind          INTEGER NOT NULL,
    pubkey        BLOB NOT NULL,
    d_tag         TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    event_id      BLOB NOT NULL,
    PRIMARY KEY (community_id, kind, pubkey, d_tag)
);


-- ── Thread metadata (reply_count / descendant_count materialization) ────────
-- Postgres keys on (community_id, event_created_at, event_id) because of
-- partitioning; every UPDATE path targets (community_id, event_id). Without
-- partitions the natural key is (community_id, event_id) — this also makes
-- the stub-insert ON CONFLICT DO NOTHING stricter (a same-event stub with a
-- differing timestamp can no longer create a second row and double-count).
-- event_created_at is retained as a payload column.

CREATE TABLE thread_metadata (
    community_id            TEXT NOT NULL REFERENCES communities(id),
    event_id                BLOB NOT NULL,
    event_created_at        INTEGER NOT NULL,
    channel_id              TEXT NOT NULL,
    parent_event_id         BLOB,
    parent_event_created_at INTEGER,
    root_event_id           BLOB,
    root_event_created_at   INTEGER,
    depth                   INTEGER NOT NULL DEFAULT 0,
    reply_count             INTEGER NOT NULL DEFAULT 0,
    descendant_count        INTEGER NOT NULL DEFAULT 0,
    last_reply_at           INTEGER,
    broadcast               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (community_id, event_id),
    FOREIGN KEY (community_id, channel_id) REFERENCES channels (community_id, id)
);

CREATE INDEX idx_thread_metadata_parent ON thread_metadata (community_id, parent_event_id);
CREATE INDEX idx_thread_metadata_root ON thread_metadata (community_id, root_event_id);
CREATE INDEX idx_thread_metadata_channel_depth
    ON thread_metadata (community_id, channel_id, depth, event_created_at);


-- ── Reactions ───────────────────────────────────────────────────────────────
-- PK kept byte-for-byte compatible with the Postgres arm's upsert target
-- ON CONFLICT (community_id, event_created_at, event_id, pubkey, emoji)
-- (crates/buzz-db/src/reaction.rs ADD_REACTION_SQL) so the statement ports
-- with only NOW() -> unixepoch().

CREATE TABLE reactions (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    event_created_at    INTEGER NOT NULL,
    event_id            BLOB NOT NULL,
    pubkey              BLOB NOT NULL,
    emoji               TEXT NOT NULL,
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    removed_at          INTEGER,
    reaction_event_id   BLOB,
    PRIMARY KEY (community_id, event_created_at, event_id, pubkey, emoji)
);

CREATE INDEX idx_reactions_event ON reactions (community_id, event_id, event_created_at);
CREATE INDEX idx_reactions_pubkey ON reactions (community_id, pubkey);
CREATE UNIQUE INDEX idx_reactions_source_event ON reactions (community_id, reaction_event_id)
    WHERE reaction_event_id IS NOT NULL;


-- ── Subscriptions + delivery log (SCHEMA PARITY ONLY) ───────────────────────
-- No serve-path Rust code queries these today (verified: zero references in
-- crates/). Carried so the sqlite schema stays 1:1 with the Postgres schema
-- minus push/mesh; safe to drop if the parity goal is abandoned.

CREATE TABLE subscriptions (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    id                  TEXT NOT NULL,
    owner_pubkey        BLOB NOT NULL,
    filter_kinds        TEXT,                  -- JSON
    filter_authors      TEXT,                  -- JSON
    filter_channel_ids  TEXT,                  -- JSON
    filter_since        INTEGER,
    filter_until        INTEGER,
    delivery_method     TEXT NOT NULL DEFAULT 'webhook'
                        CHECK (delivery_method IN ('webhook', 'websocket')),
    delivery_url        TEXT,
    status              TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paused', 'deleted')),
    pause_reason        TEXT
                        CHECK (pause_reason IS NULL OR pause_reason IN ('user', 'system', 'rate_limit')),
    delivered_count     INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, owner_pubkey) REFERENCES users (community_id, pubkey)
);

-- Partitioned-by-month in Postgres; single table here. IDENTITY -> rowid PK.
CREATE TABLE delivery_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id    TEXT NOT NULL REFERENCES communities(id),
    subscription_id TEXT,
    event_id        BLOB,
    method          TEXT CHECK (method IS NULL OR method IN ('webhook', 'websocket')),
    delivered_at    INTEGER NOT NULL DEFAULT (unixepoch()),
    success         INTEGER,
    http_status     INTEGER,
    error_message   TEXT,
    attempt_number  INTEGER DEFAULT 1
);

CREATE INDEX idx_delivery_log_delivered_at ON delivery_log (delivered_at);
CREATE INDEX idx_delivery_log_community_sub ON delivery_log (community_id, subscription_id);


-- ── Workflows ───────────────────────────────────────────────────────────────

CREATE TABLE workflows (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    id              TEXT NOT NULL
                    DEFAULT (lower(
                        hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                        substr(hex(randomblob(2)), 2) || '-' ||
                        substr('89ab', 1 + (abs(random()) % 4), 1) ||
                        substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                    )),
    name            TEXT NOT NULL,
    owner_pubkey    BLOB NOT NULL,
    channel_id      TEXT,
    definition      TEXT NOT NULL,             -- JSON
    definition_hash BLOB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'disabled', 'archived')),
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, owner_pubkey) REFERENCES users (community_id, pubkey),
    FOREIGN KEY (community_id, channel_id) REFERENCES channels (community_id, id)
);

CREATE INDEX idx_workflows_channel_active ON workflows (community_id, channel_id, status, enabled);
-- Scheduler scan; community_id returned per row for tenant-scoped side effects.
CREATE INDEX idx_workflows_enabled ON workflows (enabled, status) WHERE enabled = 1;


-- ── Workflow runs ───────────────────────────────────────────────────────────

CREATE TABLE workflow_runs (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    id                  TEXT NOT NULL
                        DEFAULT (lower(
                            hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                            substr(hex(randomblob(2)), 2) || '-' ||
                            substr('89ab', 1 + (abs(random()) % 4), 1) ||
                            substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                        )),
    workflow_id         TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'waiting_approval',
                                          'completed', 'failed', 'cancelled')),
    trigger_event_id    BLOB,
    current_step        INTEGER NOT NULL DEFAULT 0,
    execution_trace     TEXT NOT NULL DEFAULT '[]',   -- JSON
    trigger_context     TEXT,                          -- JSON
    started_at          INTEGER,
    completed_at        INTEGER,
    error_message       TEXT,
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_workflow_runs_workflow ON workflow_runs (community_id, workflow_id);
CREATE INDEX idx_workflow_runs_status ON workflow_runs (community_id, status);


-- ── Workflow approvals ──────────────────────────────────────────────────────

CREATE TABLE workflow_approvals (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    token           BLOB NOT NULL,
    workflow_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    step_id         TEXT NOT NULL,
    step_index      INTEGER NOT NULL,
    approver_spec   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'granted', 'denied', 'expired')),
    approver_pubkey BLOB,
    note            TEXT,
    granted_at      INTEGER,
    denied_at       INTEGER,
    expires_at      INTEGER NOT NULL,
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, token),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE,
    FOREIGN KEY (community_id, run_id)
        REFERENCES workflow_runs (community_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_workflow_approvals_workflow ON workflow_approvals (community_id, workflow_id);
CREATE INDEX idx_workflow_approvals_run ON workflow_approvals (community_id, run_id);
CREATE INDEX idx_workflow_approvals_status ON workflow_approvals (community_id, status);


-- ── Scheduled workflow fires (at-most-once cron claim) ──────────────────────
-- The claim's INSERT … ON CONFLICT DO NOTHING RETURNING pattern
-- (workflow.rs:508) works unchanged in SQLite.

CREATE TABLE scheduled_workflow_fires (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    workflow_id     TEXT NOT NULL,
    scheduled_for   INTEGER NOT NULL,
    claimed_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    workflow_run_id TEXT,
    PRIMARY KEY (community_id, workflow_id, scheduled_for),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE,
    FOREIGN KEY (community_id, workflow_run_id)
        REFERENCES workflow_runs (community_id, id)
);

CREATE INDEX idx_scheduled_fires_claimed_at ON scheduled_workflow_fires (claimed_at);


-- ── API tokens ──────────────────────────────────────────────────────────────

CREATE TABLE api_tokens (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    id                  TEXT NOT NULL
                        DEFAULT (lower(
                            hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                            substr(hex(randomblob(2)), 2) || '-' ||
                            substr('89ab', 1 + (abs(random()) % 4), 1) ||
                            substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                        )),
    token_hash          BLOB NOT NULL,
    owner_pubkey        BLOB NOT NULL,
    name                TEXT NOT NULL,
    scopes              TEXT NOT NULL,          -- JSON
    channel_ids         TEXT,                   -- JSON
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    expires_at          INTEGER,
    last_used_at        INTEGER,
    revoked_at          INTEGER,
    revoked_by          BLOB,
    created_by_self_mint INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, owner_pubkey) REFERENCES users (community_id, pubkey),
    CONSTRAINT chk_api_tokens_hash_len CHECK (length(token_hash) = 32)
);

CREATE UNIQUE INDEX idx_api_tokens_hash ON api_tokens (community_id, token_hash);


-- ── Rate limit violations (OPERATOR-GLOBAL; parity only) ────────────────────

CREATE TABLE rate_limit_violations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id    TEXT,
    pubkey          BLOB,
    violation_at    INTEGER NOT NULL DEFAULT (unixepoch()),
    limit_type      TEXT,
    limit_value     INTEGER,
    actual_value    INTEGER,
    action_taken    TEXT
);


-- ── Pubkey allowlist ────────────────────────────────────────────────────────

CREATE TABLE pubkey_allowlist (
    community_id TEXT NOT NULL REFERENCES communities(id),
    pubkey      BLOB NOT NULL,
    added_by    BLOB,
    added_at    INTEGER NOT NULL DEFAULT (unixepoch()),
    note        TEXT,
    PRIMARY KEY (community_id, pubkey)
);


-- ── Relay members (NIP-43) ──────────────────────────────────────────────────
-- pubkey stored as hex TEXT (wire form), unchanged.

CREATE TABLE relay_members (
    community_id TEXT NOT NULL REFERENCES communities(id),
    pubkey      TEXT NOT NULL,
    role        TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member')),
    added_by    TEXT,
    created_at  INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at  INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, pubkey)
);

CREATE INDEX idx_relay_members_role ON relay_members (community_id, role);


-- ── Join-policy acceptances (migration 0020) ────────────────────────────────

CREATE TABLE join_policy_acceptances (
    community_id TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    policy_version TEXT NOT NULL CHECK (length(policy_version) = 64),
    accepted_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, pubkey, policy_version),
    FOREIGN KEY (community_id, pubkey)
        REFERENCES relay_members (community_id, pubkey) ON DELETE CASCADE
);


-- ── Archived identities (NIP-IA) ────────────────────────────────────────────

CREATE TABLE archived_identities (
    community_id      TEXT NOT NULL REFERENCES communities(id),
    pubkey            TEXT NOT NULL,
    consent_path      TEXT NOT NULL CHECK (consent_path IN ('self', 'owner', 'admin')),
    actor             TEXT NOT NULL,
    reason            TEXT,
    replaced_by       TEXT,
    request_event_id  TEXT NOT NULL,
    archived_at       INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, pubkey)
);


-- ── Audit log (per-community hash chain) ────────────────────────────────────
-- The Postgres arm serializes appends with pg_advisory_lock per community
-- (buzz-audit/src/service.rs). SQLite's single-writer + BEGIN IMMEDIATE
-- provides the same serialization for free.

CREATE TABLE audit_log (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    seq             INTEGER NOT NULL,
    hash            BLOB NOT NULL,
    prev_hash       BLOB,
    action          TEXT NOT NULL,
    actor_pubkey    BLOB,
    object_id       TEXT,
    detail          TEXT,                      -- JSON
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, seq)
);

CREATE UNIQUE INDEX idx_audit_log_hash ON audit_log (community_id, hash);


-- ── Git repo name registry (NIP-34, migration 0002) ─────────────────────────

CREATE TABLE git_repo_names (
    community_id  TEXT NOT NULL REFERENCES communities(id),
    repo_id       TEXT NOT NULL,
    owner_pubkey  TEXT NOT NULL,
    created_at    INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, repo_id)
);

CREATE INDEX idx_git_repo_names_owner ON git_repo_names (community_id, owner_pubkey);


-- ── Moderation (migration 0006) ─────────────────────────────────────────────
-- moderation_actions is declared BEFORE moderation_reports so the
-- reports.action_id FK can be inline (Postgres added it via ALTER TABLE,
-- which SQLite does not support for constraints).

CREATE TABLE moderation_actions (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    id              TEXT NOT NULL
                    DEFAULT (lower(
                        hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                        substr(hex(randomblob(2)), 2) || '-' ||
                        substr('89ab', 1 + (abs(random()) % 4), 1) ||
                        substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                    )),
    actor_pubkey    BLOB NOT NULL CHECK (length(actor_pubkey) = 32),
    action          TEXT NOT NULL CHECK (action IN (
                        'delete_message', 'kick', 'ban', 'unban',
                        'timeout', 'untimeout', 'dismiss_report', 'escalate',
                        'resolve:delete', 'resolve:kick', 'resolve:ban',
                        'resolve:timeout')),
    target_pubkey   BLOB CHECK (target_pubkey IS NULL OR length(target_pubkey) = 32),
    target_event_id BLOB CHECK (target_event_id IS NULL OR length(target_event_id) = 32),
    channel_id      TEXT,
    reason_code     TEXT,
    public_reason   TEXT,
    private_reason  TEXT,
    matched_principal TEXT CHECK (matched_principal IS NULL OR matched_principal IN ('self', 'owner')),
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, channel_id) REFERENCES channels (community_id, id)
);

CREATE INDEX idx_moderation_actions_created
    ON moderation_actions (community_id, created_at DESC);
CREATE INDEX idx_moderation_actions_target_pubkey
    ON moderation_actions (community_id, target_pubkey)
    WHERE target_pubkey IS NOT NULL;

CREATE TABLE moderation_reports (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    id                  TEXT NOT NULL
                        DEFAULT (lower(
                            hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
                            substr(hex(randomblob(2)), 2) || '-' ||
                            substr('89ab', 1 + (abs(random()) % 4), 1) ||
                            substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
                        )),
    report_event_id     BLOB NOT NULL CHECK (length(report_event_id) = 32),
    reporter_pubkey     BLOB NOT NULL CHECK (length(reporter_pubkey) = 32),
    target_kind         TEXT NOT NULL CHECK (target_kind IN ('event', 'pubkey', 'blob')),
    target_event_id     BLOB CHECK (target_event_id IS NULL OR length(target_event_id) = 32),
    target_pubkey       BLOB CHECK (target_pubkey IS NULL OR length(target_pubkey) = 32),
    target_blob_sha256  BLOB CHECK (target_blob_sha256 IS NULL OR length(target_blob_sha256) = 32),
    channel_id          TEXT,
    report_type         TEXT NOT NULL,
    note                TEXT,
    status              TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'resolved', 'dismissed', 'escalated')),
    resolved_by         BLOB,
    resolved_at         INTEGER,
    action_id           TEXT,
    created_at          INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, id),
    CHECK (
        (target_kind = 'event'  AND target_event_id IS NOT NULL AND target_pubkey IS NULL     AND target_blob_sha256 IS NULL) OR
        (target_kind = 'pubkey' AND target_event_id IS NULL     AND target_pubkey IS NOT NULL AND target_blob_sha256 IS NULL) OR
        (target_kind = 'blob'   AND target_event_id IS NULL     AND target_pubkey IS NULL     AND target_blob_sha256 IS NOT NULL)
    ),
    FOREIGN KEY (community_id, channel_id) REFERENCES channels (community_id, id),
    FOREIGN KEY (community_id, action_id) REFERENCES moderation_actions (community_id, id)
);

CREATE INDEX idx_moderation_reports_status
    ON moderation_reports (community_id, status, created_at DESC);
CREATE INDEX idx_moderation_reports_target_event
    ON moderation_reports (community_id, target_event_id)
    WHERE target_event_id IS NOT NULL;
CREATE INDEX idx_moderation_reports_target_pubkey
    ON moderation_reports (community_id, target_pubkey)
    WHERE target_pubkey IS NOT NULL;
CREATE UNIQUE INDEX idx_moderation_reports_event
    ON moderation_reports (community_id, report_event_id);

CREATE TABLE community_bans (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    pubkey          BLOB NOT NULL CHECK (length(pubkey) = 32),
    banned          INTEGER NOT NULL DEFAULT 0,
    ban_expires_at  INTEGER,
    ban_reason      TEXT,
    muted_until     INTEGER,
    mute_reason     TEXT,
    actor_pubkey    BLOB NOT NULL CHECK (length(actor_pubkey) = 32),
    created_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at      INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (community_id, pubkey)
);


-- ── Product feedback (OPERATOR-GLOBAL inbox, migration 0017) ────────────────

CREATE TABLE product_feedback (
    id TEXT PRIMARY KEY
       DEFAULT (lower(
           hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
           substr(hex(randomblob(2)), 2) || '-' ||
           substr('89ab', 1 + (abs(random()) % 4), 1) ||
           substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))
       )),
    community_id TEXT NOT NULL REFERENCES communities(id),
    event_id BLOB NOT NULL CHECK (length(event_id) = 32),
    submitter_pubkey BLOB NOT NULL CHECK (length(submitter_pubkey) = 32),
    category TEXT CHECK (category IS NULL OR category IN ('bug', 'praise', 'needs-work')),
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    tags TEXT NOT NULL DEFAULT '[]' CHECK (json_type(tags) = 'array'),
    event_created_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE (event_id)
);

CREATE INDEX idx_product_feedback_received
    ON product_feedback (received_at DESC, id);
CREATE INDEX idx_product_feedback_community_received
    ON product_feedback (community_id, received_at DESC, id);


-- ── Lint allowlist registry ─────────────────────────────────────────────────
-- Registry of deliberately operator-global tables. Any table NOT listed here
-- MUST carry a NOT NULL community_id and lead its uniques with it.

CREATE TABLE _operator_global_tables (
    table_name  TEXT PRIMARY KEY,
    reason      TEXT NOT NULL
);

INSERT INTO _operator_global_tables (table_name, reason) VALUES
    ('communities',            'the tenant registry itself; id IS the community key'),
    ('rate_limit_violations',  'deployment abuse/health; never tenant-observable; community_id is an attribution label only'),
    ('product_feedback',       'deployment product inbox; community_id is provenance only'),
    ('_operator_global_tables', 'the registry table itself');

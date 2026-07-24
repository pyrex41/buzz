-- ============================================================================
-- Buzz SQLite migration 0002 — audit hash chain (Solo profile).
--
-- The consolidated 0001 schema (schema.sql) already ships this exact
-- `audit_log` DDL, so on any database initialized from it this migration is a
-- pure no-op (`IF NOT EXISTS`). It exists as the version-gated record that the
-- audit chain is part of the sqlite schema contract from version 2 onward:
-- `AuditService::new_sqlite` (buzz-audit) requires it, and a v1 database from
-- any lineage that lacked the table is brought up to shape here rather than
-- failing at the first audit append.
--
-- Mirrors the Postgres `audit_log` DDL from migrations/0001_initial_schema.sql
-- under the sqlite conventions established by schema.sql (0001):
--   UUID        -> TEXT lowercase hyphenated
--   BIGINT      -> INTEGER (i64)
--   BYTEA       -> BLOB
--   VARCHAR(64) -> TEXT (length cap enforced at the application layer)
--   JSONB       -> TEXT holding JSON
--   TIMESTAMPTZ -> INTEGER unix seconds (UTC)
--
-- Hash-input parity: buzz-audit's compute_hash() is backend-neutral and hashes
-- the decoded Rust `AuditEntry`, so the only obligation on this schema is that
-- every stored column round-trips to exactly the value that was hashed. The
-- one lossy column is created_at (seconds precision here vs microseconds in
-- Postgres); the sqlite arm of AuditService therefore truncates created_at to
-- whole seconds BEFORE hashing, keeping stored == hashed.
--
-- Tenant-isolation invariant preserved: community_id NOT NULL leads the PK and
-- the unique hash index, and each community's chain (seq, prev_hash) is fully
-- independent — exactly the Postgres semantics.
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_log_hash ON audit_log (community_id, hash);

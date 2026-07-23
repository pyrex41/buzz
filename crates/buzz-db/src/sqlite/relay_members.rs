//! SQLite arms for NIP-43 relay membership, the join-policy acceptance
//! ledger, and the pubkey allowlist (WP2, Phase 2d).
//!
//! Function-for-function port of [`crate::relay_members`] plus the allowlist
//! operations implemented inline on `Db` in `lib.rs`. Semantics match the
//! Postgres arm: every predicate leads with `community_id` (NIP-43 admission
//! confinement), membership inserts are idempotent (`ON CONFLICT DO
//! NOTHING`), the owner row can never be deleted or demoted by the generic
//! paths, and the claim/transfer flows keep their count/ownership invariants.
//!
//! Where Postgres serialized with `pg_advisory_xact_lock` +
//! `SELECT … FOR UPDATE` (`transfer_ownership`'s per-transferee lock and
//! owner-row lock), the SQLite arm uses a plain `BEGIN IMMEDIATE`
//! transaction: SQLite has exactly one writer, so the immediate write lock
//! serializes the whole read-verify-write sequence — concurrent transfers or
//! transfer-vs-create races cannot interleave, which is all the advisory
//! lock bought on Postgres.
#![allow(dead_code)] // Called via the Db backend-dispatch seam (orchestrator-owned lib.rs); allow until every arm is wired.

use sqlx::{Row, SqlitePool};

use super::channel::{begin_immediate, datetime_from_unix};
use crate::error::Result;
use crate::relay_members::{RelayMember, RemoveResult, TransferResult, MAX_COMMUNITIES_PER_OWNER};
use crate::{AllowlistEntry, CommunityId};

fn row_to_relay_member(row: sqlx::sqlite::SqliteRow) -> Result<RelayMember> {
    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    Ok(RelayMember {
        pubkey: row.try_get("pubkey")?,
        role: row.try_get("role")?,
        added_by: row.try_get("added_by")?,
        created_at: datetime_from_unix(created_at)?,
        updated_at: datetime_from_unix(updated_at)?,
    })
}

/// Returns `true` if `pubkey` (64-char hex) is a member of `community`.
pub(crate) async fn is_relay_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM relay_members WHERE community_id = ?1 AND pubkey = ?2")
        .bind(community.as_uuid().hyphenated())
        .bind(pubkey)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Returns the relay member record for `pubkey` in `community`, or `None`.
pub(crate) async fn get_relay_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
) -> Result<Option<RelayMember>> {
    let row = sqlx::query(
        "SELECT pubkey, role, added_by, created_at, updated_at \
         FROM relay_members WHERE community_id = ?1 AND pubkey = ?2",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .fetch_optional(pool)
    .await?;
    row.map(row_to_relay_member).transpose()
}

/// Returns all relay members of `community` ordered by `created_at` ascending.
pub(crate) async fn list_relay_members(
    pool: &SqlitePool,
    community: CommunityId,
) -> Result<Vec<RelayMember>> {
    let rows = sqlx::query(
        "SELECT pubkey, role, added_by, created_at, updated_at \
         FROM relay_members WHERE community_id = ?1 ORDER BY created_at ASC",
    )
    .bind(community.as_uuid().hyphenated())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_relay_member).collect()
}

/// Adds a new relay member to `community`.
///
/// Returns `true` if the row was actually inserted, `false` if the pubkey
/// already existed in this community (idempotent).
pub(crate) async fn add_relay_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
    role: &str,
    added_by: Option<&str>,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO relay_members (community_id, pubkey, role, added_by) \
         VALUES (?1, ?2, ?3, ?4) ON CONFLICT (community_id, pubkey) DO NOTHING",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .bind(role)
    .bind(added_by)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Claims relay membership via an invite and atomically persists policy
/// acceptance evidence in the same transaction.
///
/// Returns `true` when membership was inserted, `false` when the pubkey was
/// already a member (the acceptance record is still written either way).
pub(crate) async fn claim_relay_membership(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
    role: &str,
    policy_version: Option<&str>,
) -> Result<bool> {
    let mut tx = begin_immediate(pool).await?;
    let inserted = sqlx::query(
        "INSERT INTO relay_members (community_id, pubkey, role, added_by) \
         VALUES (?1, ?2, ?3, 'invite') \
         ON CONFLICT (community_id, pubkey) DO NOTHING",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .bind(role)
    .execute(tx.as_mut())
    .await?
    .rows_affected()
        > 0;

    if let Some(version) = policy_version {
        sqlx::query(
            "INSERT INTO join_policy_acceptances (community_id, pubkey, policy_version) \
             VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
        )
        .bind(community.as_uuid().hyphenated())
        .bind(pubkey)
        .bind(version)
        .execute(tx.as_mut())
        .await?;
    }

    tx.commit().await?;
    Ok(inserted)
}

/// Returns whether a member has persisted acceptance evidence for a policy
/// version.
pub(crate) async fn has_join_policy_acceptance(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
    policy_version: &str,
) -> Result<bool> {
    let row = sqlx::query(
        "SELECT 1 FROM join_policy_acceptances \
         WHERE community_id = ?1 AND pubkey = ?2 AND policy_version = ?3",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .bind(policy_version)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Removes a relay member atomically, refusing to delete the owner.
///
/// A single conditional `DELETE … WHERE role <> 'owner'` keeps the
/// owner-protection check and the delete one atomic statement (no TOCTOU).
pub(crate) async fn remove_relay_member(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
) -> Result<RemoveResult> {
    let result = sqlx::query(
        "DELETE FROM relay_members \
         WHERE community_id = ?1 AND pubkey = ?2 AND role <> 'owner'",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        return Ok(RemoveResult::Removed);
    }

    // rows_affected == 0: either not found or is owner. One cheap read to
    // distinguish the two cases (diagnostic only, as on Postgres).
    let exists = sqlx::query("SELECT 1 FROM relay_members WHERE community_id = ?1 AND pubkey = ?2")
        .bind(community.as_uuid().hyphenated())
        .bind(pubkey)
        .fetch_optional(pool)
        .await?;

    if exists.is_some() {
        Ok(RemoveResult::IsOwner)
    } else {
        Ok(RemoveResult::NotFound)
    }
}

/// Removes a relay member only if their current role matches `expected_role`
/// (atomic role-fenced delete — no TOCTOU).
///
/// Returns `Removed`, `NotFound`, `IsOwner`, or `RoleMismatch` (role changed
/// between the caller's read and this delete).
pub(crate) async fn remove_relay_member_if_role(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
    expected_role: &str,
) -> Result<RemoveResult> {
    let result = sqlx::query(
        "DELETE FROM relay_members WHERE community_id = ?1 AND pubkey = ?2 AND role = ?3",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .bind(expected_role)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        return Ok(RemoveResult::Removed);
    }

    let row = sqlx::query("SELECT role FROM relay_members WHERE community_id = ?1 AND pubkey = ?2")
        .bind(community.as_uuid().hyphenated())
        .bind(pubkey)
        .fetch_optional(pool)
        .await?;

    match row {
        None => Ok(RemoveResult::NotFound),
        Some(r) => {
            let role: String = r.try_get("role")?;
            if role == "owner" {
                Ok(RemoveResult::IsOwner)
            } else {
                Ok(RemoveResult::RoleMismatch)
            }
        }
    }
}

/// Updates the role of an existing relay member (never the owner). Returns
/// `true` if a row was updated.
pub(crate) async fn update_relay_member_role(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &str,
    new_role: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE relay_members SET role = ?1, updated_at = unixepoch() \
         WHERE community_id = ?2 AND pubkey = ?3 AND role <> 'owner'",
    )
    .bind(new_role)
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Ensures the configured owner pubkey holds the `"owner"` role in
/// `community` and demotes any other owners in that community to `"admin"`.
/// Idempotent; scoped to one community; runs in one immediate transaction.
///
/// Deployment-root authority exception (as on Postgres): this startup path
/// deliberately does NOT enforce [`MAX_COMMUNITIES_PER_OWNER`].
pub(crate) async fn bootstrap_owner(
    pool: &SqlitePool,
    community: CommunityId,
    owner_pubkey: &str,
) -> Result<()> {
    let pubkey = owner_pubkey.to_ascii_lowercase();
    let mut tx = begin_immediate(pool).await?;

    sqlx::query(
        "INSERT INTO relay_members (community_id, pubkey, role, added_by) \
         VALUES (?1, ?2, 'owner', NULL) \
         ON CONFLICT (community_id, pubkey) DO UPDATE SET role = 'owner', updated_at = unixepoch()",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(&pubkey)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        "UPDATE relay_members SET role = 'admin', updated_at = unixepoch() \
         WHERE community_id = ?1 AND role = 'owner' AND pubkey <> ?2",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(&pubkey)
    .execute(tx.as_mut())
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Atomically transfers ownership of `community` to `new_owner_pubkey`.
///
/// Runs entirely inside one `BEGIN IMMEDIATE` transaction, which replaces
/// both Postgres serialization devices (the per-transferee
/// `pg_advisory_xact_lock` shared with `create_community_with_owner`, and
/// the `SELECT … FOR UPDATE` on the current owner row): SQLite's single
/// global writer means no other write transaction can interleave with the
/// verify-count-promote-demote sequence, so the [`MAX_COMMUNITIES_PER_OWNER`]
/// count check and the stale-owner (`expected_owner_pubkey`) check keep
/// their atomicity guarantees.
///
/// Demotes every other owner in this community to `member` (not `admin`),
/// matching the Postgres arm's product decision.
pub(crate) async fn transfer_ownership(
    pool: &SqlitePool,
    community: CommunityId,
    new_owner_pubkey: &str,
    expected_owner_pubkey: &str,
) -> Result<TransferResult> {
    let pubkey = new_owner_pubkey.to_ascii_lowercase();
    let expected_owner = expected_owner_pubkey.to_ascii_lowercase();
    let mut tx = begin_immediate(pool).await?;

    let existing_owners: Vec<String> = sqlx::query_scalar(
        "SELECT pubkey FROM relay_members WHERE community_id = ?1 AND role = 'owner'",
    )
    .bind(community.as_uuid().hyphenated())
    .fetch_all(tx.as_mut())
    .await?;

    if existing_owners.is_empty() {
        tx.rollback().await?;
        return Ok(TransferResult::NoOwner);
    }

    // Stale-owner guard: a concurrent transfer or rotation already changed
    // hands if the expected owner is no longer among the current owners.
    if !existing_owners.iter().any(|p| p == &expected_owner) {
        tx.rollback().await?;
        return Ok(TransferResult::OwnerConflict);
    }

    // Already the sole owner — no transfer needed.
    if existing_owners.len() == 1 && existing_owners[0] == pubkey {
        tx.rollback().await?;
        return Ok(TransferResult::AlreadyOwner);
    }

    let previous_owner = if existing_owners.len() == 1 {
        Some(existing_owners[0].clone())
    } else {
        existing_owners.iter().find(|p| **p != pubkey).cloned()
    };

    // Authoritative per-transferee ownership-count check, inside the same
    // write transaction (deliberately cross-community, as on Postgres).
    let owned_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM relay_members WHERE pubkey = ?1 AND role = 'owner'",
    )
    .bind(&pubkey)
    .fetch_one(tx.as_mut())
    .await?;

    if owned_count >= MAX_COMMUNITIES_PER_OWNER {
        tx.rollback().await?;
        return Ok(TransferResult::LimitReached);
    }

    sqlx::query(
        "INSERT INTO relay_members (community_id, pubkey, role, added_by) \
         VALUES (?1, ?2, 'owner', NULL) \
         ON CONFLICT (community_id, pubkey) DO UPDATE SET role = 'owner', updated_at = unixepoch()",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(&pubkey)
    .execute(tx.as_mut())
    .await?;

    sqlx::query(
        "UPDATE relay_members SET role = 'member', updated_at = unixepoch() \
         WHERE community_id = ?1 AND role = 'owner' AND pubkey <> ?2",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(&pubkey)
    .execute(tx.as_mut())
    .await?;

    tx.commit().await?;
    Ok(TransferResult::Transferred { previous_owner })
}

/// Migrates existing `pubkey_allowlist` entries into `relay_members` for
/// `community` (BYTEA→hex conversion becomes `lower(hex(pubkey))`).
///
/// Returns the number of rows inserted, or 0 if the source table is missing
/// or `relay_members` already has rows for this community (the empty-table
/// guard prevents re-adding intentionally removed members).
pub(crate) async fn backfill_from_allowlist(
    pool: &SqlitePool,
    community: CommunityId,
) -> Result<u64> {
    // The consolidated SQLite schema always creates pubkey_allowlist, but the
    // Postgres arm tolerates its absence — keep the same defensive check.
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = 'pubkey_allowlist')",
    )
    .fetch_one(pool)
    .await?;
    if exists == 0 {
        return Ok(0);
    }

    let mut tx = begin_immediate(pool).await?;

    let has_members: i64 =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM relay_members WHERE community_id = ?1)")
            .bind(community.as_uuid().hyphenated())
            .fetch_one(tx.as_mut())
            .await?;
    if has_members != 0 {
        tx.rollback().await?;
        return Ok(0);
    }

    let result = sqlx::query(
        "INSERT INTO relay_members (community_id, pubkey, role, added_by, created_at) \
         SELECT ?1, lower(hex(pubkey)), 'member', NULL, added_at \
         FROM pubkey_allowlist \
         WHERE community_id = ?1 \
         ON CONFLICT (community_id, pubkey) DO NOTHING",
    )
    .bind(community.as_uuid().hyphenated())
    .execute(tx.as_mut())
    .await?;

    tx.commit().await?;
    Ok(result.rows_affected())
}

// ── Pubkey allowlist (inline on `Db` in the Postgres arm) ───────────────────

/// Check if a pubkey (32-byte compressed form) is in the allowlist for
/// `community`.
pub(crate) async fn is_pubkey_allowed(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
) -> Result<bool> {
    let cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pubkey_allowlist WHERE community_id = ?1 AND pubkey = ?2",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .fetch_one(pool)
    .await?;
    Ok(cnt > 0)
}

/// Check if the community allowlist has any entries (i.e. enforcement is
/// active).
pub(crate) async fn has_allowlist_entries(
    pool: &SqlitePool,
    community: CommunityId,
) -> Result<bool> {
    let cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pubkey_allowlist WHERE community_id = ?1")
            .bind(community.as_uuid().hyphenated())
            .fetch_one(pool)
            .await?;
    Ok(cnt > 0)
}

/// Add a pubkey to the community allowlist. Returns `true` if inserted,
/// `false` if it was already present (idempotent).
pub(crate) async fn add_to_allowlist(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
    added_by: &[u8],
    note: Option<&str>,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO pubkey_allowlist (community_id, pubkey, added_by, note) \
         VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
    )
    .bind(community.as_uuid().hyphenated())
    .bind(pubkey)
    .bind(added_by)
    .bind(note)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Remove a pubkey from the community allowlist. Returns `true` if a row was
/// deleted.
pub(crate) async fn remove_from_allowlist(
    pool: &SqlitePool,
    community: CommunityId,
    pubkey: &[u8],
) -> Result<bool> {
    let result =
        sqlx::query("DELETE FROM pubkey_allowlist WHERE community_id = ?1 AND pubkey = ?2")
            .bind(community.as_uuid().hyphenated())
            .bind(pubkey)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() > 0)
}

/// List all pubkeys in the community allowlist, newest first.
pub(crate) async fn list_allowlist(
    pool: &SqlitePool,
    community: CommunityId,
) -> Result<Vec<AllowlistEntry>> {
    let rows = sqlx::query(
        "SELECT pubkey, added_by, added_at, note FROM pubkey_allowlist \
         WHERE community_id = ?1 ORDER BY added_at DESC",
    )
    .bind(community.as_uuid().hyphenated())
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let added_at: i64 = row.try_get("added_at")?;
        out.push(AllowlistEntry {
            pubkey: row.try_get("pubkey")?,
            added_by: row.try_get("added_by")?,
            added_at: datetime_from_unix(added_at)?,
            note: row.try_get("note")?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr as _;
    use uuid::Uuid;

    async fn test_pool() -> SqlitePool {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .expect("options")
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn make_community(pool: &SqlitePool) -> CommunityId {
        let id = Uuid::new_v4();
        let host = format!("relay-members-test-{}.example", id.simple());
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.hyphenated())
            .bind(host)
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    fn test_pubkey() -> String {
        format!("{:064x}", Uuid::new_v4().as_u128())
    }

    async fn assert_role(pool: &SqlitePool, community: CommunityId, pubkey: &str, role: &str) {
        assert_eq!(
            get_relay_member(pool, community, pubkey)
                .await
                .expect("get relay member")
                .map(|member| member.role)
                .as_deref(),
            Some(role)
        );
    }

    #[tokio::test]
    async fn membership_is_confined_to_its_community() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let pubkey = test_pubkey();

        assert!(
            add_relay_member(&pool, community_a, &pubkey, "member", None)
                .await
                .expect("add to A")
        );
        // Idempotent re-add.
        assert!(
            !add_relay_member(&pool, community_a, &pubkey, "member", None)
                .await
                .expect("re-add to A")
        );

        assert!(is_relay_member(&pool, community_a, &pubkey)
            .await
            .expect("is member A"));
        assert!(!is_relay_member(&pool, community_b, &pubkey)
            .await
            .expect("is member B"));

        let member = get_relay_member(&pool, community_a, &pubkey)
            .await
            .expect("get A")
            .expect("exists in A");
        assert_eq!(member.pubkey, pubkey);
        assert_eq!(member.role, "member");
        assert!(member.added_by.is_none());
        assert!(get_relay_member(&pool, community_b, &pubkey)
            .await
            .expect("get B")
            .is_none());

        let list_a = list_relay_members(&pool, community_a)
            .await
            .expect("list A");
        assert!(list_a.iter().any(|m| m.pubkey == pubkey));
        let list_b = list_relay_members(&pool, community_b)
            .await
            .expect("list B");
        assert!(list_b.iter().all(|m| m.pubkey != pubkey));
    }

    #[tokio::test]
    async fn claim_membership_persists_policy_acceptance_atomically() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let policy_member = test_pubkey();
        let legacy_member = test_pubkey();
        let version = "a".repeat(64);

        assert!(
            claim_relay_membership(&pool, community, &policy_member, "member", Some(&version))
                .await
                .expect("claim with policy")
        );
        assert!(
            has_join_policy_acceptance(&pool, community, &policy_member, &version)
                .await
                .expect("acceptance recorded")
        );
        let member = get_relay_member(&pool, community, &policy_member)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(member.added_by.as_deref(), Some("invite"));

        // Second claim is idempotent (false) but still records new evidence.
        let version2 = "b".repeat(64);
        assert!(!claim_relay_membership(
            &pool,
            community,
            &policy_member,
            "member",
            Some(&version2)
        )
        .await
        .expect("second claim"));
        assert!(
            has_join_policy_acceptance(&pool, community, &policy_member, &version2)
                .await
                .expect("second acceptance recorded")
        );

        // Legacy claim without a policy version records nothing.
        assert!(
            claim_relay_membership(&pool, community, &legacy_member, "member", None)
                .await
                .expect("legacy claim")
        );
        assert!(
            !has_join_policy_acceptance(&pool, community, &legacy_member, &version)
                .await
                .expect("no legacy acceptance")
        );
    }

    #[tokio::test]
    async fn remove_relay_member_protects_owner() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = test_pubkey();
        let member = test_pubkey();
        let admin = test_pubkey();

        bootstrap_owner(&pool, community, &owner)
            .await
            .expect("bootstrap");
        add_relay_member(&pool, community, &member, "member", None)
            .await
            .expect("add member");
        add_relay_member(&pool, community, &admin, "admin", None)
            .await
            .expect("add admin");

        assert_eq!(
            remove_relay_member(&pool, community, &member)
                .await
                .expect("remove member"),
            RemoveResult::Removed
        );
        assert_eq!(
            remove_relay_member(&pool, community, &member)
                .await
                .expect("remove again"),
            RemoveResult::NotFound
        );
        assert_eq!(
            remove_relay_member(&pool, community, &owner)
                .await
                .expect("remove owner"),
            RemoveResult::IsOwner
        );

        // Role-fenced removal.
        assert_eq!(
            remove_relay_member_if_role(&pool, community, &admin, "member")
                .await
                .expect("wrong role"),
            RemoveResult::RoleMismatch
        );
        assert_eq!(
            remove_relay_member_if_role(&pool, community, &owner, "member")
                .await
                .expect("owner fence"),
            RemoveResult::IsOwner
        );
        assert_eq!(
            remove_relay_member_if_role(&pool, community, &admin, "admin")
                .await
                .expect("right role"),
            RemoveResult::Removed
        );
        assert_eq!(
            remove_relay_member_if_role(&pool, community, &admin, "admin")
                .await
                .expect("gone"),
            RemoveResult::NotFound
        );
    }

    #[tokio::test]
    async fn update_role_never_touches_owner() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = test_pubkey();
        let member = test_pubkey();

        bootstrap_owner(&pool, community, &owner)
            .await
            .expect("bootstrap");
        add_relay_member(&pool, community, &member, "member", None)
            .await
            .expect("add member");

        assert!(update_relay_member_role(&pool, community, &member, "admin")
            .await
            .expect("promote member"));
        assert_role(&pool, community, &member, "admin").await;

        assert!(
            !update_relay_member_role(&pool, community, &owner, "member")
                .await
                .expect("owner untouched")
        );
        assert_role(&pool, community, &owner, "owner").await;
    }

    #[tokio::test]
    async fn bootstrap_owner_rotates_and_is_scoped() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let first = test_pubkey();
        let second = test_pubkey();

        bootstrap_owner(&pool, community_a, &first)
            .await
            .expect("bootstrap first");
        assert_role(&pool, community_a, &first, "owner").await;

        // Idempotent.
        bootstrap_owner(&pool, community_a, &first)
            .await
            .expect("bootstrap again");
        assert_role(&pool, community_a, &first, "owner").await;

        // Rotation demotes the previous owner to admin.
        bootstrap_owner(&pool, community_a, &second)
            .await
            .expect("rotate owner");
        assert_role(&pool, community_a, &second, "owner").await;
        assert_role(&pool, community_a, &first, "admin").await;

        // Scoped: nothing leaked into community B.
        assert!(!is_relay_member(&pool, community_b, &second)
            .await
            .expect("B unaffected"));
    }

    #[tokio::test]
    async fn transfer_ownership_flows() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let old_owner = test_pubkey();
        let new_owner = test_pubkey();

        // No owner yet.
        assert_eq!(
            transfer_ownership(&pool, community, &new_owner, &old_owner)
                .await
                .expect("transfer without owner"),
            TransferResult::NoOwner
        );

        bootstrap_owner(&pool, community, &old_owner)
            .await
            .expect("bootstrap");

        // Stale expected owner.
        let wrong = test_pubkey();
        assert_eq!(
            transfer_ownership(&pool, community, &new_owner, &wrong)
                .await
                .expect("stale expected"),
            TransferResult::OwnerConflict
        );
        assert_role(&pool, community, &old_owner, "owner").await;
        assert!(get_relay_member(&pool, community, &new_owner)
            .await
            .expect("get")
            .is_none());

        // Successful transfer demotes the old owner to member (not admin).
        assert_eq!(
            transfer_ownership(&pool, community, &new_owner, &old_owner)
                .await
                .expect("transfer"),
            TransferResult::Transferred {
                previous_owner: Some(old_owner.clone()),
            }
        );
        assert_role(&pool, community, &new_owner, "owner").await;
        assert_role(&pool, community, &old_owner, "member").await;

        // Transferring to the current sole owner is a no-op.
        assert_eq!(
            transfer_ownership(&pool, community, &new_owner, &new_owner)
                .await
                .expect("self transfer"),
            TransferResult::AlreadyOwner
        );
    }

    #[tokio::test]
    async fn transfer_ownership_enforces_per_owner_limit() {
        let pool = test_pool().await;
        let owner = test_pubkey();
        let transferee = test_pubkey();

        for _ in 0..MAX_COMMUNITIES_PER_OWNER {
            let c = make_community(&pool).await;
            bootstrap_owner(&pool, c, &transferee)
                .await
                .expect("bootstrap transferee community");
        }

        let community = make_community(&pool).await;
        bootstrap_owner(&pool, community, &owner)
            .await
            .expect("bootstrap owner");

        assert_eq!(
            transfer_ownership(&pool, community, &transferee, &owner)
                .await
                .expect("transfer to maxed transferee"),
            TransferResult::LimitReached
        );
        assert_role(&pool, community, &owner, "owner").await;
        assert!(get_relay_member(&pool, community, &transferee)
            .await
            .expect("get transferee")
            .is_none());
    }

    #[tokio::test]
    async fn transfer_ownership_is_community_scoped() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let owner_a = test_pubkey();
        let owner_b = test_pubkey();
        let new_owner = test_pubkey();

        bootstrap_owner(&pool, community_a, &owner_a)
            .await
            .expect("bootstrap A");
        bootstrap_owner(&pool, community_b, &owner_b)
            .await
            .expect("bootstrap B");

        transfer_ownership(&pool, community_a, &new_owner, &owner_a)
            .await
            .expect("transfer A");

        assert_role(&pool, community_a, &new_owner, "owner").await;
        assert_role(&pool, community_a, &owner_a, "member").await;
        assert_role(&pool, community_b, &owner_b, "owner").await;
        assert!(!is_relay_member(&pool, community_b, &new_owner)
            .await
            .expect("B unaffected"));
    }

    #[tokio::test]
    async fn allowlist_round_trip_and_scoping() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let pubkey = vec![7u8; 32];
        let added_by = vec![8u8; 32];

        assert!(!has_allowlist_entries(&pool, community_a)
            .await
            .expect("empty A"));
        assert!(
            add_to_allowlist(&pool, community_a, &pubkey, &added_by, Some("a-only"))
                .await
                .expect("add")
        );
        assert!(
            !add_to_allowlist(&pool, community_a, &pubkey, &added_by, Some("dup"))
                .await
                .expect("duplicate is idempotent")
        );

        assert!(is_pubkey_allowed(&pool, community_a, &pubkey)
            .await
            .expect("allowed A"));
        assert!(!is_pubkey_allowed(&pool, community_b, &pubkey)
            .await
            .expect("not allowed B"));
        assert!(has_allowlist_entries(&pool, community_a)
            .await
            .expect("A has entries"));
        assert!(!has_allowlist_entries(&pool, community_b)
            .await
            .expect("B empty"));

        let list = list_allowlist(&pool, community_a).await.expect("list A");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pubkey, pubkey);
        assert_eq!(list[0].added_by, added_by);
        assert_eq!(list[0].note.as_deref(), Some("a-only"));

        assert!(!remove_from_allowlist(&pool, community_b, &pubkey)
            .await
            .expect("remove from wrong community"));
        assert!(remove_from_allowlist(&pool, community_a, &pubkey)
            .await
            .expect("remove"));
        assert!(!is_pubkey_allowed(&pool, community_a, &pubkey)
            .await
            .expect("removed"));
    }

    #[tokio::test]
    async fn backfill_from_allowlist_converts_and_guards() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let other = make_community(&pool).await;
        let pubkey_a = vec![1u8; 32];
        let pubkey_b = vec![2u8; 32];
        let other_pk = vec![3u8; 32];
        let added_by = vec![9u8; 32];

        for pk in [&pubkey_a, &pubkey_b] {
            add_to_allowlist(&pool, community, pk, &added_by, None)
                .await
                .expect("seed allowlist");
        }
        add_to_allowlist(&pool, other, &other_pk, &added_by, None)
            .await
            .expect("seed other community");

        let inserted = backfill_from_allowlist(&pool, community)
            .await
            .expect("backfill");
        assert_eq!(inserted, 2, "only this community's entries backfill");

        // Pubkeys were converted to lowercase hex.
        let expected_hex = hex::encode(&pubkey_a);
        assert!(is_relay_member(&pool, community, &expected_hex)
            .await
            .expect("hex member"));
        let members = list_relay_members(&pool, community).await.expect("list");
        assert_eq!(members.len(), 2);
        assert!(members.iter().all(|m| m.role == "member"));

        // Second run is guarded: relay_members is non-empty.
        assert_eq!(
            backfill_from_allowlist(&pool, community)
                .await
                .expect("guarded rerun"),
            0
        );

        // Removal sticks across restarts: remove one member, rerun, still 0.
        remove_relay_member(&pool, community, &expected_hex)
            .await
            .expect("remove");
        assert_eq!(
            backfill_from_allowlist(&pool, community)
                .await
                .expect("no re-add"),
            0
        );
        assert!(!is_relay_member(&pool, community, &expected_hex)
            .await
            .expect("stays removed"));
    }
}

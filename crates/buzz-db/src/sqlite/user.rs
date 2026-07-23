//! SQLite arms for user CRUD (WP3 — users).
//!
//! Function-for-function port of the Solo-reachable operations in
//! [`crate::user`]. Semantics match the Postgres arm:
//!
//! - every predicate leads with `community_id` (tenant scoping),
//! - `ensure_user` distinguishes created vs. already-existing via
//!   `rows_affected` on an `ON CONFLICT DO NOTHING` insert,
//! - profile updates are last-write-wins per field; `Some("")` clears a
//!   field to NULL (kind:0 absolute-state semantics — also keeps the
//!   partial-unique `nip05_handle` index from colliding on empty strings),
//! - NIP-05 lookups compare case-insensitively on an already-normalized
//!   (lowercased) handle; SQLite `lower()` is ASCII-only, which matches the
//!   write-path validation (handles are validated ASCII),
//! - `set_agent_owner` is an atomic first-mint-wins claim (conditional
//!   UPDATE where the owner is still NULL).
//!
//! `search_users` is deliberately not ported: it has zero production call
//! sites (`docs/phase2/db-callsite-inventory.md` §2).
//!
//! Type conventions per `docs/phase2/sqlite-schema-notes.md`: UUIDs bind as
//! lowercase hyphenated TEXT, pubkeys as BLOB, enum columns are plain TEXT
//! (the `::channel_add_policy` cast is dropped).

// TODO(orchestrator): drop this allow when `Db` dispatch wires these arms.

use sqlx::{Row, SqlitePool};

use buzz_core::CommunityId;

use crate::error::{DbError, Result};
use crate::sqlite::event::community_text;
use crate::user::UserProfile;

/// Ensure a user record exists for the given pubkey (upsert).
///
/// Returns `true` if a new row was inserted, `false` if the user already
/// existed — the `true` case is the reliable "user was just registered"
/// signal used to increment `buzz_users_created_total`.
pub(crate) async fn ensure_user(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO users (community_id, pubkey) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Get a single user record by pubkey.
pub(crate) async fn get_user(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
) -> Result<Option<UserProfile>> {
    let row = sqlx::query_as::<
        _,
        (
            Vec<u8>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT pubkey, display_name, avatar_url, about, nip05_handle \
         FROM users WHERE community_id = ?1 AND pubkey = ?2",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(pubkey, display_name, avatar_url, about, nip05_handle)| UserProfile {
            pubkey,
            display_name,
            avatar_url,
            about,
            nip05_handle,
        },
    ))
}

/// Update a user's profile fields (display_name, avatar_url, about,
/// nip05_handle). Only fields that are `Some` are updated — `None` fields are
/// left unchanged. If every field is `None` the call is a no-op.
///
/// Empty strings are stored as NULL (see module docs).
pub(crate) async fn update_user_profile(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
    display_name: Option<&str>,
    avatar_url: Option<&str>,
    about: Option<&str>,
    nip05_handle: Option<&str>,
) -> Result<()> {
    let mut set_parts: Vec<&str> = Vec::new();
    if display_name.is_some() {
        set_parts.push("display_name = ?");
    }
    if avatar_url.is_some() {
        set_parts.push("avatar_url = ?");
    }
    if about.is_some() {
        set_parts.push("about = ?");
    }
    if nip05_handle.is_some() {
        set_parts.push("nip05_handle = ?");
    }
    if set_parts.is_empty() {
        return Ok(());
    }

    // Convert empty string to None (NULL in DB) — parity with the Postgres
    // arm's `empty_to_none`.
    fn empty_to_none(val: Option<&str>) -> Option<&str> {
        val.filter(|s| !s.is_empty())
    }

    // Unnumbered `?` placeholders bind strictly in order: the SET values
    // first, then the WHERE key.
    let sql = format!(
        "UPDATE users SET {} WHERE community_id = ? AND pubkey = ?",
        set_parts.join(", ")
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    if display_name.is_some() {
        query = query.bind(empty_to_none(display_name));
    }
    if avatar_url.is_some() {
        query = query.bind(empty_to_none(avatar_url));
    }
    if about.is_some() {
        query = query.bind(empty_to_none(about));
    }
    if nip05_handle.is_some() {
        query = query.bind(empty_to_none(nip05_handle));
    }
    query = query.bind(community_text(community_id));
    query = query.bind(pubkey);
    query.execute(pool).await?;
    Ok(())
}

/// Look up a user by their full NIP-05 handle (exact match,
/// case-insensitive). Both `local_part` and `domain` must already be
/// lowercased by the caller (parity with the Postgres arm's contract).
pub(crate) async fn get_user_by_nip05(
    pool: &SqlitePool,
    community_id: CommunityId,
    local_part: &str,
    domain: &str,
) -> Result<Option<UserProfile>> {
    let handle = format!("{}@{}", local_part, domain);
    let row = sqlx::query_as::<
        _,
        (
            Vec<u8>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT pubkey, display_name, avatar_url, about, nip05_handle \
         FROM users \
         WHERE community_id = ?1 AND lower(nip05_handle) = lower(?2) \
         LIMIT 1",
    )
    .bind(community_text(community_id))
    .bind(&handle)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(pubkey, display_name, avatar_url, about, nip05_handle)| UserProfile {
            pubkey,
            display_name,
            avatar_url,
            about,
            nip05_handle,
        },
    ))
}

/// Atomically set the agent owner — only if no owner is currently assigned
/// (first mint wins; the conditional UPDATE eliminates the TOCTOU race).
///
/// Returns `Ok(true)` if ownership was set, `Ok(false)` if an owner already
/// exists. Returns `Err(NotFound)` if the agent pubkey is not in the users
/// table.
pub(crate) async fn set_agent_owner(
    pool: &SqlitePool,
    community_id: CommunityId,
    agent_pubkey: &[u8],
    owner_pubkey: &[u8],
) -> Result<bool> {
    let community = community_text(community_id);
    let result = sqlx::query(
        "UPDATE users SET agent_owner_pubkey = ?1 \
         WHERE community_id = ?2 AND pubkey = ?3 AND agent_owner_pubkey IS NULL",
    )
    .bind(owner_pubkey)
    .bind(&community)
    .bind(agent_pubkey)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        // Could be: (a) pubkey not found, or (b) owner already set.
        let exists = sqlx::query("SELECT 1 FROM users WHERE community_id = ?1 AND pubkey = ?2")
            .bind(&community)
            .bind(agent_pubkey)
            .fetch_optional(pool)
            .await?;
        if exists.is_none() {
            return Err(DbError::NotFound(
                "agent pubkey not found in users table".into(),
            ));
        }
        return Ok(false);
    }
    Ok(true)
}

/// Get the `channel_add_policy` and `agent_owner_pubkey` for a user.
///
/// Returns `None` if the pubkey is not in the users table,
/// `Some((policy, owner_or_none))` otherwise.
pub(crate) async fn get_agent_channel_policy(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
) -> Result<Option<(String, Option<Vec<u8>>)>> {
    let row = sqlx::query(
        "SELECT channel_add_policy, agent_owner_pubkey FROM users \
         WHERE community_id = ?1 AND pubkey = ?2",
    )
    .bind(community_text(community_id))
    .bind(pubkey)
    .fetch_optional(pool)
    .await?;

    row.map(|r| -> Result<(String, Option<Vec<u8>>)> {
        let policy: String = r.try_get("channel_add_policy")?;
        let owner: Option<Vec<u8>> = r.try_get("agent_owner_pubkey").unwrap_or(None);
        Ok((policy, owner))
    })
    .transpose()
}

/// Check whether `actor_pubkey` is the `agent_owner_pubkey` of
/// `target_pubkey`. Rows without an owner never match.
pub(crate) async fn is_agent_owner(
    pool: &SqlitePool,
    community_id: CommunityId,
    target_pubkey: &[u8],
    actor_pubkey: &[u8],
) -> Result<bool> {
    let row = sqlx::query_scalar::<_, bool>(
        "SELECT agent_owner_pubkey = ?3 FROM users \
         WHERE community_id = ?1 AND pubkey = ?2 AND agent_owner_pubkey IS NOT NULL",
    )
    .bind(community_text(community_id))
    .bind(target_pubkey)
    .bind(actor_pubkey)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or(false))
}

/// Set the `channel_add_policy` for a user.
///
/// Returns `Err(InvalidData)` for an unknown policy value and
/// `Err(NotFound)` if the pubkey is not in the users table.
pub(crate) async fn set_channel_add_policy(
    pool: &SqlitePool,
    community_id: CommunityId,
    pubkey: &[u8],
    policy: &str,
) -> Result<()> {
    if !matches!(policy, "anyone" | "owner_only" | "nobody") {
        return Err(DbError::InvalidData(format!(
            "invalid channel_add_policy: {policy}"
        )));
    }
    let result = sqlx::query(
        "UPDATE users SET channel_add_policy = ?1 WHERE community_id = ?2 AND pubkey = ?3",
    )
    .bind(policy)
    .bind(community_text(community_id))
    .bind(pubkey)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::NotFound("pubkey not found in users table".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    async fn setup_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        pool
    }

    async fn make_community(pool: &SqlitePool) -> CommunityId {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.to_string())
            .bind(format!("user-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    fn pk(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    #[tokio::test]
    async fn ensure_user_created_vs_existing() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let user = pk(1);

        let created = ensure_user(&pool, community, &user).await.expect("first");
        assert!(created, "first ensure must report created");

        let again = ensure_user(&pool, community, &user).await.expect("second");
        assert!(!again, "second ensure must report already-existing");

        // Exactly one row.
        let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM users WHERE pubkey = ?1")
            .bind(&user)
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn profile_round_trip_partial_update_and_empty_clears() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let user = pk(2);
        ensure_user(&pool, community, &user).await.expect("ensure");

        // Fresh user: all profile fields NULL.
        let profile = get_user(&pool, community, &user)
            .await
            .expect("get")
            .expect("row exists");
        assert_eq!(profile.pubkey, user);
        assert!(profile.display_name.is_none());
        assert!(profile.nip05_handle.is_none());

        update_user_profile(
            &pool,
            community,
            &user,
            Some("Alice"),
            Some("https://example.com/a.png"),
            Some("about alice"),
            Some("alice@solo.example"),
        )
        .await
        .expect("full update");

        let profile = get_user(&pool, community, &user)
            .await
            .expect("get")
            .expect("row exists");
        assert_eq!(profile.display_name.as_deref(), Some("Alice"));
        assert_eq!(
            profile.avatar_url.as_deref(),
            Some("https://example.com/a.png")
        );
        assert_eq!(profile.about.as_deref(), Some("about alice"));
        assert_eq!(profile.nip05_handle.as_deref(), Some("alice@solo.example"));

        // None fields are untouched; Some("") clears to NULL.
        update_user_profile(
            &pool,
            community,
            &user,
            Some("Alicia"),
            None,
            Some(""),
            None,
        )
        .await
        .expect("partial update");
        let profile = get_user(&pool, community, &user)
            .await
            .expect("get")
            .expect("row exists");
        assert_eq!(profile.display_name.as_deref(), Some("Alicia"));
        assert_eq!(
            profile.avatar_url.as_deref(),
            Some("https://example.com/a.png"),
            "None must leave the field unchanged"
        );
        assert!(profile.about.is_none(), "empty string must clear to NULL");
        assert_eq!(profile.nip05_handle.as_deref(), Some("alice@solo.example"));

        // All-None update is a no-op (does not error).
        update_user_profile(&pool, community, &user, None, None, None, None)
            .await
            .expect("no-op update");

        // NIP-05 lookup is case-insensitive on the stored handle.
        let by_nip05 = get_user_by_nip05(&pool, community, "alice", "solo.example")
            .await
            .expect("nip05 lookup")
            .expect("found");
        assert_eq!(by_nip05.pubkey, user);
        assert!(
            get_user_by_nip05(&pool, community, "nobody", "solo.example")
                .await
                .expect("nip05 miss")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cross_community_isolation() {
        let pool = setup_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let user = pk(3);

        // Same pubkey registers independently per community.
        assert!(ensure_user(&pool, community_a, &user).await.expect("a"));
        assert!(
            ensure_user(&pool, community_b, &user).await.expect("b"),
            "same pubkey in another community is a fresh row"
        );

        update_user_profile(
            &pool,
            community_a,
            &user,
            Some("A-side"),
            None,
            None,
            Some("split@solo.example"),
        )
        .await
        .expect("update in A");

        let in_a = get_user(&pool, community_a, &user)
            .await
            .expect("get a")
            .expect("row in A");
        assert_eq!(in_a.display_name.as_deref(), Some("A-side"));

        let in_b = get_user(&pool, community_b, &user)
            .await
            .expect("get b")
            .expect("row in B");
        assert!(
            in_b.display_name.is_none(),
            "profile written in A must not leak into B"
        );

        // NIP-05 handle resolves only in the community it was set in.
        assert!(
            get_user_by_nip05(&pool, community_a, "split", "solo.example")
                .await
                .expect("lookup a")
                .is_some()
        );
        assert!(
            get_user_by_nip05(&pool, community_b, "split", "solo.example")
                .await
                .expect("lookup b")
                .is_none()
        );
    }

    #[tokio::test]
    async fn agent_owner_first_mint_wins_and_policy_round_trip() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let agent = pk(4);
        let owner1 = pk(5);
        let owner2 = pk(6);
        for p in [&agent, &owner1, &owner2] {
            ensure_user(&pool, community, p).await.expect("ensure");
        }

        // Unknown pubkey → None policy, NotFound on set_agent_owner.
        let ghost = pk(9);
        assert!(get_agent_channel_policy(&pool, community, &ghost)
            .await
            .expect("policy query")
            .is_none());
        assert!(matches!(
            set_agent_owner(&pool, community, &ghost, &owner1).await,
            Err(DbError::NotFound(_))
        ));

        // First mint wins.
        assert!(set_agent_owner(&pool, community, &agent, &owner1)
            .await
            .expect("first set"));
        assert!(
            !set_agent_owner(&pool, community, &agent, &owner2)
                .await
                .expect("second set"),
            "second mint must return false, not overwrite"
        );

        let (policy, owner) = get_agent_channel_policy(&pool, community, &agent)
            .await
            .expect("policy")
            .expect("row exists");
        assert_eq!(policy, "anyone", "default policy");
        assert_eq!(owner, Some(owner1.clone()), "original owner preserved");

        assert!(is_agent_owner(&pool, community, &agent, &owner1)
            .await
            .expect("is owner"));
        assert!(!is_agent_owner(&pool, community, &agent, &owner2)
            .await
            .expect("not owner"));
        // No owner set → never an owner match.
        assert!(!is_agent_owner(&pool, community, &owner1, &owner1)
            .await
            .expect("ownerless"));

        // channel_add_policy round trip + validation.
        set_channel_add_policy(&pool, community, &agent, "owner_only")
            .await
            .expect("set owner_only");
        let (policy, _) = get_agent_channel_policy(&pool, community, &agent)
            .await
            .expect("policy")
            .expect("row exists");
        assert_eq!(policy, "owner_only");

        assert!(matches!(
            set_channel_add_policy(&pool, community, &agent, "bogus").await,
            Err(DbError::InvalidData(_))
        ));
        assert!(matches!(
            set_channel_add_policy(&pool, community, &ghost, "nobody").await,
            Err(DbError::NotFound(_))
        ));
    }
}

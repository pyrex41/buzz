//! SQLite arms for the git repository name registry (WP8 — NIP-34
//! kind:30617).
//!
//! Straight port of `crate::git_repo`: names are unique **within a
//! community** via the `(community_id, repo_id)` primary key, and the atomic
//! `INSERT … ON CONFLICT DO NOTHING RETURNING` claim (with post-conflict
//! re-read to classify same-owner re-announce vs collision) works unchanged
//! on SQLite ≥ 3.35. SQLite's single global writer makes the claim statement
//! fully serialized, so no extra transaction is needed.
//!
//! `owner_pubkey` is hex TEXT (wire form), matching the Postgres column.

// TODO(dispatch): remove once the `Db` facade (lib.rs) wires these arms —
// until then nothing outside this module calls them.

use sqlx::{Row as _, SqlitePool};

use crate::error::Result;
use crate::git_repo::ReserveOutcome;
use crate::CommunityId;

use super::event::community_text;

/// Return the current owner pubkey of `repo_id` in `community`, or `None` if
/// the name is unreserved. Used to classify an announce (same-owner
/// re-announce vs cross-owner collision) and to gate the quota check before
/// a fresh claim.
pub(crate) async fn repo_name_owner(
    pool: &SqlitePool,
    community: CommunityId,
    repo_id: &str,
) -> Result<Option<String>> {
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner_pubkey FROM git_repo_names WHERE community_id = ?1 AND repo_id = ?2",
    )
    .bind(community_text(community))
    .bind(repo_id)
    .fetch_optional(pool)
    .await?;
    Ok(owner)
}

/// Reserve `repo_id` for `owner_pubkey` within `community`.
///
/// Semantics (identical to the Postgres arm):
/// - free name → atomically claimed, [`ReserveOutcome::Reserved`];
/// - already reserved by the same owner → [`ReserveOutcome::AlreadyOwned`]
///   (idempotent re-announce, no row inserted, quota not re-checked);
/// - held by a different owner → [`ReserveOutcome::TakenByOther`].
///
/// A full quota is *not* an error here — the caller enforces the per-pubkey
/// limit against [`count_repos_for_owner`] before a fresh claim.
pub(crate) async fn reserve_repo_name(
    pool: &SqlitePool,
    community: CommunityId,
    repo_id: &str,
    owner_pubkey: &str,
) -> Result<ReserveOutcome> {
    // Atomic claim: RETURNING is non-empty exactly when *this* statement
    // inserted the row (TOCTOU-free, same guarantee as the Postgres arm).
    let inserted = sqlx::query(
        "INSERT INTO git_repo_names (community_id, repo_id, owner_pubkey) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT (community_id, repo_id) DO NOTHING \
         RETURNING owner_pubkey",
    )
    .bind(community_text(community))
    .bind(repo_id)
    .bind(owner_pubkey)
    .fetch_optional(pool)
    .await?;

    if inserted.is_some() {
        return Ok(ReserveOutcome::Reserved);
    }

    // The row already existed — read the holder to classify.
    let existing = sqlx::query(
        "SELECT owner_pubkey FROM git_repo_names WHERE community_id = ?1 AND repo_id = ?2",
    )
    .bind(community_text(community))
    .bind(repo_id)
    .fetch_optional(pool)
    .await?;

    match existing {
        Some(row) => {
            let holder: String = row.try_get("owner_pubkey")?;
            if holder == owner_pubkey {
                Ok(ReserveOutcome::AlreadyOwned)
            } else {
                Ok(ReserveOutcome::TakenByOther)
            }
        }
        // Narrow race: the conflicting row was deleted between our INSERT and
        // this SELECT (e.g. a concurrent seed-failure rollback). Treat as
        // taken-by-other rather than silently granting — the announcer can
        // retry, and we never hand out a name we didn't atomically claim.
        None => Ok(ReserveOutcome::TakenByOther),
    }
}

/// Count the repos currently reserved by `owner_pubkey` in `community`
/// (backs the per-pubkey quota, enforced by the caller).
pub(crate) async fn count_repos_for_owner(
    pool: &SqlitePool,
    community: CommunityId,
    owner_pubkey: &str,
) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM git_repo_names WHERE community_id = ?1 AND owner_pubkey = ?2",
    )
    .bind(community_text(community))
    .bind(owner_pubkey)
    .fetch_one(pool)
    .await?;
    Ok(n)
}

/// Release a reservation held by `owner_pubkey` (rollback path after a
/// failed manifest seed). Scoped to `owner_pubkey` so a rollback can never
/// delete a name a *different* owner concurrently holds. Returns the number
/// of rows removed (0 or 1).
pub(crate) async fn release_repo_name(
    pool: &SqlitePool,
    community: CommunityId,
    repo_id: &str,
    owner_pubkey: &str,
) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM git_repo_names \
         WHERE community_id = ?1 AND repo_id = ?2 AND owner_pubkey = ?3",
    )
    .bind(community_text(community))
    .bind(repo_id)
    .bind(owner_pubkey)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ── Tests ───────────────────────────────────────────────────────────────────

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
        sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
            .bind(id.to_string())
            .bind(format!("git-repo-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    fn pk() -> String {
        format!("{:064x}", Uuid::new_v4().as_u128())
    }

    /// A fresh name is `Reserved`; re-announcing as the *same* owner is
    /// `AlreadyOwned` (idempotent, no quota growth); a *different* owner is
    /// `TakenByOther`.
    #[tokio::test]
    async fn reserve_classifies_fresh_idempotent_and_collision() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk();
        let other = pk();
        let repo = format!("repo-{}", Uuid::new_v4().simple());

        assert_eq!(
            reserve_repo_name(&pool, community, &repo, &owner)
                .await
                .expect("fresh reserve"),
            ReserveOutcome::Reserved
        );
        assert_eq!(
            reserve_repo_name(&pool, community, &repo, &owner)
                .await
                .expect("re-reserve same owner"),
            ReserveOutcome::AlreadyOwned
        );
        assert_eq!(
            reserve_repo_name(&pool, community, &repo, &other)
                .await
                .expect("re-reserve other owner"),
            ReserveOutcome::TakenByOther
        );
        assert_eq!(
            count_repos_for_owner(&pool, community, &owner)
                .await
                .expect("count owner"),
            1,
            "re-announce must not double-count the owner's quota"
        );
        assert_eq!(
            count_repos_for_owner(&pool, community, &other)
                .await
                .expect("count other"),
            0,
            "a failed claim must not count toward the loser's quota"
        );
    }

    /// Release is owner-scoped: a non-holder's release is a no-op; the
    /// holder's release frees the name for a subsequent re-reserve.
    #[tokio::test]
    async fn release_is_owner_scoped_and_frees_the_name() {
        let pool = test_pool().await;
        let community = make_community(&pool).await;
        let owner = pk();
        let stranger = pk();
        let repo = format!("repo-{}", Uuid::new_v4().simple());

        assert_eq!(
            reserve_repo_name(&pool, community, &repo, &owner)
                .await
                .expect("reserve"),
            ReserveOutcome::Reserved
        );
        assert_eq!(
            repo_name_owner(&pool, community, &repo)
                .await
                .expect("owner lookup"),
            Some(owner.clone())
        );

        // A non-holder cannot release the name.
        assert_eq!(
            release_repo_name(&pool, community, &repo, &stranger)
                .await
                .expect("stranger release"),
            0
        );
        assert_eq!(
            repo_name_owner(&pool, community, &repo)
                .await
                .expect("still owned"),
            Some(owner.clone()),
            "the reservation survives a stranger's release attempt"
        );

        // The holder releases it, freeing the name for a new owner.
        assert_eq!(
            release_repo_name(&pool, community, &repo, &owner)
                .await
                .expect("owner release"),
            1
        );
        assert!(repo_name_owner(&pool, community, &repo)
            .await
            .expect("freed")
            .is_none());
        assert_eq!(
            reserve_repo_name(&pool, community, &repo, &stranger)
                .await
                .expect("reclaim after release"),
            ReserveOutcome::Reserved,
            "once released, the name is free for a new owner"
        );
    }

    /// Names are unique *within* a community, not globally.
    #[tokio::test]
    async fn names_are_scoped_per_community() {
        let pool = test_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let owner_a = pk();
        let owner_b = pk();
        let repo = format!("repo-{}", Uuid::new_v4().simple());

        assert_eq!(
            reserve_repo_name(&pool, community_a, &repo, &owner_a)
                .await
                .expect("reserve in A"),
            ReserveOutcome::Reserved
        );
        assert_eq!(
            reserve_repo_name(&pool, community_b, &repo, &owner_b)
                .await
                .expect("reserve same name in B"),
            ReserveOutcome::Reserved,
            "the same name in another community is a fresh, independent claim"
        );
        assert_eq!(
            repo_name_owner(&pool, community_a, &repo)
                .await
                .expect("owner in A"),
            Some(owner_a)
        );
        assert_eq!(
            repo_name_owner(&pool, community_b, &repo)
                .await
                .expect("owner in B"),
            Some(owner_b)
        );
    }
}

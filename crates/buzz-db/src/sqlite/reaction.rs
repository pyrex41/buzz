//! SQLite arms for reaction persistence (WP4 — reactions).
//!
//! Function-for-function port of the Solo-reachable operations in
//! [`crate::reaction`]: `remove_reaction` and
//! `remove_reaction_by_source_event_id` (both called from the relay's
//! event side-effects when a kind:5 deletion retires a reaction).
//!
//! Semantics match the Postgres arm: reactions are unique per
//! `(community_id, event_created_at, event_id, pubkey, emoji)` (the table
//! PK), removal is a soft delete (`removed_at = unixepoch()`, the Postgres
//! `NOW()`), and removing an already-removed or missing row returns `false`.
//!
//! **Not ported** (zero production call sites,
//! `docs/phase2/db-callsite-inventory.md` §2): `add_reaction`,
//! `get_active_reaction_record`, `set_reaction_event_id`, `get_reactions`,
//! `get_reactions_bulk`. The production add/re-activate path — the
//! `ON CONFLICT … DO UPDATE … WHERE removed_at IS NOT NULL` three-state
//! upsert — lives inside
//! `crate::sqlite::event::insert_reaction_event_with_thread_metadata` and is
//! exercised by the add/remove/re-add tests in this file.

// TODO(orchestrator): drop this allow when `Db` dispatch wires these arms.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use crate::error::Result;
use crate::sqlite::event::community_text;
use crate::CommunityId;

/// Soft-delete a reaction identified by its
/// `(target event, actor, emoji)` tuple by setting `removed_at`.
///
/// Returns `true` if a row was retired, `false` if not found or already
/// removed (idempotent).
pub(crate) async fn remove_reaction(
    pool: &SqlitePool,
    community: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
    pubkey: &[u8],
    emoji: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE reactions \
         SET removed_at = unixepoch() \
         WHERE community_id = ?1 \
           AND event_created_at = ?2 \
           AND event_id = ?3 \
           AND pubkey = ?4 \
           AND emoji = ?5 \
           AND removed_at IS NULL",
    )
    .bind(community_text(community))
    .bind(event_created_at.timestamp())
    .bind(event_id)
    .bind(pubkey)
    .bind(emoji)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Soft-delete a reaction by the kind:7 reaction event's own ID.
///
/// Returns `true` if a row was retired, `false` if not found or already
/// removed (idempotent).
pub(crate) async fn remove_reaction_by_source_event_id(
    pool: &SqlitePool,
    community: CommunityId,
    reaction_event_id: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE reactions \
         SET removed_at = unixepoch() \
         WHERE community_id = ?1 \
           AND reaction_event_id = ?2 \
           AND removed_at IS NULL",
    )
    .bind(community_text(community))
    .bind(reaction_event_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ReactionEventInsertOutcome;
    use crate::sqlite::event::{insert_event, insert_reaction_event_with_thread_metadata};
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
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
            .bind(format!("reaction-test-{}.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert community");
        CommunityId::from_uuid(id)
    }

    /// Build a kind:7 reaction event. `created_at` must be distinct per
    /// event — Nostr event ids are content-addressed, so two reactions from
    /// the same key with identical content/tags/timestamp are the SAME
    /// event.
    fn reaction_event(
        keys: &Keys,
        target: &nostr::Event,
        emoji: &str,
        created_at: u64,
    ) -> nostr::Event {
        EventBuilder::new(Kind::Custom(7), emoji)
            .custom_created_at(Timestamp::from(created_at))
            .tags([Tag::parse(["e", &target.id.to_hex()]).expect("e tag")])
            .sign_with_keys(keys)
            .expect("sign reaction")
    }

    /// Active row count for one (target, actor, emoji) tuple, plus the
    /// stored `reaction_event_id` (raw table state).
    async fn active_row(
        pool: &SqlitePool,
        community: CommunityId,
        target: &nostr::Event,
        actor: &[u8],
        emoji: &str,
    ) -> Option<Vec<u8>> {
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT reaction_event_id FROM reactions \
             WHERE community_id = ?1 AND event_id = ?2 AND pubkey = ?3 AND emoji = ?4 \
               AND removed_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(target.id.as_bytes().as_slice())
        .bind(actor)
        .bind(emoji)
        .fetch_optional(pool)
        .await
        .expect("query active reaction row")
    }

    /// Full add → duplicate → remove → re-add → remove-by-source lifecycle
    /// through the production insert path, pinning the
    /// (target, actor, emoji) uniqueness and idempotence at each step.
    #[tokio::test]
    async fn reaction_add_remove_and_re_add_lifecycle() {
        let pool = setup_pool().await;
        let community = make_community(&pool).await;
        let author = Keys::generate();
        let actor = Keys::generate();
        let actor_pk = actor.public_key().to_bytes().to_vec();
        let emoji = "👍";

        // Target message the reactions attach to.
        let target = EventBuilder::new(Kind::Custom(9), "react to me")
            .custom_created_at(Timestamp::from(1_800_000_000u64))
            .sign_with_keys(&author)
            .expect("sign target");
        let target_ts = DateTime::from_timestamp(target.created_at.as_secs() as i64, 0)
            .expect("valid target ts");
        insert_event(&pool, community, &target, None)
            .await
            .expect("insert target");

        // Add: fresh reaction row + kind:7 event stored.
        let first = reaction_event(&actor, &target, emoji, 1_800_000_010);
        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &first,
            None,
            None,
            target.id.as_bytes(),
            &actor_pk,
            emoji,
        )
        .await
        .expect("first add");
        assert!(matches!(
            outcome,
            ReactionEventInsertOutcome::Inserted {
                was_inserted: true,
                ..
            }
        ));
        assert_eq!(
            active_row(&pool, community, &target, &actor_pk, emoji).await,
            Some(first.id.as_bytes().to_vec()),
            "active row links the kind:7 source event"
        );

        // Duplicate add while active: short-circuits, stores nothing.
        let dup = reaction_event(&actor, &target, emoji, 1_800_000_011);
        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &dup,
            None,
            None,
            target.id.as_bytes(),
            &actor_pk,
            emoji,
        )
        .await
        .expect("duplicate add");
        assert!(matches!(outcome, ReactionEventInsertOutcome::Duplicate));

        // A different emoji from the same actor is a distinct row
        // ((target, actor, emoji) uniqueness — not (target, actor)).
        let other_emoji = reaction_event(&actor, &target, "🎉", 1_800_000_012);
        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &other_emoji,
            None,
            None,
            target.id.as_bytes(),
            &actor_pk,
            "🎉",
        )
        .await
        .expect("other emoji add");
        assert!(matches!(
            outcome,
            ReactionEventInsertOutcome::Inserted { .. }
        ));

        // Remove: retires the row once; the second call is a no-op.
        assert!(remove_reaction(
            &pool,
            community,
            target.id.as_bytes(),
            target_ts,
            &actor_pk,
            emoji
        )
        .await
        .expect("remove"));
        assert!(active_row(&pool, community, &target, &actor_pk, emoji)
            .await
            .is_none());
        assert!(!remove_reaction(
            &pool,
            community,
            target.id.as_bytes(),
            target_ts,
            &actor_pk,
            emoji
        )
        .await
        .expect("second remove"),);

        // The other emoji's row is untouched by the removal.
        assert!(
            active_row(&pool, community, &target, &actor_pk, "🎉")
                .await
                .is_some(),
            "removal must be scoped to the exact emoji"
        );

        // Re-add after removal: reactivates the SAME row (still exactly one
        // row for the tuple) and re-links the new kind:7 event id.
        let second = reaction_event(&actor, &target, emoji, 1_800_000_013);
        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community,
            &second,
            None,
            None,
            target.id.as_bytes(),
            &actor_pk,
            emoji,
        )
        .await
        .expect("re-add");
        assert!(matches!(
            outcome,
            ReactionEventInsertOutcome::Inserted {
                was_inserted: true,
                ..
            }
        ));
        assert_eq!(
            active_row(&pool, community, &target, &actor_pk, emoji).await,
            Some(second.id.as_bytes().to_vec()),
            "re-activation must relink the new source event"
        );
        let (rows,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM reactions \
             WHERE community_id = ?1 AND event_id = ?2 AND pubkey = ?3 AND emoji = ?4",
        )
        .bind(community.as_uuid().to_string())
        .bind(target.id.as_bytes().as_slice())
        .bind(&actor_pk)
        .bind(emoji)
        .fetch_one(&pool)
        .await
        .expect("count rows");
        assert_eq!(rows, 1, "one row per (target, actor, emoji), ever");

        // Remove by the kind:7 source event id (NIP-09 deletion path).
        assert!(
            remove_reaction_by_source_event_id(&pool, community, second.id.as_bytes())
                .await
                .expect("remove by source")
        );
        assert!(active_row(&pool, community, &target, &actor_pk, emoji)
            .await
            .is_none());
        assert!(
            !remove_reaction_by_source_event_id(&pool, community, second.id.as_bytes())
                .await
                .expect("second remove by source"),
            "already-removed row must not match again"
        );
        // The retired first-generation event id matches nothing either.
        assert!(
            !remove_reaction_by_source_event_id(&pool, community, first.id.as_bytes())
                .await
                .expect("stale source id"),
        );
    }

    /// Tenant scoping: a reaction added in community A is invisible to
    /// removal attempts scoped to community B.
    #[tokio::test]
    async fn reaction_removal_is_tenant_scoped() {
        let pool = setup_pool().await;
        let community_a = make_community(&pool).await;
        let community_b = make_community(&pool).await;
        let author = Keys::generate();
        let actor = Keys::generate();
        let actor_pk = actor.public_key().to_bytes().to_vec();
        let emoji = "👍";

        let target = EventBuilder::new(Kind::Custom(9), "scoped target")
            .custom_created_at(Timestamp::from(1_800_000_050u64))
            .sign_with_keys(&author)
            .expect("sign target");
        let target_ts = DateTime::from_timestamp(target.created_at.as_secs() as i64, 0)
            .expect("valid target ts");
        insert_event(&pool, community_a, &target, None)
            .await
            .expect("insert target in A");

        let reaction = reaction_event(&actor, &target, emoji, 1_800_000_060);
        let outcome = insert_reaction_event_with_thread_metadata(
            &pool,
            community_a,
            &reaction,
            None,
            None,
            target.id.as_bytes(),
            &actor_pk,
            emoji,
        )
        .await
        .expect("add in A");
        assert!(matches!(
            outcome,
            ReactionEventInsertOutcome::Inserted { .. }
        ));

        // B cannot remove A's reaction — by tuple or by source event id.
        assert!(!remove_reaction(
            &pool,
            community_b,
            target.id.as_bytes(),
            target_ts,
            &actor_pk,
            emoji
        )
        .await
        .expect("cross-tenant remove"));
        assert!(
            !remove_reaction_by_source_event_id(&pool, community_b, reaction.id.as_bytes())
                .await
                .expect("cross-tenant remove by source")
        );
        assert!(
            active_row(&pool, community_a, &target, &actor_pk, emoji)
                .await
                .is_some(),
            "A's reaction survives B's removal attempts"
        );

        // A's own removal still works.
        assert!(remove_reaction(
            &pool,
            community_a,
            target.id.as_bytes(),
            target_ts,
            &actor_pk,
            emoji
        )
        .await
        .expect("in-tenant remove"));
    }
}

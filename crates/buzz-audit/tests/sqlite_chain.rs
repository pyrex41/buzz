//! SQLite arm of the audit hash chain — infrastructure-free tests.
//!
//! Unlike the Postgres tests in `service.rs` (which skip without a reachable
//! `DATABASE_URL`), these run unconditionally: the database is an in-memory
//! SQLite pool with buzz-db's real embedded migrations applied (the
//! `audit_log` table ships in migration 0002), so the tests exercise the exact
//! DDL the Solo profile runs against.

use std::sync::Arc;

use buzz_audit::{AuditAction, AuditError, AuditService, NewAuditEntry};
use buzz_core::CommunityId;
use sqlx::SqlitePool;
use uuid::Uuid;

/// In-memory pool with the real buzz-db sqlite schema (v1 + v2) applied.
async fn setup_pool() -> SqlitePool {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("connect in-memory sqlite");
    buzz_db::sqlite::run_migrations(&pool)
        .await
        .expect("apply buzz-db sqlite migrations");
    pool
}

/// Insert a throwaway community row (FK target for audit_log) and return its id.
async fn make_community(pool: &SqlitePool) -> Uuid {
    let id = Uuid::new_v4();
    let host = format!("test-{id}.example");
    sqlx::query("INSERT INTO communities (id, host) VALUES (?1, ?2)")
        .bind(id.to_string())
        .bind(host)
        .execute(pool)
        .await
        .expect("insert test community");
    id
}

fn new_entry(community_id: Uuid, action: AuditAction) -> NewAuditEntry {
    NewAuditEntry {
        community_id: CommunityId::from_uuid(community_id),
        action,
        actor_pubkey: Some(vec![0xab; 32]),
        object_id: Some(format!("obj_{}", Uuid::new_v4())),
        detail: serde_json::json!({"test": true, "nested": {"z": 1, "a": [1, 2]}}),
    }
}

#[tokio::test]
async fn append_n_entries_chain_verifies() {
    let pool = setup_pool().await;
    let svc = AuditService::new_sqlite(pool.clone());
    let c = make_community(&pool).await;

    let mut prev_hash: Option<Vec<u8>> = None;
    for i in 1..=5i64 {
        let e = svc
            .log(new_entry(c, AuditAction::EventCreated))
            .await
            .expect("log entry");
        assert_eq!(e.seq, i);
        assert_eq!(e.community_id, c);
        assert_eq!(e.hash.len(), 32);
        assert_eq!(
            e.prev_hash, prev_hash,
            "entry {i} must chain to its predecessor"
        );
        // Stored as INTEGER unix seconds — the hashed value must already be
        // second-precision so stored == hashed (round-trip parity).
        assert_eq!(e.created_at.timestamp_subsec_nanos(), 0);
        prev_hash = Some(e.hash);
    }

    // verify_chain refetches from the DB, so this proves every column decodes
    // back to exactly the value that was hashed.
    assert!(svc
        .verify_chain(CommunityId::from_uuid(c), 1, 5)
        .await
        .expect("verify"));

    // Empty range reports false, matching the Postgres arm.
    let fresh = make_community(&pool).await;
    assert!(!svc
        .verify_chain(CommunityId::from_uuid(fresh), 1, 100)
        .await
        .expect("verify empty"));
}

#[tokio::test]
async fn tampered_middle_entry_reported_at_its_seq() {
    let pool = setup_pool().await;
    let svc = AuditService::new_sqlite(pool.clone());
    let c = make_community(&pool).await;

    for _ in 0..5 {
        svc.log(new_entry(c, AuditAction::EventCreated))
            .await
            .expect("log entry");
    }

    // Tamper with the payload of the middle entry via direct SQL.
    sqlx::query("UPDATE audit_log SET detail = ?1 WHERE community_id = ?2 AND seq = 3")
        .bind(serde_json::json!({"test": false, "forged": true}))
        .bind(c.to_string())
        .execute(&pool)
        .await
        .expect("tamper");

    let r = svc.verify_chain(CommunityId::from_uuid(c), 1, 5).await;
    assert!(
        matches!(r, Err(AuditError::HashMismatch { seq: 3 })),
        "expected HashMismatch at seq 3, got {r:?}"
    );

    // A broken chain link (forged prev_hash) is reported as a ChainViolation
    // at the seq whose prev_hash no longer matches.
    let c2 = make_community(&pool).await;
    for _ in 0..3 {
        svc.log(new_entry(c2, AuditAction::ChannelCreated))
            .await
            .expect("log entry");
    }
    sqlx::query("UPDATE audit_log SET prev_hash = ?1 WHERE community_id = ?2 AND seq = 2")
        .bind(vec![0xffu8; 32])
        .bind(c2.to_string())
        .execute(&pool)
        .await
        .expect("forge prev_hash");
    let r = svc.verify_chain(CommunityId::from_uuid(c2), 1, 3).await;
    assert!(
        matches!(r, Err(AuditError::ChainViolation { seq: 2 })),
        "expected ChainViolation at seq 2, got {r:?}"
    );
}

#[tokio::test]
async fn get_entries_pagination_and_scoping_match_pg_behavior() {
    let pool = setup_pool().await;
    let svc = AuditService::new_sqlite(pool.clone());
    let a = make_community(&pool).await;
    let b = make_community(&pool).await;

    let mut logged = Vec::new();
    for _ in 0..5 {
        logged.push(
            svc.log(new_entry(a, AuditAction::EventCreated))
                .await
                .expect("log a"),
        );
    }
    // Another community's rows must never appear in A's reads.
    svc.log(new_entry(b, AuditAction::EventCreated))
        .await
        .expect("log b");

    // Page: from_seq is inclusive, limit caps the page, order is ascending.
    let page = svc
        .get_entries(CommunityId::from_uuid(a), 2, 2)
        .await
        .expect("page");
    assert_eq!(
        page.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3],
        "from_seq=2 limit=2 must return seqs [2, 3]"
    );
    // Round-trip fidelity: the paged rows equal what log() returned.
    assert_eq!(page[0], logged[1]);
    assert_eq!(page[1], logged[2]);

    // Full read returns only A's 5 entries.
    let all = svc
        .get_entries(CommunityId::from_uuid(a), 1, 100)
        .await
        .expect("all");
    assert_eq!(all.len(), 5);
    assert!(all.iter().all(|e| e.community_id == a));

    // from_seq past the head returns nothing.
    assert!(svc
        .get_entries(CommunityId::from_uuid(a), 6, 100)
        .await
        .expect("past head")
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_produce_valid_unbroken_chain() {
    let pool = setup_pool().await;
    let svc = Arc::new(AuditService::new_sqlite(pool.clone()));
    let c = make_community(&pool).await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let svc = Arc::clone(&svc);
        handles.push(tokio::spawn(async move {
            svc.log(new_entry(c, AuditAction::EventCreated)).await
        }));
    }

    let mut seqs = Vec::new();
    for h in handles {
        let e = h.await.expect("join").expect("log under contention");
        seqs.push(e.seq);
    }
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        (1..=10).collect::<Vec<i64>>(),
        "the append mutex must serialize writers into gapless distinct seqs"
    );

    assert!(svc
        .verify_chain(CommunityId::from_uuid(c), 1, 10)
        .await
        .expect("verify after contention"));
}

#[tokio::test]
async fn interleaved_communities_keep_independent_valid_chains() {
    let pool = setup_pool().await;
    let svc = AuditService::new_sqlite(pool.clone());
    let a = make_community(&pool).await;
    let b = make_community(&pool).await;

    let a1 = svc
        .log(new_entry(a, AuditAction::EventCreated))
        .await
        .expect("a1");
    let b1 = svc
        .log(new_entry(b, AuditAction::EventCreated))
        .await
        .expect("b1");
    let a2 = svc
        .log(new_entry(a, AuditAction::ChannelCreated))
        .await
        .expect("a2");
    let b2 = svc
        .log(new_entry(b, AuditAction::ChannelCreated))
        .await
        .expect("b2");

    // Each community's seq is independent and starts at 1.
    assert_eq!((a1.seq, a2.seq), (1, 2));
    assert_eq!((b1.seq, b2.seq), (1, 2));

    // A's chain links only within A, B's only within B — B1 written between
    // A1 and A2 must not appear in A's linkage.
    assert!(a1.prev_hash.is_none());
    assert!(b1.prev_hash.is_none());
    assert_eq!(a2.prev_hash.as_deref(), Some(a1.hash.as_slice()));
    assert_eq!(b2.prev_hash.as_deref(), Some(b1.hash.as_slice()));

    assert!(svc
        .verify_chain(CommunityId::from_uuid(a), 1, 2)
        .await
        .expect("verify a"));
    assert!(svc
        .verify_chain(CommunityId::from_uuid(b), 1, 2)
        .await
        .expect("verify b"));
}

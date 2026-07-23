//! SQLite arms for the deployment-global read-only admin plane (WP6, Phase 2d).
//!
//! Function-for-function port of [`crate::admin_moderation`], reusing its
//! [`AdminReport`]/[`AdminFeedback`] types and `MAX_PAGE_SIZE` clamp.
//! Function names match the `Db` facade methods for name-matched dispatch.
//! Like the Postgres arm, this is the only moderation surface allowed to
//! omit a `CommunityId` — reads span all communities and join `communities`
//! for host provenance.
//!
//! Translation notes:
//!
//! - `($n::type IS NULL OR …)` optional-filter casts drop to plain
//!   `(?n IS NULL OR …)` (SQLite is dynamically typed).
//! - The keyset cursor keeps the Postgres row-value comparison
//!   `(created_at, id) < (?, ?)` (supported since SQLite 3.15). Divergences:
//!   `created_at` is INTEGER unix seconds, so cursor timestamps truncate to
//!   second granularity, and the id tie-break compares lowercase hyphenated
//!   TEXT rather than Postgres UUID bytes — both orderings are total and
//!   self-consistent (the `ORDER BY` uses the same comparisons), so
//!   pagination never skips or repeats rows within this backend.

use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use super::channel::{datetime_from_unix, opt_datetime_from_unix};
use super::event::{parse_uuid_text, uuid_text};
use crate::admin_moderation::{AdminFeedback, AdminReport, MAX_PAGE_SIZE};
use crate::error::Result;

fn bounded_limit(limit: i64) -> i64 {
    limit.clamp(1, MAX_PAGE_SIZE)
}

/// Shared admin-report column list (joined against `communities c`).
const ADMIN_REPORT_COLUMNS: &str = "r.id, r.community_id, c.host AS community_host, \
     r.report_event_id, r.reporter_pubkey, r.target_kind, \
     r.target_event_id, r.target_pubkey, r.target_blob_sha256, \
     r.channel_id, r.report_type, r.note, r.status, r.resolved_by, \
     r.resolved_at, r.action_id, r.created_at";

/// Shared admin-feedback column list (joined against `communities c`).
const ADMIN_FEEDBACK_COLUMNS: &str = "f.id, f.community_id, c.host AS community_host, \
     f.event_id, f.submitter_pubkey, f.category, f.body, f.tags, \
     f.event_created_at, f.received_at";

fn get_optional_uuid(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<Option<Uuid>> {
    let raw: Option<String> = row.try_get(column)?;
    raw.as_deref().map(parse_uuid_text).transpose()
}

fn row_to_report(row: sqlx::sqlite::SqliteRow) -> Result<AdminReport> {
    let target_kind: String = row.try_get("target_kind")?;
    let target = match target_kind.as_str() {
        "event" => row.try_get::<Vec<u8>, _>("target_event_id")?,
        "pubkey" => row.try_get::<Vec<u8>, _>("target_pubkey")?,
        "blob" => row.try_get::<Vec<u8>, _>("target_blob_sha256")?,
        _ => Vec::new(),
    };
    let id: String = row.try_get("id")?;
    let community_id: String = row.try_get("community_id")?;
    let created_at: i64 = row.try_get("created_at")?;
    Ok(AdminReport {
        id: parse_uuid_text(&id)?,
        community_id: parse_uuid_text(&community_id)?,
        community_host: row.try_get("community_host")?,
        report_event_id: hex::encode(row.try_get::<Vec<u8>, _>("report_event_id")?),
        reporter_pubkey: hex::encode(row.try_get::<Vec<u8>, _>("reporter_pubkey")?),
        target_kind,
        target: hex::encode(target),
        channel_id: get_optional_uuid(&row, "channel_id")?,
        report_type: row.try_get("report_type")?,
        note: row.try_get("note")?,
        status: row.try_get("status")?,
        resolved_by: row
            .try_get::<Option<Vec<u8>>, _>("resolved_by")?
            .map(hex::encode),
        resolved_at: opt_datetime_from_unix(row.try_get("resolved_at")?)?,
        action_id: get_optional_uuid(&row, "action_id")?,
        created_at: datetime_from_unix(created_at)?,
    })
}

fn row_to_feedback(row: sqlx::sqlite::SqliteRow) -> Result<AdminFeedback> {
    let id: String = row.try_get("id")?;
    let community_id: String = row.try_get("community_id")?;
    let tags_text: String = row.try_get("tags")?;
    let event_created_at: i64 = row.try_get("event_created_at")?;
    let received_at: i64 = row.try_get("received_at")?;
    Ok(AdminFeedback {
        id: parse_uuid_text(&id)?,
        community_id: parse_uuid_text(&community_id)?,
        community_host: row.try_get("community_host")?,
        event_id: hex::encode(row.try_get::<Vec<u8>, _>("event_id")?),
        submitter_pubkey: hex::encode(row.try_get::<Vec<u8>, _>("submitter_pubkey")?),
        category: row.try_get("category")?,
        body: row.try_get("body")?,
        tags: serde_json::from_str(&tags_text)?,
        event_created_at: datetime_from_unix(event_created_at)?,
        received_at: datetime_from_unix(received_at)?,
    })
}

/// List reports across all communities by stable descending keyset.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn admin_list_reports(
    pool: &SqlitePool,
    community_id: Option<Uuid>,
    status: Option<&str>,
    report_type: Option<&str>,
    target_kind: Option<&str>,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<AdminReport>> {
    let (cursor_time, cursor_id) = cursor.unzip();
    let sql = format!(
        "SELECT {ADMIN_REPORT_COLUMNS} \
         FROM moderation_reports r \
         JOIN communities c ON c.id = r.community_id \
         WHERE (?1 IS NULL OR r.community_id = ?1) \
           AND (?2 IS NULL OR r.status = ?2) \
           AND (?3 IS NULL OR r.report_type = ?3) \
           AND (?4 IS NULL OR r.target_kind = ?4) \
           AND (?5 IS NULL OR r.created_at >= ?5) \
           AND (?6 IS NULL OR r.created_at < ?6) \
           AND (?7 IS NULL OR (r.created_at, r.id) < (?7, ?8)) \
         ORDER BY r.created_at DESC, r.id DESC \
         LIMIT ?9"
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community_id.map(uuid_text))
        .bind(status)
        .bind(report_type)
        .bind(target_kind)
        .bind(after.map(|t| t.timestamp()))
        .bind(before.map(|t| t.timestamp()))
        .bind(cursor_time.map(|t| t.timestamp()))
        .bind(cursor_id.map(uuid_text))
        .bind(bounded_limit(limit))
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_report).collect()
}

/// Fetch one report globally by its row id.
pub(crate) async fn admin_get_report(
    pool: &SqlitePool,
    report_id: Uuid,
) -> Result<Option<AdminReport>> {
    let sql = format!(
        "SELECT {ADMIN_REPORT_COLUMNS} \
         FROM moderation_reports r \
         JOIN communities c ON c.id = r.community_id \
         WHERE r.id = ?1"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(uuid_text(report_id))
        .fetch_optional(pool)
        .await?;
    row.map(row_to_report).transpose()
}

/// List product feedback across all communities, newest first.
pub(crate) async fn admin_list_feedback(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<AdminFeedback>> {
    let sql = format!(
        "SELECT {ADMIN_FEEDBACK_COLUMNS} \
         FROM product_feedback f \
         JOIN communities c ON c.id = f.community_id \
         ORDER BY f.received_at DESC, f.id DESC \
         LIMIT ?1"
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(bounded_limit(limit))
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_feedback).collect()
}

/// Fetch one feedback submission globally by its row id.
pub(crate) async fn admin_get_feedback(
    pool: &SqlitePool,
    id: Uuid,
) -> Result<Option<AdminFeedback>> {
    let sql = format!(
        "SELECT {ADMIN_FEEDBACK_COLUMNS} \
         FROM product_feedback f \
         JOIN communities c ON c.id = f.community_id \
         WHERE f.id = ?1"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(uuid_text(id))
        .fetch_optional(pool)
        .await?;
    row.map(row_to_feedback).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::moderation::{NewReport, ReportTarget};
    use crate::CommunityId;

    async fn setup() -> (SqlitePool, CommunityId, CommunityId) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect in-memory sqlite");
        crate::sqlite::run_migrations(&pool).await.expect("migrate");
        let a = crate::sqlite::community::ensure_configured_community(&pool, "a.example")
            .await
            .expect("community a")
            .id;
        let b = crate::sqlite::community::ensure_configured_community(&pool, "b.example")
            .await
            .expect("community b")
            .id;
        (pool, a, b)
    }

    fn pk(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    async fn insert_report(
        pool: &SqlitePool,
        community: CommunityId,
        report_event_id: &[u8],
        report_type: &str,
    ) -> Uuid {
        let reporter = pk(9);
        let target = pk(8);
        crate::sqlite::moderation::insert_moderation_report(
            pool,
            community,
            NewReport {
                report_event_id,
                reporter_pubkey: &reporter,
                target: ReportTarget::Event(target),
                channel_id: None,
                report_type,
                note: None,
            },
        )
        .await
        .expect("insert report")
    }

    #[tokio::test]
    async fn admin_reports_span_communities_and_filter() {
        let (pool, a, b) = setup().await;
        let id_a = insert_report(&pool, a, &pk(1), "spam").await;
        let id_b = insert_report(&pool, b, &pk(2), "illegal").await;

        // Unfiltered list spans both communities with host provenance.
        let all = admin_list_reports(&pool, None, None, None, None, None, None, None, 50)
            .await
            .expect("list all");
        assert_eq!(all.len(), 2);
        let hosts: Vec<&str> = all.iter().map(|r| r.community_host.as_str()).collect();
        assert!(hosts.contains(&"a.example") && hosts.contains(&"b.example"));

        // Community and report_type filters narrow the set.
        let only_a = admin_list_reports(
            &pool,
            Some(*a.as_uuid()),
            None,
            None,
            None,
            None,
            None,
            None,
            50,
        )
        .await
        .expect("list community a");
        assert_eq!(only_a.len(), 1);
        assert_eq!(only_a[0].id, id_a);

        let only_illegal = admin_list_reports(
            &pool,
            None,
            None,
            Some("illegal"),
            None,
            None,
            None,
            None,
            50,
        )
        .await
        .expect("list illegal");
        assert_eq!(only_illegal.len(), 1);
        assert_eq!(only_illegal[0].id, id_b);

        // Point read resolves globally, without a community id.
        let got = admin_get_report(&pool, id_b)
            .await
            .expect("get report")
            .expect("report exists");
        assert_eq!(got.community_host, "b.example");
        assert_eq!(got.target_kind, "event");
        assert_eq!(got.target, hex::encode(pk(8)));
    }

    #[tokio::test]
    async fn admin_report_cursor_paginates_without_overlap() {
        let (pool, a, _) = setup().await;
        insert_report(&pool, a, &pk(1), "spam").await;
        insert_report(&pool, a, &pk(2), "spam").await;
        insert_report(&pool, a, &pk(3), "spam").await;

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = admin_list_reports(&pool, None, None, None, None, None, None, cursor, 1)
                .await
                .expect("page");
            let Some(last) = page.last() else { break };
            cursor = Some((last.created_at, last.id));
            seen.extend(page.iter().map(|r| r.id));
        }
        assert_eq!(seen.len(), 3, "keyset pagination must visit every row");
        let mut dedup = seen.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), 3, "keyset pagination must not repeat rows");
    }

    #[tokio::test]
    async fn admin_feedback_lists_and_fetches_globally() {
        let (pool, a, b) = setup().await;
        for (community, event, body, category) in [
            (a, pk(1), "love it", Some("praise")),
            (b, pk(2), "broken button", Some("bug")),
        ] {
            sqlx::query(
                "INSERT INTO product_feedback \
                     (community_id, event_id, submitter_pubkey, category, body, tags, event_created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .bind(community.as_uuid().to_string())
            .bind(event)
            .bind(pk(7))
            .bind(category)
            .bind(body)
            .bind(r#"[["t","feedback"]]"#)
            .bind(1_700_000_000_i64)
            .execute(&pool)
            .await
            .expect("insert feedback");
        }

        let all = admin_list_feedback(&pool, 50).await.expect("list feedback");
        assert_eq!(all.len(), 2);
        let hosts: Vec<&str> = all.iter().map(|f| f.community_host.as_str()).collect();
        assert!(hosts.contains(&"a.example") && hosts.contains(&"b.example"));

        let bug = all
            .iter()
            .find(|f| f.category.as_deref() == Some("bug"))
            .expect("bug row listed");
        let got = admin_get_feedback(&pool, bug.id)
            .await
            .expect("get feedback")
            .expect("feedback exists");
        assert_eq!(got.body, "broken button");
        assert_eq!(got.event_id, hex::encode(pk(2)));
        assert_eq!(
            got.tags,
            serde_json::json!([["t", "feedback"]]),
            "tags TEXT column must round-trip as JSON"
        );
        assert_eq!(got.event_created_at.timestamp(), 1_700_000_000);

        assert!(admin_get_feedback(&pool, Uuid::new_v4())
            .await
            .expect("get missing")
            .is_none());
    }
}

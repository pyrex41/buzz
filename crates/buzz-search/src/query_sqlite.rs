//! NIP-50 search query against SQLite FTS5, community-scoped (Solo profile).
//!
//! Mirrors [`crate::query`] (the Postgres path) clause-for-clause over the
//! `events_fts` external-content table defined in
//! `buzz-db/src/sqlite/schema.sql`. The mapping from the Postgres path:
//!
//! | Postgres | SQLite |
//! |---|---|
//! | `websearch_to_tsquery('simple', $q)` | every whitespace token double-quoted, joined by implicit AND (see [`build_match_string`]) |
//! | trailing-token `:*` prefix tsquery | trailing quoted token suffixed `*` (`"pro"*`) |
//! | `ts_rank_cd(search_tsv, query)` (higher = better) | `-bm25(events_fts)` (bm25 is lower-is-better; negating restores the higher-is-better convention, so `ORDER BY rank DESC` is unchanged) |
//! | migration-0008 kind allowlist in the generated column | kind allowlist enforced at index time by the `trg_events_fts_*` triggers — unsearchable kinds are simply never in the index |
//! | `tsvector` `'simple'` config | `tokenize='unicode61'` (case-folding + word splitting, no stemming, no stopwords) |
//!
//! Quoting every token means no FTS5 query syntax (`NEAR`, `*`, `^`, `-`,
//! `OR`, parens, `col:`) survives into the MATCH expression — user input is
//! always literal text.
//!
//! As on Postgres, the relay never trusts a hit by itself: this layer returns
//! canonical event ids ordered by relevance, the relay refetches
//! `StoredEvent`s through buzz-db's `(community_id, event_id)` scoped
//! fetcher, and runs the access predicate per hit. Search is never the
//! access boundary — it cannot widen visibility (conformance row 50).

use sqlx::{QueryBuilder, Row, SqlitePool};
use uuid::Uuid;

use crate::error::SearchError;
use crate::query::{
    normalized_search_text, ChannelScope, SearchHit, SearchMode, SearchQuery, SearchResult,
    PAGE_MAX, PER_PAGE_DEFAULT, PER_PAGE_MAX,
};

/// Build a safe FTS5 MATCH expression from normalized search text.
///
/// Splits on whitespace and wraps every token in double quotes (internal `"`
/// doubled per FTS5 string escaping), so no query syntax can be injected —
/// `NEAR(a b)`, `foo*`, `-x`, `a OR b`, `col:val` all become literal terms.
/// Adjacent tokens are joined with a space, which FTS5 treats as implicit
/// AND — matching `websearch_to_tsquery`'s default AND semantics.
///
/// [`SearchMode::Prefix`] appends `*` after the closing quote of the LAST
/// token (`"pro"*`), mirroring the Postgres prefix-typeahead semantics:
/// completed tokens match exactly, only the trailing token prefix-matches.
///
/// Returns `None` when tokenization yields no tokens (caller short-circuits
/// to zero hits without SQL).
pub(crate) fn build_match_string(mode: SearchMode, search_text: &str) -> Option<String> {
    let tokens: Vec<&str> = search_text.split_whitespace().collect();
    let last = tokens.len().checked_sub(1)?;
    let mut out = String::with_capacity(search_text.len() + tokens.len() * 3);
    for (i, token) in tokens.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push('"');
        for ch in token.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
        if i == last && mode == SearchMode::Prefix {
            out.push('*');
        }
    }
    Some(out)
}

/// Execute a community-scoped FTS5 query.
///
/// SQL shape (always):
/// ```sql
/// SELECT e.id, e.kind, e.pubkey, e.channel_id, e.created_at,
///        -bm25(events_fts) AS rank
/// FROM events_fts
/// JOIN events e ON e.rowid = events_fts.rowid
/// WHERE events_fts MATCH $match
///   AND e.community_id = $ctx
///   AND e.deleted_at IS NULL
///   [+ channel scope, kinds, authors, since, until]
/// ORDER BY rank DESC, e.created_at DESC, e.id
/// LIMIT $per_page OFFSET (($page - 1) * $per_page)
/// ```
///
/// `e.community_id = $ctx` is non-negotiable. There is no code path through
/// this function that omits it. Filter semantics mirror the Postgres path
/// exactly, including the empty-vec edge cases documented on
/// [`ChannelScope`]: `Channels(vec![])` yields zero hits and
/// `ChannelsOrChannelLess(vec![])` is equivalent to `ChannelLessOnly`.
pub async fn search(pool: &SqlitePool, query: &SearchQuery) -> Result<SearchResult, SearchError> {
    let page = query.page.clamp(1, PAGE_MAX);
    let empty = |page| {
        Ok(SearchResult {
            hits: Vec::new(),
            page,
        })
    };
    let Some(search_text) = normalized_search_text(&query.q) else {
        return empty(page);
    };
    let Some(match_expr) = build_match_string(query.mode, &search_text) else {
        return empty(page);
    };

    let per_page = query.per_page.clamp(1, PER_PAGE_MAX);
    let per_page_actual = if query.per_page == 0 {
        PER_PAGE_DEFAULT
    } else {
        per_page
    };
    let offset = ((page - 1) as i64) * (per_page_actual as i64);

    let mut qb: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
        "SELECT e.id, e.kind, e.pubkey, e.channel_id, e.created_at, \
         -bm25(events_fts) AS rank \
         FROM events_fts \
         JOIN events e ON e.rowid = events_fts.rowid \
         WHERE events_fts MATCH ",
    );
    qb.push_bind(match_expr);
    qb.push(" AND e.community_id = ");
    // UUIDs are TEXT on the SQLite backend (see buzz-db sqlite conventions —
    // binding a raw `uuid::Uuid` would encode a 16-byte BLOB and miss).
    qb.push_bind(query.community.as_uuid().to_string());
    qb.push(" AND e.deleted_at IS NULL");

    // Channel scope — same four-case mapping as the Postgres path. SQLite has
    // no array types, so `= ANY($ids)` becomes an expanded `IN (?, ?, …)`;
    // the Postgres empty-vec semantics are preserved explicitly:
    // `ANY('{}')` is false-for-all-rows, so an empty `Channels` emits `1 = 0`.
    match &query.channel_scope {
        ChannelScope::Any => {
            // No channel constraint.
        }
        ChannelScope::ChannelLessOnly => {
            qb.push(" AND e.channel_id IS NULL");
        }
        ChannelScope::Channels(ids) => {
            if ids.is_empty() {
                qb.push(" AND 1 = 0");
            } else {
                qb.push(" AND e.channel_id IN (");
                push_uuid_list(&mut qb, ids);
                qb.push(")");
            }
        }
        ChannelScope::ChannelsOrChannelLess(ids) => {
            if ids.is_empty() {
                // Equivalent to ChannelLessOnly, exactly as
                // `(channel_id = ANY('{}') OR channel_id IS NULL)` is on Postgres.
                qb.push(" AND e.channel_id IS NULL");
            } else {
                qb.push(" AND (e.channel_id IN (");
                push_uuid_list(&mut qb, ids);
                qb.push(") OR e.channel_id IS NULL)");
            }
        }
    }

    // Empty kinds/authors vecs mean no constraint clause — mirroring the
    // Postgres path's `!is_empty()` guards.
    if let Some(ref kinds) = query.kinds {
        if !kinds.is_empty() {
            qb.push(" AND e.kind IN (");
            let mut sep = qb.separated(", ");
            for kind in kinds {
                sep.push_bind(*kind);
            }
            qb.push(")");
        }
    }

    if let Some(ref authors) = query.authors {
        if !authors.is_empty() {
            qb.push(" AND e.pubkey IN (");
            let mut sep = qb.separated(", ");
            for author in authors {
                sep.push_bind(author.clone());
            }
            qb.push(")");
        }
    }

    if let Some(since) = query.since {
        qb.push(" AND e.created_at >= ");
        qb.push_bind(since);
    }

    if let Some(until) = query.until {
        qb.push(" AND e.created_at <= ");
        qb.push_bind(until);
    }

    qb.push(" ORDER BY rank DESC, e.created_at DESC, e.id LIMIT ");
    qb.push_bind(per_page_actual as i64);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build().fetch_all(pool).await?;

    let mut hits = Vec::with_capacity(rows.len());
    for row in rows {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let pk_bytes: Vec<u8> = row.try_get("pubkey")?;
        let id: [u8; 32] = id_bytes.try_into().map_err(|v: Vec<u8>| {
            sqlx::Error::Decode(format!("event id column is {} bytes, expected 32", v.len()).into())
        })?;
        let pubkey: [u8; 32] = pk_bytes.try_into().map_err(|v: Vec<u8>| {
            sqlx::Error::Decode(format!("pubkey column is {} bytes, expected 32", v.len()).into())
        })?;
        let channel_text: Option<String> = row.try_get("channel_id")?;
        let channel_id = channel_text
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|e| {
                sqlx::Error::Decode(format!("invalid channel_id uuid text: {e}").into())
            })?;
        let rank: f64 = row.try_get("rank")?;
        hits.push(SearchHit {
            event_id: id,
            kind: row.try_get("kind")?,
            pubkey,
            channel_id,
            created_at: row.try_get("created_at")?,
            rank: rank as f32,
        });
    }

    Ok(SearchResult { hits, page })
}

/// Push a comma-separated list of UUID binds in their canonical TEXT form.
fn push_uuid_list(qb: &mut QueryBuilder<sqlx::Sqlite>, ids: &[Uuid]) {
    let mut sep = qb.separated(", ");
    for id in ids {
        sep.push_bind(id.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::channel::{ChannelType, ChannelVisibility};
    use buzz_core::CommunityId;
    use buzz_db::Db;
    use nostr::{EventBuilder, Keys, Kind, Timestamp};

    // ── MATCH-string builder unit tests ─────────────────────────────────────

    #[test]
    fn match_string_quotes_every_token_with_implicit_and() {
        assert_eq!(
            build_match_string(SearchMode::FullText, "foo bar").as_deref(),
            Some(r#""foo" "bar""#)
        );
    }

    #[test]
    fn match_string_doubles_internal_quotes() {
        assert_eq!(
            build_match_string(SearchMode::FullText, r#"fo"o"#).as_deref(),
            Some(r#""fo""o""#)
        );
        assert_eq!(
            build_match_string(SearchMode::FullText, r#"foo" OR "bar"#).as_deref(),
            Some(r#""foo""" "OR" """bar""#)
        );
    }

    #[test]
    fn match_string_neutralizes_fts5_operators() {
        assert_eq!(
            build_match_string(SearchMode::FullText, "NEAR(a b)").as_deref(),
            Some(r#""NEAR(a" "b)""#)
        );
        assert_eq!(
            build_match_string(SearchMode::FullText, "-x col:val ^y").as_deref(),
            Some(r#""-x" "col:val" "^y""#)
        );
    }

    #[test]
    fn match_string_prefix_stars_only_the_last_token() {
        assert_eq!(
            build_match_string(SearchMode::Prefix, "pro").as_deref(),
            Some(r#""pro"*"#)
        );
        assert_eq!(
            build_match_string(SearchMode::Prefix, "project pl").as_deref(),
            Some(r#""project" "pl"*"#)
        );
    }

    #[test]
    fn match_string_empty_input_yields_none() {
        assert_eq!(build_match_string(SearchMode::FullText, ""), None);
        assert_eq!(build_match_string(SearchMode::Prefix, ""), None);
    }

    // ── End-to-end tests over the real buzz-db SQLite backend ───────────────
    //
    // Events are inserted through the PUBLIC `Db` surface so the schema's
    // `trg_events_fts_*` triggers maintain the index exactly as in
    // production; the search side then runs over a clone of the same pool.

    struct TestDb {
        db: Db,
        pool: sqlx::SqlitePool,
        community: CommunityId,
        path: String,
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-wal", self.path));
            let _ = std::fs::remove_file(format!("{}-shm", self.path));
        }
    }

    async fn test_db() -> TestDb {
        let path = std::env::temp_dir()
            .join(format!("buzz-search-fts5-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned();
        let db = Db::new_sqlite(&path).await.expect("open sqlite db");
        db.migrate().await.expect("migrate");
        let community = db
            .ensure_configured_community("solo.example")
            .await
            .expect("ensure community")
            .id;
        let pool = db.sqlite_pool().expect("sqlite pool");
        TestDb {
            db,
            pool,
            community,
            path,
        }
    }

    async fn make_channel(t: &TestDb, name: &str) -> Uuid {
        let creator = Keys::generate().public_key().to_bytes().to_vec();
        t.db.create_channel(
            t.community,
            name,
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &creator,
            None,
        )
        .await
        .expect("create channel")
        .id
    }

    async fn insert(
        t: &TestDb,
        keys: &Keys,
        kind: u16,
        content: &str,
        channel: Option<Uuid>,
        created_at: Option<i64>,
    ) -> [u8; 32] {
        insert_in(t, t.community, keys, kind, content, channel, created_at).await
    }

    async fn insert_in(
        t: &TestDb,
        community: CommunityId,
        keys: &Keys,
        kind: u16,
        content: &str,
        channel: Option<Uuid>,
        created_at: Option<i64>,
    ) -> [u8; 32] {
        let mut builder = EventBuilder::new(Kind::Custom(kind), content);
        if let Some(ch) = channel {
            builder = builder.tags([nostr::Tag::parse(["h", &ch.to_string()]).expect("h tag")]);
        }
        if let Some(ts) = created_at {
            builder = builder.custom_created_at(Timestamp::from(ts as u64));
        }
        let event = builder.sign_with_keys(keys).expect("sign");
        let (_, inserted) =
            t.db.insert_event(community, &event, channel)
                .await
                .expect("insert event");
        assert!(inserted, "fresh event must insert");
        event.id.to_bytes()
    }

    fn query(t: &TestDb, q: &str) -> SearchQuery {
        SearchQuery {
            community: t.community,
            q: q.to_string(),
            channel_scope: ChannelScope::Any,
            kinds: None,
            authors: None,
            since: None,
            until: None,
            page: 1,
            per_page: 100,
            mode: SearchMode::FullText,
        }
    }

    fn ids(result: &SearchResult) -> Vec<[u8; 32]> {
        result.hits.iter().map(|h| h.event_id).collect()
    }

    #[tokio::test]
    async fn full_text_match_returns_the_right_event_with_rank() {
        let t = test_db().await;
        let keys = Keys::generate();
        let hit_id = insert(&t, &keys, 9, "the quick brown fox", None, None).await;
        insert(&t, &keys, 9, "completely unrelated words", None, None).await;

        let result = search(&t.pool, &query(&t, "fox")).await.expect("search");
        assert_eq!(ids(&result), vec![hit_id]);
        assert_eq!(result.page, 1);
        let rank = result.hits[0].rank;
        assert!(
            rank.is_finite() && rank > 0.0,
            "-bm25 must be positive, got {rank}"
        );
        assert_eq!(result.hits[0].kind, 9);
        assert_eq!(result.hits[0].pubkey, keys.public_key().to_bytes());
        assert_eq!(result.hits[0].channel_id, None);
    }

    #[tokio::test]
    async fn equal_rank_hits_order_by_created_at_desc_stable() {
        let t = test_db().await;
        let keys = Keys::generate();
        let older = insert(&t, &keys, 9, "stable ordering probe", None, Some(1000)).await;
        let middle = insert(&t, &keys, 9, "stable ordering probe", None, Some(2000)).await;
        let newer = insert(&t, &keys, 9, "stable ordering probe", None, Some(3000)).await;

        let result = search(&t.pool, &query(&t, "ordering"))
            .await
            .expect("search");
        assert_eq!(ids(&result), vec![newer, middle, older]);
    }

    #[tokio::test]
    async fn prefix_mode_matches_trailing_token_prefix_only() {
        let t = test_db().await;
        let keys = Keys::generate();
        let id = insert(&t, &keys, 9, "project plan review", None, None).await;

        // Trailing token prefix-matches.
        let mut q = query(&t, "pro");
        q.mode = SearchMode::Prefix;
        let result = search(&t.pool, &q).await.expect("prefix search");
        assert_eq!(ids(&result), vec![id]);

        // Completed (non-trailing) tokens stay exact: "proj" != "project".
        let mut q = query(&t, "proj plan");
        q.mode = SearchMode::Prefix;
        let result = search(&t.pool, &q).await.expect("prefix search");
        assert!(result.hits.is_empty(), "completed token must match exactly");

        // FullText mode gets no prefixing at all.
        let result = search(&t.pool, &query(&t, "pro")).await.expect("search");
        assert!(result.hits.is_empty(), "full-text must not prefix-match");
    }

    #[tokio::test]
    async fn community_fence_holds() {
        let t = test_db().await;
        let other =
            t.db.ensure_configured_community("other.example")
                .await
                .expect("second community")
                .id;
        let keys = Keys::generate();
        let mine = insert(&t, &keys, 9, "fenced content", None, None).await;
        // Same content stored under the other community (distinct event id
        // via a distinct timestamp).
        insert_in(&t, other, &keys, 9, "fenced content", None, Some(12345)).await;

        let result = search(&t.pool, &query(&t, "fenced")).await.expect("search");
        assert_eq!(ids(&result), vec![mine], "must only see this community");
    }

    #[tokio::test]
    async fn channel_scopes_mirror_postgres_semantics() {
        let t = test_db().await;
        let keys = Keys::generate();
        let chan_a = make_channel(&t, "alpha").await;
        let chan_b = make_channel(&t, "beta").await;
        let in_a = insert(&t, &keys, 9, "scoped haystack", Some(chan_a), Some(1000)).await;
        let in_b = insert(&t, &keys, 9, "scoped haystack", Some(chan_b), Some(2000)).await;
        let chanless = insert(&t, &keys, 9, "scoped haystack", None, Some(3000)).await;

        let run = |scope: ChannelScope| {
            let mut q = query(&t, "scoped");
            q.channel_scope = scope;
            let pool = &t.pool;
            async move { ids(&search(pool, &q).await.expect("search")) }
        };

        assert_eq!(run(ChannelScope::Any).await, vec![chanless, in_b, in_a]);
        assert_eq!(run(ChannelScope::Channels(vec![chan_a])).await, vec![in_a]);
        assert_eq!(run(ChannelScope::ChannelLessOnly).await, vec![chanless]);
        assert_eq!(
            run(ChannelScope::ChannelsOrChannelLess(vec![chan_a])).await,
            vec![chanless, in_a]
        );
        // Empty-vec edge cases, exactly as on Postgres:
        assert_eq!(
            run(ChannelScope::Channels(vec![])).await,
            Vec::<[u8; 32]>::new()
        );
        assert_eq!(
            run(ChannelScope::ChannelsOrChannelLess(vec![])).await,
            vec![chanless]
        );
        // Hits inside a channel surface the channel id.
        let mut q = query(&t, "scoped");
        q.channel_scope = ChannelScope::Channels(vec![chan_a]);
        let result = search(&t.pool, &q).await.expect("search");
        assert_eq!(result.hits[0].channel_id, Some(chan_a));
    }

    #[tokio::test]
    async fn kinds_and_authors_filters() {
        let t = test_db().await;
        let alice = Keys::generate();
        let bob = Keys::generate();
        let alice_9 = insert(&t, &alice, 9, "filterable text", None, Some(1000)).await;
        let bob_45001 = insert(&t, &bob, 45001, "filterable text", None, Some(2000)).await;

        let mut q = query(&t, "filterable");
        q.kinds = Some(vec![9]);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![alice_9]
        );

        let mut q = query(&t, "filterable");
        q.authors = Some(vec![bob.public_key().to_bytes().to_vec()]);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![bob_45001]
        );

        // Empty vecs mean NO constraint (mirrors the Pg path's !is_empty guard).
        let mut q = query(&t, "filterable");
        q.kinds = Some(vec![]);
        q.authors = Some(vec![]);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![bob_45001, alice_9]
        );
    }

    #[tokio::test]
    async fn since_until_bounds_are_inclusive() {
        let t = test_db().await;
        let keys = Keys::generate();
        let at_1000 = insert(&t, &keys, 9, "timebound probe", None, Some(1000)).await;
        let at_2000 = insert(&t, &keys, 9, "timebound probe", None, Some(2000)).await;
        let at_3000 = insert(&t, &keys, 9, "timebound probe", None, Some(3000)).await;

        let mut q = query(&t, "timebound");
        q.since = Some(2000);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![at_3000, at_2000]
        );

        let mut q = query(&t, "timebound");
        q.until = Some(2000);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![at_2000, at_1000]
        );

        let mut q = query(&t, "timebound");
        q.since = Some(2000);
        q.until = Some(2000);
        assert_eq!(
            ids(&search(&t.pool, &q).await.expect("search")),
            vec![at_2000]
        );
    }

    #[tokio::test]
    async fn soft_deleted_events_are_excluded() {
        let t = test_db().await;
        let keys = Keys::generate();
        let id = insert(&t, &keys, 9, "doomed content", None, None).await;

        assert_eq!(
            ids(&search(&t.pool, &query(&t, "doomed")).await.expect("search")),
            vec![id]
        );
        assert!(t
            .db
            .soft_delete_event(t.community, &id)
            .await
            .expect("soft delete"));
        let result = search(&t.pool, &query(&t, "doomed")).await.expect("search");
        assert!(result.hits.is_empty(), "soft-deleted events must not match");
    }

    #[tokio::test]
    async fn unsearchable_kinds_never_match() {
        let t = test_db().await;
        let keys = Keys::generate();
        // Kind 1059 (gift wrap) is outside the trigger allowlist — stored but
        // never indexed.
        insert(&t, &keys, 1059, "sealed wrapcontent", None, None).await;

        let result = search(&t.pool, &query(&t, "wrapcontent"))
            .await
            .expect("search");
        assert!(result.hits.is_empty(), "kind 1059 must be unsearchable");
    }

    #[tokio::test]
    async fn fts5_query_syntax_is_treated_as_literal_text() {
        let t = test_db().await;
        let keys = Keys::generate();
        let foo_bar = insert(&t, &keys, 9, "foo bar", None, Some(1000)).await;
        insert(&t, &keys, 9, "foodie town", None, Some(2000)).await;

        // An FTS5 OR operator would match "foo bar"; treated literally the
        // token OR is required and absent — zero hits, no syntax error.
        let result = search(&t.pool, &query(&t, r#"foo" OR "bar"#))
            .await
            .expect("no injection");
        assert!(
            result.hits.is_empty(),
            "OR must be literal, not an operator"
        );

        // A star must not act as a prefix operator: "foodie" stays unmatched.
        let result = search(&t.pool, &query(&t, "foo*")).await.expect("no star");
        assert_eq!(ids(&result), vec![foo_bar]);

        // The rest must simply not error.
        for hostile in [
            "NEAR(a b)",
            "-x",
            "col:val",
            "^caret",
            "(paren",
            "\"",
            "a AND b",
        ] {
            search(&t.pool, &query(&t, hostile))
                .await
                .unwrap_or_else(|e| panic!("query {hostile:?} must not error: {e}"));
        }
    }

    #[tokio::test]
    async fn empty_or_whitespace_query_short_circuits() {
        let t = test_db().await;
        let keys = Keys::generate();
        insert(&t, &keys, 9, "present content", None, None).await;

        for q_text in ["", "   ", "\t\n"] {
            let result = search(&t.pool, &query(&t, q_text)).await.expect("empty q");
            assert!(result.hits.is_empty());
            assert_eq!(result.page, 1);
        }
    }

    #[tokio::test]
    async fn pagination_windows_hits() {
        let t = test_db().await;
        let keys = Keys::generate();
        let a = insert(&t, &keys, 9, "paged content", None, Some(1000)).await;
        let b = insert(&t, &keys, 9, "paged content", None, Some(2000)).await;
        let c = insert(&t, &keys, 9, "paged content", None, Some(3000)).await;

        let mut q = query(&t, "paged");
        q.per_page = 2;
        q.page = 1;
        let page1 = search(&t.pool, &q).await.expect("page 1");
        assert_eq!(ids(&page1), vec![c, b]);
        q.page = 2;
        let page2 = search(&t.pool, &q).await.expect("page 2");
        assert_eq!(ids(&page2), vec![a]);
        assert_eq!(page2.page, 2);
    }
}

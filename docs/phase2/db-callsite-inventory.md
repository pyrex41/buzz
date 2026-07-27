# Db Call-Site Inventory (Phase 2 — SQLite/Solo backend)

Ground truth for which `buzz_db::Db` methods production code actually calls, and
from where. Produced for the Solo-profile SQLite backend work: methods not
reachable on Solo can return `UnsupportedBackend`.

**Method.** The 218 `pub fn`/`pub async fn` items on the `Db` facade (plus the
free `insert_mentions` and companion types) in `crates/buzz-db/src/lib.rs` were
each grepped as `.<method>(` / `::<method>(` across every workspace crate that
depends on `buzz-db`: **buzz-relay**, **buzz-workflow**, and **buzz-admin**
(the only three — verified via `grep buzz-db crates/*/Cargo.toml`; `buzz-audit`
takes a raw `PgPool`, not `Db`). Hits inside `#[cfg(test)]` items were
classified test-only and excluded from "production". Ambiguously named methods
(`new`, `read`, `ping`, `archive`, `is_live`, …) were manually disambiguated by
receiver.

**Solo-reachable? = NO** only when *every* production call site sits in a
subsystem that is config-gated off in the Solo profile:

- push runtime / push gateway (`push_runtime.rs`, `handlers/push_lease.rs`) — gated on `push_gateway_delivery_url`
- mesh/tunnel (`mesh_boot.rs`, `tunnel/`) — **calls no Db methods at all** (verified)
- replica/fence plumbing (read pool, `ReplicaFence`)
- usage-metrics poller — **noted separately as NO\*** because the poller loop *runs by default* (see §4)
- storage sweep — **calls no Db methods** (media/S3 only, verified)
- partition maintenance in `main.rs` (`ensure_future_partitions`)

Everything else = YES. Line numbers refer to the working tree of branch
`claude/hive-workspace-architecture-wzzji6` and were re-verified against the
current tree. **Caveat:** `crates/buzz-db/src/lib.rs` is being concurrently
modified on this branch by the SQLite backend work (it has already gained
`new_sqlite` and lost the unused `read` accessor since analysis started), so
buzz-db-internal line numbers are volatile; the consumer-crate call-site lines
below are stable (those files are untouched by that work). Paths abbreviated:
`relay/` = `crates/buzz-relay/src/`, `workflow/` = `crates/buzz-workflow/src/`,
`admin/` = `crates/buzz-admin/src/`.

---

## 1. Production call sites per Db method


| Db method | Production call sites (file:line — context) | Solo-reachable? |
|---|---|---|
| `is_live` | `relay/main.rs:1393` — startup/background. *on `UsageMetricsLeader`, not `Db`; leader-lock liveness check in the usage poller* | **NO\*** — usage poller only |
| `new` | `relay/main.rs:150` — startup/background; `admin/main.rs:423` — buzz-admin CLI | YES |
| `fence` | `relay/main.rs:965` — startup/background | **NO** — replica/fence plumbing |
| `spawn_fence_probe` | `relay/main.rs:185` — startup/background | **NO** — replica/fence plumbing |
| `has_read_pool` | `relay/main.rs:154` — startup/background | **NO** — replica/fence plumbing |
| `migrate` | `relay/main.rs:163` — startup/background; `admin/main.rs:141` — buzz-admin CLI. *gated by `BUZZ_AUTO_MIGRATE`; also buzz-admin `migrate`* | YES |
| `ping` | `relay/router.rs:354` — /health handler | YES |
| `pool_stats` | `relay/main.rs:948` — startup/background. *pool-metrics loop, always on; needs at least a stub on SQLite* | YES |
| `read_pool_stats` | `relay/main.rs:955` — startup/background | **NO** — replica/fence plumbing |
| `try_lock_usage_metrics` | `relay/main.rs:1402` — startup/background | **NO\*** — usage poller only |
| `admin_list_reports` | `relay/api/admin/mod.rs:111` — admin HTTP API | YES |
| `admin_get_report` | `relay/api/admin/mod.rs:133` — admin HTTP API | YES |
| `admin_list_feedback` | `relay/api/admin/mod.rs:158` — admin HTTP API | YES |
| `admin_get_feedback` | `relay/api/admin/mod.rs:185,203` — admin HTTP API | YES |
| `usage_community_count` | `relay/main.rs:1478` — startup/background | **NO\*** — usage poller only |
| `usage_user_counts` | `relay/main.rs:1479` — startup/background | **NO\*** — usage poller only |
| `usage_channel_counts` | `relay/main.rs:1480` — startup/background | **NO\*** — usage poller only |
| `usage_message_counts` | `relay/main.rs:1481` — startup/background | **NO\*** — usage poller only |
| `usage_relay_member_counts` | `relay/main.rs:1482` — startup/background | **NO\*** — usage poller only |
| `usage_workflow_counts` | `relay/main.rs:1483` — startup/background | **NO\*** — usage poller only |
| `usage_git_repo_counts` | `relay/main.rs:1484` — startup/background | **NO\*** — usage poller only |
| `usage_active_user_counts` | `relay/main.rs:1485,1486,1487` — startup/background | **NO\*** — usage poller only |
| `usage_active_channel_counts` | `relay/main.rs:1488,1489` — startup/background | **NO\*** — usage poller only |
| `usage_community_hosts` | `relay/push_runtime.rs:320` — push matcher/delivery; `relay/main.rs:1375` — startup/background; `relay/handlers/side_effects.rs:2763` — event side-effects. *called by push (gated), the usage poller, **and** `reconcile_nip43_membership_snapshots` (serve path when `require_relay_membership`) — so YES* | YES |
| `persist_command_event` | `relay/handlers/command_executor.rs:95` — command events | YES |
| `lookup_community_by_host` | `relay/tenant.rs:142` — row-zero host→community resolution; `admin/main.rs:450` — buzz-admin CLI | YES |
| `is_community_active` | `relay/state.rs:1150` — AppState visibility cache fill; `relay/connection.rs:135` — WS connection lifecycle; `relay/audio/handler.rs:161` — huddle audio | YES |
| `lookup_community_by_host_for_management` | `relay/api/operator.rs:488` — operator provisioning API | YES |
| `list_communities_owned_by` | `relay/api/operator.rs:327` — operator provisioning API | YES |
| `lookup_community_host` | `relay/workflow_sink.rs:201` — workflow action sink; `relay/api/operator.rs:437` — operator provisioning API | YES |
| `get_community_icon` | `relay/nip11.rs:283` — NIP-11 info doc | YES |
| `set_community_icon` | `relay/handlers/relay_admin.rs:157` — NIP-43 relay admin | YES |
| `ensure_configured_community` | `relay/main.rs:246` — startup/background; `relay/handlers/community_provisioning.rs:323` — community provisioning | YES |
| `create_community_with_owner` | `relay/handlers/community_provisioning.rs:286` — community provisioning | YES |
| `archive_community_owned_by` | `relay/api/operator.rs:233` — operator provisioning API | YES |
| `unarchive_community_owned_by` | `relay/api/operator.rs:288` — operator provisioning API | YES |
| `communities_of_channels` | `relay/handlers/req.rs:349,665` — REQ handler | YES |
| `insert_event` | `relay/handlers/moderation_notices.rs:174` — moderation DM notices; `relay/handlers/side_effects.rs:678,845,2739,… (5 sites)` — event side-effects; `relay/audio/handler.rs:1281` — huddle audio; `relay/api/git/transport.rs:1614` — git smart HTTP | YES |
| `query_events` | `relay/handlers/req.rs:313` — REQ handler; `relay/handlers/identity_archive.rs:273` — identity archive; `relay/handlers/count.rs:175,245` — COUNT handler; `relay/handlers/moderation_notices.rs:236` — moderation DM notices; `relay/handlers/side_effects.rs:908,2951,3082` — event side-effects; `relay/api/bridge.rs:500,1269,1476,1540` — HTTP bridge; `relay/api/git/policy.rs:262` — git push policy; `admin/main.rs:499` — buzz-admin CLI | YES |
| `count_events` | `relay/handlers/count.rs:164,235` — COUNT handler; `relay/api/bridge.rs:1466,1531` — HTTP bridge | YES |
| `huddle_started_link_exists` | `relay/audio/handler.rs:1179` — huddle audio | YES |
| `get_latest_global_replaceable` | `admin/main.rs:331` — buzz-admin CLI | YES — buzz-admin CLI only |
| `get_event_by_id` | `relay/handlers/report.rs:56` — NIP-56 reports; `relay/handlers/ingest.rs:358,602,638,… (7 sites)` — event ingest pipeline; `relay/handlers/side_effects.rs:558,2188` — event side-effects | YES |
| `get_event_by_id_including_deleted` | `relay/handlers/side_effects.rs:217,1579,2110` — event side-effects | YES |
| `soft_delete_by_coordinate` | `relay/handlers/side_effects.rs:2061` — event side-effects | YES |
| `soft_delete_event_and_update_thread` | `relay/handlers/side_effects.rs:1610,2133` — event side-effects | YES |
| `get_events_by_ids` | `relay/handlers/req.rs:636` — REQ handler; `relay/api/bridge.rs:1717` — HTTP bridge | YES |
| `claim_due_push_match_batch` | `relay/push_runtime.rs:72` — push matcher/delivery | **NO** — push subsystem |
| `active_push_match_leases` | `relay/push_runtime.rs:102` — push matcher/delivery | **NO** — push subsystem |
| `complete_push_match_batch` | `relay/push_runtime.rs:199` — push matcher/delivery | **NO** — push subsystem |
| `retry_push_match_batch` | `relay/push_runtime.rs:141,206` — push matcher/delivery | **NO** — push subsystem |
| `reap_exhausted_push_matches` | `relay/push_runtime.rs:62` — push matcher/delivery | **NO** — push subsystem |
| `enqueue_push_wakes` | `relay/push_runtime.rs:184` — push matcher/delivery | **NO** — push subsystem |
| `claim_due_push_wakes` | `relay/push_runtime.rs:325` — push matcher/delivery | **NO** — push subsystem |
| `revalidate_push_wake` | `relay/push_runtime.rs:356,405` — push matcher/delivery | **NO** — push subsystem |
| `complete_push_wake` | `relay/push_runtime.rs:438,494` — push matcher/delivery | **NO** — push subsystem |
| `retry_push_wake` | `relay/push_runtime.rs:390,541` — push matcher/delivery | **NO** — push subsystem |
| `fail_push_wake` | `relay/push_runtime.rs:363,382,412,… (7 sites)` — push matcher/delivery | **NO** — push subsystem |
| `disable_push_endpoint` | `relay/push_runtime.rs:457` — push matcher/delivery | **NO** — push subsystem |
| `accept_push_lease_event` | `relay/handlers/push_lease.rs:563` — NIP-PL lease accept. *handler returns early when `push_gateway_delivery_url` is unset (push_lease.rs:480)* | **NO** — push subsystem |
| `insert_event_with_thread_metadata` | `relay/workflow_sink.rs:339` — workflow action sink; `relay/handlers/ingest.rs:2389` — event ingest pipeline | YES |
| `insert_reaction_event_with_thread_metadata` | `relay/handlers/ingest.rs:2293` — event ingest pipeline | YES |
| `create_channel` | `relay/handlers/side_effects.rs:1688,1710` — event side-effects | YES |
| `create_channel_with_id` | `relay/handlers/ingest.rs:2099` — event ingest pipeline | YES |
| `get_channel` | `relay/state.rs:1211` — AppState visibility cache fill; `relay/workflow_sink.rs:223` — workflow action sink; `relay/handlers/command_executor.rs:350,480,632` — command events; `relay/handlers/ingest.rs:513,820,1739` — event ingest pipeline; `relay/handlers/side_effects.rs:282,590,953,… (6 sites)` — event side-effects; `relay/audio/handler.rs:389,1164` — huddle audio; `relay/api/git/policy.rs:308` — git push policy | YES |
| `add_member` | `relay/handlers/side_effects.rs:1206,1855` — event side-effects; `relay/audio/handler.rs:1219` — huddle audio | YES |
| `remove_member` | `relay/handlers/side_effects.rs:1277,1923` — event side-effects | YES |
| `is_member` | `relay/state.rs:910` — AppState visibility cache fill; `relay/push_runtime.rs:375` — push matcher/delivery; `relay/handlers/req.rs:139` — REQ handler; `relay/handlers/count.rs:125` — COUNT handler | YES |
| `membership_pairs` | `relay/push_runtime.rs:115` — push matcher/delivery | **NO** — push subsystem |
| `get_members` | `relay/workflow_sink.rs:275` — workflow action sink; `relay/handlers/command_executor.rs:360` — command events; `relay/handlers/side_effects.rs:102,305,374,… (12 sites)` — event side-effects; `admin/main.rs:513` — buzz-admin CLI | YES |
| `get_accessible_channel_ids` | `relay/state.rs:1173` — AppState visibility cache fill | YES |
| `list_channels` | `relay/handlers/side_effects.rs:2940` — event side-effects; `admin/main.rs:485` — buzz-admin CLI. *relay site is `reconcile_channel_events` (runs only with `BUZZ_RECONCILE_CHANNELS`, dev/CI); admin CLI also calls it* | YES |
| `get_users_bulk` | `relay/workflow_sink.rs:281` — workflow action sink | YES |
| `update_channel` | `relay/handlers/side_effects.rs:1338,1351,1400,1444` — event side-effects | YES |
| `set_topic` | `relay/handlers/side_effects.rs:1364` — event side-effects | YES |
| `set_purpose` | `relay/handlers/side_effects.rs:1379` — event side-effects | YES |
| `archive_channel` | `relay/handlers/side_effects.rs:1468` — event side-effects; `relay/audio/handler.rs:838` — huddle audio | YES |
| `unarchive_channel` | `relay/handlers/side_effects.rs:1483` — event side-effects | YES |
| `soft_delete_channel` | `relay/handlers/ingest.rs:2404` — event ingest pipeline; `relay/handlers/side_effects.rs:1781` — event side-effects | YES |
| `get_member_role` | `relay/handlers/moderation_authz.rs:126` — moderation authz; `relay/api/git/policy.rs:358` — git push policy | YES |
| `reap_expired_ephemeral_channels` | `relay/main.rs:615` — startup/background. *ephemeral channel reaper loop, always on* | YES |
| `query_due_reminders` | `relay/main.rs:720` — startup/background. *reminder scheduler loop, always on* | YES |
| `claim_due_reminder_with_stamp` | `relay/main.rs:755` — startup/background. *reminder scheduler loop, always on* | YES |
| `release_due_reminder` | `relay/main.rs:792` — startup/background. *reminder scheduler loop, always on* | YES |
| `ensure_user` | `relay/handlers/command_executor.rs:47` — command events; `relay/handlers/side_effects.rs:1080,1138` — event side-effects; `relay/api/mod.rs:183` — agent-owner HTTP API | YES |
| `get_user` | `relay/api/media.rs:264` — media (Blossom) auth | YES |
| `update_user_profile` | `relay/handlers/side_effects.rs:1154,1171` — event side-effects | YES |
| `get_user_by_nip05` | `relay/api/nip05.rs:49` — NIP-05 | YES |
| `set_agent_owner` | `relay/api/mod.rs:203` — agent-owner HTTP API | YES |
| `get_agent_channel_policy` | `relay/handlers/ingest.rs:1338` — event ingest pipeline; `relay/handlers/side_effects.rs:340` — event side-effects | YES |
| `is_agent_owner` | `relay/handlers/event.rs:1001` — EVENT dispatch; `relay/handlers/ingest.rs:833,2003` — event ingest pipeline; `relay/handlers/side_effects.rs:206,226,251,… (5 sites)` — event side-effects; `relay/api/mod.rs:209` — agent-owner HTTP API; `relay/api/git/policy.rs:333` — git push policy | YES |
| `set_channel_add_policy` | `relay/handlers/side_effects.rs:1091` — event side-effects | YES |
| `open_dm` | `relay/handlers/command_executor.rs:236,397` — command events; `relay/handlers/moderation_notices.rs:102` — moderation DM notices | YES |
| `hide_dm` | `relay/handlers/command_executor.rs:502` — command events | YES |
| `unhide_dm` | `relay/handlers/moderation_notices.rs:127` — moderation DM notices | YES |
| `list_hidden_dms` | `relay/handlers/side_effects.rs:3050` — event side-effects | YES |
| `get_thread_replies` | `relay/api/bridge.rs:1171` — HTTP bridge | YES |
| `get_thread_summary` | `relay/handlers/side_effects.rs:721` — event side-effects | YES |
| `get_channel_window` | `relay/api/bridge.rs:467` — HTTP bridge | YES |
| `get_thread_metadata_by_event` | `relay/handlers/ingest.rs:605` — event ingest pipeline; `relay/handlers/side_effects.rs:1600,2126` — event side-effects | YES |
| `remove_reaction` | `relay/handlers/side_effects.rs:2198` — event side-effects | YES |
| `remove_reaction_by_source_event_id` | `relay/handlers/side_effects.rs:2156` — event side-effects | YES |
| `query_feed_mentions` | `relay/api/bridge.rs:1086` — HTTP bridge | YES |
| `query_feed_needs_action` | `relay/api/bridge.rs:1097` — HTTP bridge | YES |
| `query_feed_activity` | `relay/api/bridge.rs:1108` — HTTP bridge | YES |
| `upsert_workflow` | `relay/handlers/command_executor.rs:638` — command events | YES |
| `get_workflow` | `relay/handlers/command_executor.rs:563,705,1133` — command events; `relay/api/bridge.rs:1790` — HTTP bridge; `workflow/executor.rs:546` — workflow executor | YES |
| `list_enabled_channel_workflows` | `workflow/lib.rs:303` — workflow engine | YES |
| `list_all_enabled_workflows` | `workflow/lib.rs:436` — workflow engine | YES |
| `claim_scheduled_workflow_fire` | `workflow/lib.rs:549` — workflow engine | YES |
| `latest_scheduled_workflow_fire` | `workflow/lib.rs:502` — workflow engine | YES |
| `attach_scheduled_workflow_run` | `workflow/lib.rs:619` — workflow engine | YES |
| `delete_workflow_for_owner` | `relay/handlers/side_effects.rs:1998,2017` — event side-effects | YES |
| `find_workflow_by_owner_and_name` | `relay/handlers/side_effects.rs:2011` — event side-effects | YES |
| `create_workflow_run` | `relay/handlers/command_executor.rs:757` — command events; `relay/api/bridge.rs:1859` — HTTP bridge; `workflow/lib.rs:346,592` — workflow engine | YES |
| `get_workflow_run` | `relay/handlers/command_executor.rs:1061,1116` — command events; `workflow/executor.rs:537,1034` — workflow executor | YES |
| `update_workflow_run` | `relay/handlers/command_executor.rs:782,1079,1147` — command events; `relay/api/bridge.rs:1874` — HTTP bridge; `workflow/lib.rs:201,220,244` — workflow engine; `workflow/executor.rs:984,1046` — workflow executor | YES |
| `get_approval_by_stored_hash` | `relay/handlers/command_executor.rs:881,992` — command events | YES |
| `update_approval_by_stored_hash` | `relay/handlers/command_executor.rs:922,1033` — command events | YES |
| `ensure_future_partitions` | `relay/main.rs:172` — startup/background | **NO** — partition maintenance (Postgres-specific DDL) |
| `backfill_d_tags` | `relay/main.rs:315` — startup/background. *startup backfill, always runs (idempotent)* | YES |
| `is_pubkey_allowed` | `relay/handlers/auth.rs:192` — NIP-42 auth | YES |
| `is_relay_member` | `relay/api/mod.rs:74,91` — agent-owner HTTP API | YES |
| `get_relay_member` | `relay/handlers/identity_archive.rs:241` — identity archive; `relay/handlers/moderation_authz.rs:98,110` — moderation authz; `relay/handlers/relay_admin.rs:135,317` — NIP-43 relay admin; `relay/api/invites.rs:241` — invite claim API | YES |
| `list_relay_members` | `admin/main.rs:263,345` — buzz-admin CLI | YES — buzz-admin CLI only |
| `add_relay_member` | `relay/handlers/relay_admin.rs:199` — NIP-43 relay admin; `admin/main.rs:176` — buzz-admin CLI | YES |
| `claim_relay_membership` | `relay/api/invites.rs:332` — invite claim API | YES |
| `remove_relay_member` | `relay/handlers/relay_admin.rs:250` — NIP-43 relay admin; `relay/handlers/ingest.rs:1858` — event ingest pipeline; `admin/main.rs:218` — buzz-admin CLI | YES |
| `remove_relay_member_if_role` | `relay/handlers/relay_admin.rs:243` — NIP-43 relay admin; `admin/main.rs:215` — buzz-admin CLI | YES |
| `update_relay_member_role` | `relay/handlers/relay_admin.rs:309` — NIP-43 relay admin | YES |
| `bootstrap_owner` | `relay/main.rs:293` — startup/background; `relay/handlers/community_provisioning.rs:330` — community provisioning. *startup owner bootstrap + provisioning API* | YES |
| `transfer_ownership` | `relay/api/operator.rs:402` — operator provisioning API | YES |
| `backfill_from_allowlist` | `relay/main.rs:270` — startup/background. *startup, only when deployment community resolved* | YES |
| `insert_product_feedback` | `relay/handlers/product_feedback.rs:43` — product feedback | YES |
| `list_product_feedback` | `admin/main.rs:255` — buzz-admin CLI | YES — buzz-admin CLI only |
| `insert_moderation_report` | `relay/handlers/report.rs:79` — NIP-56 reports | YES |
| `list_moderation_reports` | `relay/api/bridge.rs:2089` — HTTP bridge | YES |
| `get_moderation_report_by_event` | `relay/handlers/moderation_commands.rs:414` — moderation commands | YES |
| `resolve_moderation_report` | `relay/handlers/moderation_commands.rs:461` — moderation commands | YES |
| `ban_community_member` | `relay/handlers/moderation_commands.rs:169` — moderation commands | YES |
| `unban_community_member` | `relay/handlers/moderation_commands.rs:248` — moderation commands | YES |
| `timeout_community_member` | `relay/handlers/moderation_commands.rs:287` — moderation commands | YES |
| `untimeout_community_member` | `relay/handlers/moderation_commands.rs:351` — moderation commands | YES |
| `moderation_restriction_state` | `relay/handlers/moderation_commands.rs:105` — moderation commands; `relay/handlers/auth.rs:121,143` — NIP-42 auth; `relay/handlers/ingest.rs:1616` — event ingest pipeline | YES |
| `list_community_restrictions` | `relay/api/bridge.rs:2126` — HTTP bridge | YES |
| `insert_moderation_action` | `relay/handlers/moderation_commands.rs:529` — moderation commands | YES |
| `list_moderation_actions` | `relay/api/bridge.rs:2111` — HTTP bridge | YES |
| `repo_name_owner` | `relay/handlers/side_effects.rs:2456` — event side-effects | YES |
| `reserve_repo_name` | `relay/handlers/side_effects.rs:2483` — event side-effects | YES |
| `count_repos_for_owner` | `relay/handlers/side_effects.rs:2476` — event side-effects | YES |
| `release_repo_name` | `relay/handlers/side_effects.rs:2533` — event side-effects | YES |
| `archive` | `relay/handlers/identity_archive.rs:72` — identity archive | YES |
| `unarchive` | `relay/handlers/identity_archive.rs:86` — identity archive | YES |
| `list_archived` | `relay/handlers/side_effects.rs:2998` — event side-effects | YES |
| `soft_delete_discovery_events` | `relay/handlers/side_effects.rs:1792` — event side-effects | YES |
| `replace_addressable_event` | `relay/handlers/moderation_notices.rs:206` — moderation DM notices; `relay/handlers/ingest.rs:2367` — event ingest pipeline; `relay/handlers/side_effects.rs:931,3018` — event side-effects; `admin/main.rs:364,539,557,572` — buzz-admin CLI | YES |
| `nip43_membership_snapshot_needs_reconciliation` | `relay/handlers/side_effects.rs:2772` — event side-effects | YES |
| `publish_nip43_membership_locked` | `relay/handlers/side_effects.rs:2844` — event side-effects | YES |
| `replace_parameterized_event` | `relay/handlers/ingest.rs:2382` — event ingest pipeline; `relay/handlers/side_effects.rs:3105` — event side-effects | YES |


159 methods have at least one production call site.

---

## 2. Methods with ZERO production call sites

Candidates for exclusion or dead code — listed, not judged. None of these is
called from production code in buzz-relay, buzz-workflow, or buzz-admin.
(Some wrap submodule SQL that *is* still exercised internally by other facade
methods — noted inline.)


- **api_token**: `create_api_token`, `create_api_token_if_under_limit`, `get_api_token_by_hash_including_revoked`, `list_tokens_by_owner`, `revoke_all_tokens`, `revoke_token`, `update_token_last_used`
- **channel**: `create_dm`, `find_dm_by_participants`, `get_accessible_channels`, `get_bot_members`, `get_canvas`, `get_member_count`, `get_member_counts_bulk`, `get_members_bulk`, `set_canvas`
- **core (lib.rs)**: `begin_transaction`, `from_pools` (`read` also had zero production call sites; it has since been removed from `lib.rs` by the in-flight SQLite work on this branch)
- **dm**: `list_dms_for_user`
- **event**: `claim_due_reminder`, `get_last_message_at`, `get_last_message_at_bulk`, `soft_delete_event`
- **lib.rs (mentions helper)**: `insert_mentions` (*called internally by `insert_event`, `insert_event_with_thread_metadata`, `insert_reaction_event_with_thread_metadata`*)
- **lib.rs inline (community / allowlist / tokens)**: `add_to_allowlist`, `community_of_channel`, `get_api_token_by_hash`, `has_allowlist_entries`, `list_active_tokens`, `list_allowlist`, `remove_from_allowlist`, `touch_api_token`
- **moderation**: `get_community_ban`, `get_moderation_report`
- **push**: `enqueue_push_wake`
- **reaction**: `add_reaction` (*underlying `reaction::add_reaction_tx` is used inside `event::insert_reaction_event_with_thread_metadata`*), `get_active_reaction_record` (*underlying `reaction::get_active_reaction_record` is used inside `event.rs` reaction insert/remove paths*), `get_reactions`, `get_reactions_bulk`, `set_reaction_event_id`
- **thread**: `decrement_reply_count`, `insert_thread_metadata`
- **user**: `search_users`
- **workflow**: `create_approval`, `create_workflow`, `delete_workflow`, `get_approval`, `get_run_approvals`, `list_channel_workflows`, `list_workflow_runs`, `prune_scheduled_workflow_fires_before`, `set_workflow_enabled`, `update_approval`, `update_workflow`, `update_workflow_status`

Total: 56 methods (55 still present — `read` has since been removed).

**Test-only call sites** (production count zero, but called from `#[cfg(test)]`
code in the consumer crates): `from_pool` (10 test helpers across buzz-relay),
`has_join_policy_acceptance` (`relay/api/invites.rs:861`), `is_archived`
(`relay/handlers/identity_archive.rs:552`). `from_pool` is also how relay unit
tests construct a `Db`, so a SQLite `Db` will want an equivalent constructor for
its own tests even though production always goes through `Db::new`.

---

## 3. Solo serve-path method set

Deduplicated methods that need a working SQLite arm (every method marked YES in
§1), grouped by the buzz-db submodule the facade delegates to. Methods marked
*(admin CLI only)* are reached only via the `buzz-admin` operator CLI — they
need an arm only if `buzz-admin` is expected to run against a Solo relay's
database.


- **`admin_moderation`** (4): `admin_get_feedback`, `admin_get_report`, `admin_list_feedback`, `admin_list_reports`
- **`archived_identities`** (3): `archive`, `list_archived`, `unarchive`
- **`channel`** (19): `add_member`, `archive_channel`, `create_channel`, `create_channel_with_id`, `get_accessible_channel_ids`, `get_channel`, `get_member_role`, `get_members`, `get_users_bulk`, `is_member`, `list_channels`, `open_dm`, `reap_expired_ephemeral_channels`, `remove_member`, `set_purpose`, `set_topic`, `soft_delete_channel`, `unarchive_channel`, `update_channel`
- **`core (lib.rs)`** (3): `new`, `ping`, `pool_stats`
- **`dm`** (3): `hide_dm`, `list_hidden_dms`, `unhide_dm`
- **`event`** (20): `claim_due_reminder_with_stamp`, `count_events`, `get_event_by_id`, `get_event_by_id_including_deleted`, `get_events_by_ids`, `get_latest_global_replaceable *(admin CLI only)*`, `huddle_started_link_exists`, `insert_event`, `insert_event_with_thread_metadata`, `insert_reaction_event_with_thread_metadata`, `nip43_membership_snapshot_needs_reconciliation`, `persist_command_event`, `publish_nip43_membership_locked`, `query_due_reminders`, `query_events`, `release_due_reminder`, `replace_addressable_event`, `replace_parameterized_event`, `soft_delete_by_coordinate`, `soft_delete_event_and_update_thread`
- **`feed`** (3): `query_feed_activity`, `query_feed_mentions`, `query_feed_needs_action`
- **`git_repo`** (4): `count_repos_for_owner`, `release_repo_name`, `repo_name_owner`, `reserve_repo_name`
- **`lib.rs inline (community / allowlist / tokens)`** (14): `archive_community_owned_by`, `backfill_d_tags`, `communities_of_channels`, `ensure_configured_community`, `get_community_icon`, `is_community_active`, `is_pubkey_allowed`, `list_communities_owned_by`, `lookup_community_by_host`, `lookup_community_by_host_for_management`, `lookup_community_host`, `set_community_icon`, `soft_delete_discovery_events`, `unarchive_community_owned_by`
- **`migration`** (1): `migrate`
- **`moderation`** (12): `ban_community_member`, `get_moderation_report_by_event`, `insert_moderation_action`, `insert_moderation_report`, `list_community_restrictions`, `list_moderation_actions`, `list_moderation_reports`, `moderation_restriction_state`, `resolve_moderation_report`, `timeout_community_member`, `unban_community_member`, `untimeout_community_member`
- **`product_feedback`** (2): `insert_product_feedback`, `list_product_feedback *(admin CLI only)*`
- **`reaction`** (2): `remove_reaction`, `remove_reaction_by_source_event_id`
- **`relay_members`** (12): `add_relay_member`, `backfill_from_allowlist`, `bootstrap_owner`, `claim_relay_membership`, `create_community_with_owner`, `get_relay_member`, `is_relay_member`, `list_relay_members *(admin CLI only)*`, `remove_relay_member`, `remove_relay_member_if_role`, `transfer_ownership`, `update_relay_member_role`
- **`thread`** (4): `get_channel_window`, `get_thread_metadata_by_event`, `get_thread_replies`, `get_thread_summary`
- **`usage`** (1): `usage_community_hosts`
- **`user`** (8): `ensure_user`, `get_agent_channel_policy`, `get_user`, `get_user_by_nip05`, `is_agent_owner`, `set_agent_owner`, `set_channel_add_policy`, `update_user_profile`
- **`workflow`** (14): `attach_scheduled_workflow_run`, `claim_scheduled_workflow_fire`, `create_workflow_run`, `delete_workflow_for_owner`, `find_workflow_by_owner_and_name`, `get_approval_by_stored_hash`, `get_workflow`, `get_workflow_run`, `latest_scheduled_workflow_fire`, `list_all_enabled_workflows`, `list_enabled_channel_workflows`, `update_approval_by_stored_hash`, `update_workflow_run`, `upsert_workflow`

Total: 129 methods (of which 3 are admin-CLI-only).

Not in the set (all call sites gated off on Solo): the 14 push methods
(`claim_due_push_match_batch`, `active_push_match_leases`,
`complete_push_match_batch`, `retry_push_match_batch`,
`reap_exhausted_push_matches`, `enqueue_push_wakes`, `claim_due_push_wakes`,
`revalidate_push_wake`, `complete_push_wake`, `retry_push_wake`,
`fail_push_wake`, `disable_push_endpoint`, `membership_pairs`,
`accept_push_lease_event`), the fence/replica set (`spawn_fence_probe`,
`fence`, `has_read_pool`, `read_pool_stats`), `ensure_future_partitions`, and
the usage-poller set (§4) — though the poller runs by default and must be
either disabled on Solo or given no-op/supported arms.

---

## 4. Background tasks on Solo

Every task spawn in `relay/main.rs`, whether it runs in the Solo profile, and
the Db methods it hits. "Always on" = no config gate today; Solo must support
these methods or explicitly disable the loop.

### Startup sequence (before serving, always runs)

`main.rs:145-320`: `Db::new` (150) → `has_read_pool` (154, log only) →
`migrate` (163, gated `BUZZ_AUTO_MIGRATE`) → `ensure_future_partitions` (172,
Postgres partition DDL — needs a no-op arm or gate on Solo) →
`spawn_fence_probe` (185, replica fence — no-op without read pool) →
`ensure_configured_community` (246) → `backfill_from_allowlist` (270) →
`bootstrap_owner` (293) → `backfill_d_tags` (315). All but the partition/fence
pair are Solo-relevant.

### Spawned loops

| Task (main.rs) | Runs on Solo? | Db methods hit |
|---|---|---|
| Pub/sub transport driver (:349) | yes (in-process backend) | none |
| NIP-43 snapshot reconciler, startup + 60s loop (:501-537) | only if `require_relay_membership` | `usage_community_hosts`, `nip43_membership_snapshot_needs_reconciliation`, `publish_nip43_membership_locked` + event dispatch (`insert_event`) via `publish_nip43_membership_list` |
| Channel-event reconciler (:543) | only with `BUZZ_RECONCILE_CHANNELS` (dev/CI) | `lookup_community_by_host` (via `bind_deployment_community`), then `list_channels`, `query_events`, `replace_addressable_event` in `reconcile_channel_events` |
| Workflow cron loop (:593) | **always on** | via buzz-workflow: `list_all_enabled_workflows`, `latest_scheduled_workflow_fire`, `claim_scheduled_workflow_fire`, `create_workflow_run`, `attach_scheduled_workflow_run`, `update_workflow_run`; executor adds `get_workflow`, `get_workflow_run` |
| Ephemeral channel reaper, 60s (:607) | **always on** | `reap_expired_ephemeral_channels`; per expired channel, side-effects hit `insert_event`, `get_channel`, `get_members`, `replace_addressable_event` (system message + discovery events) |
| Push matcher + delivery worker (:680) | no — gated `push_gateway_delivery_url` | (push method set, §3) |
| NIP-ER reminder scheduler, 10s (:703) | **always on** | `query_due_reminders`, `claim_due_reminder_with_stamp`, `release_due_reminder` |
| Multi-node fan-out consumer (:817) | yes | none directly (fan-out uses AppState caches; misses fill via `is_member` / `get_accessible_channel_ids` / `get_channel` in `state.rs`) |
| Cache-invalidation consumer (:847) | yes | none |
| Community revalidator, 30s (:882) | **always on** | `is_community_active` (via `state.revalidate_live_communities`) |
| Connection-control consumer (:898) | yes | none |
| Pool metrics loop, 10s (:944) | **always on** | `pool_stats` (sync), `read_pool_stats` (None without replica), `fence()` (only inside the read-pool branch) |
| Usage metrics poller (:1004) | **always on — note!** | every tick: `usage_community_hosts`; leader path: `try_lock_usage_metrics` (pg advisory lock!), `UsageMetricsLeader::is_live`, then `usage_community_count`, `usage_user_counts`, `usage_channel_counts`, `usage_message_counts`, `usage_relay_member_counts`, `usage_workflow_counts`, `usage_git_repo_counts`, `usage_active_user_counts` (x3), `usage_active_channel_counts` (x2). Storage-sweep half is env-gated and touches no Db methods |

**Usage poller on Solo (the flagged one):** the loop itself has no config gate.
On SQLite either (a) give `try_lock_usage_metrics` a trivial always-leader arm
plus SQLite arms for the ten `usage_*` count queries and `usage_community_hosts`,
or (b) add a Solo gate that skips the leader-only half. Note
`usage_community_hosts` is *also* on the serve path (NIP-43 reconciliation),
so it needs a real arm regardless. `try_lock_usage_metrics` returns a
`UsageMetricsLeader` holding a dedicated Postgres session advisory lock — the
concept has no SQLite equivalent and needs a Solo-specific design.

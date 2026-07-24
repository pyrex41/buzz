//! Relay configuration from environment variables.

use std::net::SocketAddr;
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::warn;

/// Default maximum inbound WebSocket frame size in bytes.
///
/// Must comfortably exceed accepted event content sizes after Nostr JSON and
/// NIP-44 encryption overhead.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 512 * 1024;

/// Errors that can occur while loading relay configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The `BUZZ_BIND_ADDR` environment variable could not be parsed as a socket address.
    #[error("invalid BUZZ_BIND_ADDR: {0}")]
    InvalidBindAddr(String),
    /// A configuration value failed validation.
    #[error("invalid config: {0}")]
    InvalidValue(String),
}

/// Which storage engine backs `buzz-db`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbBackendKind {
    /// Postgres (the default; multi-pod capable, full feature set).
    Postgres,
    /// SQLite (single node, zero external services). Excludes the
    /// Postgres-only subsystems: push gateway, read replicas, partitions,
    /// usage-metrics leader election, audit, and Postgres FTS search.
    Sqlite,
}

/// Which messaging transport the relay runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessagingBackend {
    /// Redis pub/sub (multi-pod capable; the default).
    Redis,
    /// In-process fan-out (single node, zero external services). Rejected
    /// when the mesh is enabled.
    InProcess,
    /// ZeroMQ static-mesh transport (`BUZZ_ZMQ_BIND` + `BUZZ_ZMQ_PEERS`).
    /// Experimental; multi-node ZMQ requires `BUZZ_STATE_BACKEND=redis`.
    Zmq,
}

/// Which shared-state backend (presence, rate limits, NIP-98 replay) the
/// relay runs on. Defaults follow the messaging backend; with ZMQ peers
/// configured the choice must be explicit and shared (Redis).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateBackend {
    /// Redis TTL-KV + atomic counters (multi-pod capable).
    Redis,
    /// In-process store — single node only; the replay guard and rate
    /// limiter are correctness fences that must be shared across pods.
    InProcess,
}

/// Optional relay subsystems, switched on or off as a whole (Hive plan §5.3).
///
/// A capability is a subsystem the relay can run entirely without: no state
/// constructed, no routes mounted, no background queries, and its event kinds
/// refused at ingest with a machine-readable reason. This is one uniform block
/// rather than a scatter of per-subsystem toggles so that "what does this
/// deployment actually run?" has a single answer.
///
/// Every capability defaults **on** so an existing deployment upgrading with
/// untouched environment behaves identically — except where the zero-service
/// `solo` profile needs otherwise (see [`Capabilities::from_env`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Git-on-object-storage: `git_router` + `git_policy_router`, the
    /// [`crate::api::git::store::GitStore`] / pack cache on `AppState`, the
    /// NIP-34 kind family at ingest, the A3 conformance probe, the git web
    /// GUI fallback, and the `buzz_*_git_repos` usage gauges.
    ///
    /// `BUZZ_CAPABILITY_GIT`. Defaults **on**, except under
    /// `BUZZ_PROFILE=solo` where it defaults **off**: the git object store is
    /// S3-only today, so a zero-service boot has nothing to back it.
    pub git: bool,

    /// Huddle (voice) audio relaying — the in-process `AudioRoomManager` that
    /// forwards audio frames between peers in the same huddle.
    ///
    /// `BUZZ_CAPABILITY_HUDDLE_AUDIO`, with the older
    /// `BUZZ_HUDDLE_AUDIO_AVAILABLE` still honoured as an alias.
    ///
    /// Audio frames are relayed peer-to-peer *within a single pod* (only
    /// huddle lifecycle events cross pods via Redis). Under horizontal
    /// scaling two peers in the same huddle can land on different pods and
    /// never hear each other, so a multi-pod deployment turns this off and
    /// the relay surfaces a clear, client-handleable "huddle audio
    /// unavailable" signal on join instead of a silent split room.
    ///
    /// Defaults **on** so single-pod (N=1) deployments keep today's behavior.
    pub huddle_audio: bool,
}

impl Capabilities {
    /// Reads the `[capabilities]` block from the environment.
    ///
    /// `solo_profile` is the already-resolved `BUZZ_PROFILE=solo` decision; it
    /// only moves *defaults*, never overrides an explicit variable.
    ///
    /// Precedence for huddle audio, highest first:
    /// 1. `BUZZ_CAPABILITY_HUDDLE_AUDIO` — the canonical variable.
    /// 2. `BUZZ_HUDDLE_AUDIO_AVAILABLE` — legacy alias, retained for
    ///    deployments (and Helm charts) that already set it. Parsed with its
    ///    original lenient semantics (`false`/`0` disable, anything else
    ///    enables) so no existing deployment changes behavior.
    /// 3. Default `true`.
    fn from_env(solo_profile: bool) -> Result<Self, ConfigError> {
        let git = parse_capability_flag("BUZZ_CAPABILITY_GIT", !solo_profile)?;

        // Legacy alias resolves the default the canonical variable falls back
        // to, so setting only the old variable keeps working unchanged.
        let huddle_audio_default = std::env::var("BUZZ_HUDDLE_AUDIO_AVAILABLE")
            .map(|v| !(v == "false" || v == "0"))
            .unwrap_or(true);
        let huddle_audio =
            parse_capability_flag("BUZZ_CAPABILITY_HUDDLE_AUDIO", huddle_audio_default)?;

        Ok(Self { git, huddle_audio })
    }
}

/// Parses a capability boolean, rejecting values that are neither truthy nor
/// falsy rather than silently defaulting.
///
/// Silent coercion is how `BUZZ_CAPABILITY_GIT=flase` becomes a relay that
/// serves git when the operator meant to turn it off; capabilities decide
/// which subsystems exist, so a typo must fail the boot loudly.
fn parse_capability_flag(var: &str, default: bool) -> Result<bool, ConfigError> {
    match std::env::var(var) {
        Err(_) => Ok(default),
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(default),
            "true" | "1" | "on" | "yes" => Ok(true),
            "false" | "0" | "off" | "no" => Ok(false),
            other => Err(ConfigError::InvalidValue(format!(
                "{var} must be a boolean ('true'/'false', '1'/'0', 'on'/'off', 'yes'/'no'), got '{other}'"
            ))),
        },
    }
}

/// Deny-by-default read-only deployment-admin configuration.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Exact admin HTTP authority.
    pub host: String,
    /// Optional admin SPA bundle directory.
    pub web_dir: Option<std::path::PathBuf>,
}

/// Relay-hosted policy content presented on join surfaces.
#[derive(Debug, Clone)]
pub struct JoinPolicyConfig {
    /// Operator-provided Terms of Service document in Markdown.
    pub terms_markdown: Option<String>,
    /// Operator-provided Privacy Policy document in Markdown.
    pub privacy_markdown: Option<String>,
    /// Whether join surfaces must collect an 18+ attestation.
    pub age_attestation_required: bool,
    /// Content-derived identifier binding receipts to the exact policy revision.
    pub version: String,
}

/// Relay runtime configuration, loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the relay HTTP/WebSocket server binds to.
    pub bind_addr: SocketAddr,
    /// Postgres database connection URL.
    pub database_url: String,
    /// Optional read-replica connection URL (e.g. an Aurora `cluster-ro-`
    /// endpoint). Unset means all reads stay on the writer.
    pub read_database_url: Option<String>,
    /// Redis connection URL used by the pub/sub manager.
    pub redis_url: String,
    /// Whether the zero-service solo profile selected the backend defaults
    /// (`BUZZ_PROFILE=solo` / `--profile solo`). Explicit `BUZZ_*` variables
    /// always override individual defaults; this flag only records the
    /// profile choice so boot-time gates (e.g. the S3 git conformance
    /// probe) can pick solo-appropriate defaults too.
    pub solo_profile: bool,
    /// Storage engine selection (`BUZZ_DB_BACKEND`: "postgres" | "sqlite").
    pub db_backend: DbBackendKind,
    /// SQLite database path (`BUZZ_SQLITE_PATH`), sqlite backend only.
    pub sqlite_path: String,
    /// Messaging + shared-state backend selection (`BUZZ_MESSAGING_BACKEND`).
    ///
    /// `redis` (default) keeps today's behavior; `inproc` runs fan-out
    /// in-process (single node, ADR 0001); `zmq` uses the static-mesh
    /// ZeroMQ transport. Non-Redis backends are mutually exclusive with
    /// `BUZZ_MESH=on`.
    pub messaging_backend: MessagingBackend,
    /// Shared-state backend (`BUZZ_STATE_BACKEND`): presence, rate limits,
    /// and NIP-98 replay. Defaults follow `messaging_backend`.
    pub state_backend: StateBackend,
    /// ZMQ PUB bind endpoint (`BUZZ_ZMQ_BIND`, default `tcp://0.0.0.0:5559`).
    pub zmq_bind: String,
    /// ZMQ peer PUB endpoints (`BUZZ_ZMQ_PEERS`, comma-separated; must not
    /// include this node's own endpoint).
    pub zmq_peers: Vec<String>,
    /// Public WebSocket URL of this relay, advertised in NIP-11.
    pub relay_url: String,
    /// Public WebSocket URL of the dedicated device-pairing relay, when configured.
    pub pairing_relay_url: Option<String>,
    /// Maximum number of concurrent WebSocket connections.
    pub max_connections: usize,
    /// Maximum number of concurrently executing message handlers.
    pub max_concurrent_handlers: usize,
    /// Per-connection outbound message buffer size (number of messages).
    pub send_buffer_size: usize,
    /// Maximum inbound WebSocket frame size in bytes.
    pub max_frame_bytes: usize,
    /// Number of consecutive buffer-full events tolerated before cancelling a slow client.
    pub slow_client_grace_limit: u8,
    /// Authentication provider configuration.
    pub auth: buzz_auth::AuthConfig,
    /// Whether REST API requests must present a valid token. Independent of
    /// WebSocket protocol auth, which is *always* required by REQ/EVENT/COUNT.
    pub require_auth_token: bool,
    /// Comma-separated list of allowed CORS origins.
    /// If empty, permissive CORS is used (dev mode).
    /// Example: "tauri://localhost,http://localhost:3000"
    pub cors_origins: Vec<String>,
    /// Optional hex-encoded private key for the relay's signing keypair.
    /// If absent, a fresh keypair is generated at startup.
    pub relay_private_key: Option<String>,
    /// Optional Unix Domain Socket path. When set, the relay also listens on this
    /// UDS for traffic (e.g. service mesh sidecar). Health probes still use TCP.
    pub uds_path: Option<String>,
    /// TCP port for the health-only router (`/_liveness`, `/_readiness`, `/_status`).
    /// Separate from the app router so K8s probes bypass Istio and auth middleware.
    pub health_port: u16,
    /// TCP port for the Prometheus metrics exporter (`GET /metrics`).
    pub metrics_port: u16,

    /// When true, NIP-42 pubkey-only authentication (no API token) is
    /// restricted to pubkeys in the `pubkey_allowlist` table. Users with valid
    /// API tokens bypass the allowlist entirely.
    /// Applies to all NIP-42 pubkey-only connections, regardless of `require_auth_token`.
    pub pubkey_allowlist_enabled: bool,

    /// When true, every authenticated request must also pass a relay-level
    /// membership check against the `relay_members` table.
    /// When false (default), the check is a no-op and all authenticated callers
    /// are permitted regardless of auth method (API token, NIP-42).
    pub require_relay_membership: bool,

    /// Optional subsystems this deployment runs (`BUZZ_CAPABILITY_*`).
    ///
    /// A capability that is off constructs no state, mounts no routes, runs no
    /// background work, and refuses its event kinds at ingest. See
    /// [`Capabilities`] for the per-capability contract and defaults.
    pub capabilities: Capabilities,

    /// Inter-relay mesh configuration (`BUZZ_MESH`, `BUZZ_MESH_BIND_ADDR`).
    /// Opt-in: mesh forms only when `BUZZ_MESH=on` is explicit. The default
    /// (absent/off) is exact single-instance behavior — no bind, no Redis
    /// registry write — so an image upgrade with untouched env is a strict
    /// no-regression rollout.
    pub mesh: buzz_relay_mesh::MeshConfig,

    /// Testbed-only reliable-stream echo consumer (`BUZZ_MESH_DEMO_ECHO`).
    /// When `on`, the owner side of an inbound reliable mesh stream echoes
    /// every validated `Data` frame back to the sender — a transport/
    /// session-routing smoke for cross-pod evidence runs, NOT a product flow.
    /// Same strict opt-in as `BUZZ_MESH`; default off means inbound reliable
    /// streams are accepted, logged, and closed (no session consumer yet).
    pub mesh_demo_echo: bool,

    /// Optional hex-encoded pubkey of the relay owner.
    /// When set, this pubkey is automatically bootstrapped into `relay_members`
    /// with the `owner` role on first startup.
    pub relay_owner_pubkey: Option<String>,

    /// Canonical HTTP origin of the deployment-global operator API.
    ///
    /// Every operator NIP-98 `u` tag is verified against this origin, independent
    /// of the inbound HTTP `Host` header and tenant registry. Required when
    /// `RELAY_OPERATOR_PUBKEYS` is non-empty. Set via `RELAY_OPERATOR_API_ORIGIN`
    /// as an `http://` or `https://` origin with no path, query, or fragment.
    pub relay_operator_api_origin: Option<String>,

    /// Deployment-level relay operator pubkeys allowed to use the
    /// `/operator/communities` management endpoints.
    ///
    /// Unlike `relay_owner_pubkey` (a role *within* the deployment community),
    /// operators span tenants: they may create new communities and bootstrap
    /// initial owners, but hold no implicit tenant membership row.
    /// Empty (the default) disables community provisioning entirely — fail closed.
    ///
    /// Set via `RELAY_OPERATOR_PUBKEYS` as a comma-separated list of 64-char
    /// hex pubkeys. Invalid entries are rejected at startup (config error), not
    /// skipped — a typo must not silently disable an operator.
    pub relay_operator_pubkeys: Vec<String>,

    /// Allow NIP-OA owner attestation for relay membership.
    ///
    /// When `true` and `require_relay_membership` is also `true`, agents
    /// bearing a valid NIP-OA `auth` tag can authenticate by proving their
    /// owner is a relay member. The agent gets session-scoped access.
    ///
    /// On open relays (`require_relay_membership = false`), NIP-OA owner
    /// extraction for agent→owner backfill happens unconditionally (the
    /// signature is cryptographically self-proving). This flag only controls
    /// whether NIP-OA can grant membership access on closed relays.
    ///
    /// Default: `false`. Set via `BUZZ_ALLOW_NIP_OA_AUTH=true`.
    pub allow_nip_oa_auth: bool,

    /// Media storage configuration (S3/MinIO).
    pub media: buzz_media::MediaConfig,
    /// Maximum concurrent media uploads handled by one relay process.
    pub media_max_concurrent_uploads: usize,
    /// Maximum concurrent media uploads accepted from one pubkey.
    pub media_max_concurrent_uploads_per_pubkey: u32,
    /// Maximum media upload starts accepted from one pubkey per minute.
    pub media_uploads_per_minute: u32,

    /// Require Blossom kind:24242 `t=get` auth plus relay membership before
    /// serving media GET/HEAD. Default off for staged client rollout.
    pub require_media_get_auth: bool,

    /// Whether tamper-evident event/media audit logging is enabled. Defaults to true.
    /// This does not control the separate `moderation_actions` audit trail.
    /// Set `BUZZ_AUDIT_ENABLED=false` for deployments that do not require it.
    pub audit_enabled: bool,

    /// Optional override for ephemeral channel TTL (in seconds).
    /// When set, any channel created with a TTL tag will use this value instead
    /// of the client-provided one. Useful for testing ephemeral expiry quickly.
    /// Example: `BUZZ_EPHEMERAL_TTL_OVERRIDE=60` → all ephemeral channels expire
    /// 60 seconds after the last message.
    pub ephemeral_ttl_override: Option<i32>,

    /// Root directory for the relay's local git scratch. No authoritative
    /// repository state lives here — runtime reads/writes hydrate ephemeral
    /// repos from object storage per request. Temporary workspaces, buffered
    /// subprocess output, and the disposable immutable pack cache live below
    /// this path.
    /// Repo-name uniqueness lives in Postgres (`git_repo_names`), not on disk,
    /// so this directory need not be persistent or shared across replicas.
    pub git_repo_path: std::path::PathBuf,
    /// Parent directory for process-isolated immutable pack cache sessions.
    pub git_pack_cache_path: std::path::PathBuf,
    /// Maximum pack file size for git push (bytes). Default: 500 MB.
    pub git_max_pack_bytes: u64,
    /// Maximum total bytes materialized for one git repo request. Default: 1 GB.
    ///
    /// This bounds clone/fetch hydration work across a repo's historical pack
    /// set rather than only bounding one incoming push body.
    pub git_max_repo_bytes: u64,
    /// Maximum bytes retained in the process-local immutable pack/index cache.
    /// Zero disables retention while preserving request-local hydration.
    pub git_pack_cache_max_bytes: u64,
    /// Maximum pack digests populated concurrently in one relay process.
    pub git_pack_cache_max_concurrent_populations: usize,
    /// Maximum number of repos per pubkey. Default: 100.
    pub git_max_repos_per_pubkey: u32,
    /// Maximum concurrent git subprocess operations. Default: 20.
    pub git_max_concurrent_ops: usize,
    /// HMAC secret for git pre-receive hook callbacks.
    /// Used to authenticate internal policy endpoint requests.
    pub git_hook_hmac_secret: String,

    /// Descriptor key identifier accepted in kind:30350 `exec` tags.
    pub push_executor_key_id: String,
    /// Exact HTTPS gateway endpoint used to submit client-authorized APNs delivery capabilities.
    /// Push lease support is disabled when unset.
    pub push_gateway_delivery_url: Option<url::Url>,
    /// Hard timeout for one gateway delivery request.
    pub push_gateway_timeout: Duration,

    /// Optional relay-hosted policy shown on join surfaces. Disabled when no
    /// documents or age attestation are configured.
    pub join_policy: Option<JoinPolicyConfig>,

    /// Deployment-admin API and SPA configuration. Absent means the surface is disabled.
    pub admin: Option<AdminConfig>,

    /// Optional path to the web UI `dist/` directory.
    /// When set, the relay serves the invite landing page and its static assets.
    /// When unset, no static file serving happens (relay behaves as before).
    pub web_dir: Option<std::path::PathBuf>,
    /// Whether the configured web bundle serves Git browser routes in addition
    /// to the public invite landing page. Defaults to false.
    pub serve_git_web_gui: bool,
}

fn parse_bind_addr(raw: &str) -> Result<SocketAddr, ConfigError> {
    raw.parse::<SocketAddr>()
        .map_err(|e| ConfigError::InvalidBindAddr(e.to_string()))
}

fn positive_u64_from_env(name: &str, default: u64) -> Result<u64, ConfigError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| ConfigError::InvalidValue(format!("{name} must be a positive integer"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidValue(format!(
            "{name} must be valid Unicode"
        ))),
    }
}

fn rate_limit_config_from_env() -> Result<buzz_auth::RateLimitConfig, ConfigError> {
    let defaults = buzz_auth::RateLimitConfig::default();
    Ok(buzz_auth::RateLimitConfig {
        human_messages_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_HUMAN_MESSAGES_PER_MIN",
            defaults.human_messages_per_min,
        )?,
        human_api_calls_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_HUMAN_API_CALLS_PER_MIN",
            defaults.human_api_calls_per_min,
        )?,
        human_ws_events_per_sec: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC",
            defaults.human_ws_events_per_sec,
        )?,
        agent_standard_messages_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_AGENT_STANDARD_MESSAGES_PER_MIN",
            defaults.agent_standard_messages_per_min,
        )?,
        agent_standard_api_calls_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_AGENT_STANDARD_API_CALLS_PER_MIN",
            defaults.agent_standard_api_calls_per_min,
        )?,
        agent_elevated_messages_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_AGENT_ELEVATED_MESSAGES_PER_MIN",
            defaults.agent_elevated_messages_per_min,
        )?,
        agent_platform_messages_per_min: positive_u64_from_env(
            "BUZZ_RATE_LIMIT_AGENT_PLATFORM_MESSAGES_PER_MIN",
            defaults.agent_platform_messages_per_min,
        )?,
    })
}

fn parse_operator_api_origin(raw: &str) -> Result<String, ConfigError> {
    let raw = raw.trim();
    let url = url::Url::parse(raw).map_err(|e| {
        ConfigError::InvalidValue(format!("RELAY_OPERATOR_API_ORIGIN is not a valid URL: {e}"))
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidValue(
            "RELAY_OPERATOR_API_ORIGIN must be an http(s) origin with no credentials, path, query, or fragment"
                .to_string(),
        ));
    }
    Ok(raw.trim_end_matches('/').to_string())
}

const DEFAULT_PUSH_GATEWAY_DELIVERY_URL: &str = "https://push.buzz.xyz/v1/deliveries/apns";

fn parse_push_gateway_delivery_url(raw: &str) -> Result<url::Url, ConfigError> {
    let url = url::Url::parse(raw.trim()).map_err(|e| {
        ConfigError::InvalidValue(format!(
            "BUZZ_PUSH_GATEWAY_DELIVERY_URL is not a valid URL: {e}"
        ))
    })?;
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/v1/deliveries/apns"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidValue(
            "BUZZ_PUSH_GATEWAY_DELIVERY_URL must be an exact HTTPS /v1/deliveries/apns URL without credentials, query, or fragment"
                .to_string(),
        ));
    }
    Ok(url)
}

fn parse_bool(name: &str, default: bool) -> Result<bool, ConfigError> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ConfigError::InvalidValue(format!(
            "{name} must be valid UTF-8: {error}"
        ))),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "on" => Ok(true),
            "false" | "0" | "off" | "" => Ok(false),
            _ => Err(ConfigError::InvalidValue(format!(
                "{name} must be true or false"
            ))),
        },
    }
}

fn parse_optional_bool(name: &str) -> Result<bool, ConfigError> {
    parse_bool(name, false)
}

fn ensure_git_repo_path(
    raw: impl Into<std::path::PathBuf>,
) -> Result<std::path::PathBuf, ConfigError> {
    ensure_git_path("BUZZ_GIT_REPO_PATH", raw)
}

fn ensure_git_path(
    setting: &str,
    raw: impl Into<std::path::PathBuf>,
) -> Result<std::path::PathBuf, ConfigError> {
    let git_repo_path = raw.into();
    if let Err(e) = std::fs::create_dir_all(&git_repo_path) {
        return Err(ConfigError::InvalidValue(format!(
            "{setting}={} could not be created: {e}",
            git_repo_path.display()
        )));
    }
    Ok(git_repo_path)
}

impl Config {
    /// Loads configuration from environment variables, falling back to development defaults.
    pub fn from_env() -> Result<Self, ConfigError> {
        // Profile selects the DEFAULT backend trio; every explicit BUZZ_*
        // variable still wins. `solo` is the zero-service profile from the
        // Hive plan (§8 Phase 2.5): SQLite storage, in-process messaging,
        // local-filesystem media — one binary, no Postgres/Redis/S3, data
        // under ./data. The relay binary also accepts `--profile solo`,
        // which main() maps onto this variable before config load.
        let solo_profile = match std::env::var("BUZZ_PROFILE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "" | "default" | "postgres" => false,
            "solo" => true,
            other => {
                return Err(ConfigError::InvalidValue(format!(
                    "BUZZ_PROFILE must be 'solo' or unset, got '{other}'"
                )))
            }
        };
        let bind_addr_raw =
            std::env::var("BUZZ_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
        let bind_addr = parse_bind_addr(&bind_addr_raw)?;

        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://buzz:buzz_dev@localhost:5432/buzz".to_string()); // sadscan:disable np.postgres.1

        let read_database_url = std::env::var("READ_DATABASE_URL")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let db_backend = match std::env::var("BUZZ_DB_BACKEND")
            .unwrap_or_else(|_| {
                if solo_profile {
                    "sqlite".to_string()
                } else {
                    "postgres".to_string()
                }
            })
            .to_ascii_lowercase()
            .as_str()
        {
            "postgres" | "pg" => DbBackendKind::Postgres,
            "sqlite" => DbBackendKind::Sqlite,
            other => {
                return Err(ConfigError::InvalidValue(format!(
                    "BUZZ_DB_BACKEND must be 'postgres' or 'sqlite', got '{other}'"
                )))
            }
        };
        let sqlite_path =
            std::env::var("BUZZ_SQLITE_PATH").unwrap_or_else(|_| "./data/buzz.db".to_string());
        if db_backend == DbBackendKind::Sqlite && read_database_url.is_some() {
            return Err(ConfigError::InvalidValue(
                "READ_DATABASE_URL is Postgres-only: unset it with BUZZ_DB_BACKEND=sqlite"
                    .to_string(),
            ));
        }

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

        let relay_url =
            std::env::var("RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".to_string());

        let pairing_relay_url = std::env::var("BUZZ_PAIRING_RELAY_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(|value| {
                let parsed = url::Url::parse(&value).map_err(|e| {
                    ConfigError::InvalidValue(format!(
                        "BUZZ_PAIRING_RELAY_URL must be a valid ws:// or wss:// URL: {e}"
                    ))
                })?;
                if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
                    return Err(ConfigError::InvalidValue(
                        "BUZZ_PAIRING_RELAY_URL must be a valid ws:// or wss:// URL".to_string(),
                    ));
                }
                Ok(value)
            })
            .transpose()?;

        let max_connections = std::env::var("BUZZ_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        let max_concurrent_handlers = std::env::var("BUZZ_MAX_CONCURRENT_HANDLERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);

        let send_buffer_size = std::env::var("BUZZ_SEND_BUFFER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000);

        let max_frame_bytes = std::env::var("BUZZ_MAX_FRAME_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);

        let slow_client_grace_limit = std::env::var("BUZZ_SLOW_CLIENT_GRACE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15);

        let require_auth_token = std::env::var("BUZZ_REQUIRE_AUTH_TOKEN")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let pubkey_allowlist_enabled = std::env::var("BUZZ_PUBKEY_ALLOWLIST")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let require_relay_membership = std::env::var("BUZZ_REQUIRE_RELAY_MEMBERSHIP")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        // Uniform `[capabilities]` block. `git` defaults off under the solo
        // profile (S3-only object store); `huddle_audio` defaults on so
        // single-pod (N=1) keeps today's behavior. See `Capabilities`.
        let capabilities = Capabilities::from_env(solo_profile)?;

        // Mesh opt-in: default OFF. Strict rollout no-regression — an image
        // upgrade with untouched env must not bind a new UDP port or write a
        // new Redis key. Horizontally-scaled deployments explicitly set
        // `BUZZ_MESH=on`; anything else (absent, `off`, other values) keeps
        // exact single-instance behavior.
        let mesh_enabled = std::env::var("BUZZ_MESH")
            .map(|v| v.eq_ignore_ascii_case("on") || v == "true" || v == "1")
            .unwrap_or(false);
        let mesh_bind_addr = std::env::var("BUZZ_MESH_BIND_ADDR")
            .map(|raw| {
                raw.parse::<SocketAddr>().map_err(|e| {
                    ConfigError::InvalidValue(format!("invalid BUZZ_MESH_BIND_ADDR: {e}"))
                })
            })
            .unwrap_or_else(|_| Ok("0.0.0.0:3478".parse().expect("static default parses")))?;
        let mesh = buzz_relay_mesh::MeshConfig {
            enabled: mesh_enabled,
            bind_addr: mesh_bind_addr,
            registry_refresh: std::time::Duration::from_secs(15),
        };

        // Backend selection. In-process is single-node by definition, and the
        // mesh's session fencing + cross-pod correctness fences require shared
        // (Redis) state — reject the combination instead of silently weakening
        // replay/rate-limit guarantees.
        let messaging_backend = match std::env::var("BUZZ_MESSAGING_BACKEND")
            .unwrap_or_else(|_| {
                if solo_profile {
                    "inproc".to_string()
                } else {
                    "redis".to_string()
                }
            })
            .to_ascii_lowercase()
            .as_str()
        {
            "redis" => MessagingBackend::Redis,
            "inproc" | "in-process" | "inprocess" => MessagingBackend::InProcess,
            "zmq" | "zeromq" => MessagingBackend::Zmq,
            other => {
                return Err(ConfigError::InvalidValue(format!(
                    "BUZZ_MESSAGING_BACKEND must be 'redis', 'inproc', or 'zmq', got '{other}'"
                )))
            }
        };
        if db_backend == DbBackendKind::Sqlite && mesh.enabled {
            return Err(ConfigError::InvalidValue(
                "BUZZ_MESH=on requires BUZZ_DB_BACKEND=postgres: the mesh is multi-node \
                 and SQLite is single-node storage"
                    .to_string(),
            ));
        }
        if messaging_backend != MessagingBackend::Redis && mesh.enabled {
            return Err(ConfigError::InvalidValue(
                "BUZZ_MESH=on requires BUZZ_MESSAGING_BACKEND=redis: mesh session fencing \
                 and cross-pod replay/rate-limit state need a shared Redis"
                    .to_string(),
            ));
        }

        let zmq_bind =
            std::env::var("BUZZ_ZMQ_BIND").unwrap_or_else(|_| "tcp://0.0.0.0:5559".to_string());
        let zmq_peers: Vec<String> = std::env::var("BUZZ_ZMQ_PEERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        // Shared-state backend. Defaults follow the messaging backend; a
        // multi-node ZMQ mesh must state its choice explicitly because the
        // replay guard and rate limiter are cross-pod correctness fences.
        let state_backend = match std::env::var("BUZZ_STATE_BACKEND") {
            Ok(raw) => match raw.to_ascii_lowercase().as_str() {
                "redis" => StateBackend::Redis,
                "inproc" | "in-process" | "inprocess" => StateBackend::InProcess,
                other => {
                    return Err(ConfigError::InvalidValue(format!(
                        "BUZZ_STATE_BACKEND must be 'redis' or 'inproc', got '{other}'"
                    )))
                }
            },
            Err(_) => match messaging_backend {
                MessagingBackend::Redis => StateBackend::Redis,
                MessagingBackend::InProcess => StateBackend::InProcess,
                MessagingBackend::Zmq if zmq_peers.is_empty() => StateBackend::InProcess,
                MessagingBackend::Zmq => {
                    return Err(ConfigError::InvalidValue(
                        "BUZZ_ZMQ_PEERS is set: choose BUZZ_STATE_BACKEND explicitly \
                         (multi-node replay/rate-limit fences need shared state — \
                         'redis' is the safe choice)"
                            .to_string(),
                    ))
                }
            },
        };
        if state_backend == StateBackend::InProcess && !zmq_peers.is_empty() {
            return Err(ConfigError::InvalidValue(
                "BUZZ_STATE_BACKEND=inproc is single-node only: with BUZZ_ZMQ_PEERS \
                 configured, replay protection and rate limits would not be shared \
                 across nodes — use BUZZ_STATE_BACKEND=redis"
                    .to_string(),
            ));
        }

        // Demo echo opt-in: same strict pattern as BUZZ_MESH — explicit
        // `on`/`true`/`1` only, anything else (absent, `off`, typos) is off.
        let mesh_demo_echo = std::env::var("BUZZ_MESH_DEMO_ECHO")
            .map(|v| v.eq_ignore_ascii_case("on") || v == "true" || v == "1")
            .unwrap_or(false);

        let allow_nip_oa_auth = std::env::var("BUZZ_ALLOW_NIP_OA_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        // Note: intentionally not prefixed with BUZZ_ — this is a relay-identity
        // config that may be shared across multiple services (e.g., ACP agent).
        let relay_owner_pubkey = std::env::var("RELAY_OWNER_PUBKEY")
            .ok()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .and_then(|s| {
                // Must be exactly 64 lowercase hex characters (32-byte pubkey).
                let valid = s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit());
                if valid {
                    Some(s)
                } else {
                    warn!(
                        "RELAY_OWNER_PUBKEY is not a valid 64-char hex pubkey — ignoring. \
                         Got: {s:?}"
                    );
                    None
                }
            });

        // Note: intentionally not prefixed with BUZZ_ — same relay-identity
        // config family as RELAY_OWNER_PUBKEY. Comma-separated 64-char hex
        // pubkeys. Unlike RELAY_OWNER_PUBKEY (warn-and-ignore), an invalid
        // entry here is a hard config error: silently dropping an operator
        // pubkey would silently disable provisioning for that operator.
        let relay_operator_api_origin = std::env::var("RELAY_OPERATOR_API_ORIGIN")
            .ok()
            .filter(|raw| !raw.trim().is_empty())
            .map(|raw| parse_operator_api_origin(&raw))
            .transpose()?;

        let relay_operator_pubkeys = match std::env::var("RELAY_OPERATOR_PUBKEYS") {
            Ok(raw) => {
                let mut pubkeys = Vec::new();
                for entry in raw.split(',') {
                    let entry = entry.trim().to_lowercase();
                    if entry.is_empty() {
                        continue;
                    }
                    let valid = entry.len() == 64 && entry.chars().all(|c| c.is_ascii_hexdigit());
                    if !valid {
                        return Err(ConfigError::InvalidValue(format!(
                            "RELAY_OPERATOR_PUBKEYS entry is not a valid 64-char hex pubkey: {entry:?}"
                        )));
                    }
                    if !pubkeys.contains(&entry) {
                        pubkeys.push(entry);
                    }
                }
                pubkeys
            }
            Err(_) => Vec::new(),
        };
        if !relay_operator_pubkeys.is_empty() && relay_operator_api_origin.is_none() {
            return Err(ConfigError::InvalidValue(
                "RELAY_OPERATOR_API_ORIGIN is required when RELAY_OPERATOR_PUBKEYS is configured"
                    .to_string(),
            ));
        }

        let auth = buzz_auth::AuthConfig {
            rate_limits: rate_limit_config_from_env()?,
        };

        if !require_auth_token {
            warn!(
                "BUZZ_REQUIRE_AUTH_TOKEN is false — REST API requests bypass token auth. \
                 WebSocket protocol auth is unaffected. Set to true for production."
            );
        }

        let cors_origins = std::env::var("BUZZ_CORS_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let relay_private_key = std::env::var("BUZZ_RELAY_PRIVATE_KEY").ok();

        let uds_path = std::env::var("BUZZ_UDS_PATH")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let health_port = std::env::var("BUZZ_HEALTH_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8080);

        let metrics_port = std::env::var("BUZZ_METRICS_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(9102);

        let media_backend = match std::env::var("BUZZ_MEDIA_BACKEND")
            .unwrap_or_else(|_| {
                if solo_profile {
                    "local".to_string()
                } else {
                    "s3".to_string()
                }
            })
            .to_ascii_lowercase()
            .as_str()
        {
            "s3" => buzz_media::config::MediaBackendKind::S3,
            "local" | "fs" | "localfs" => buzz_media::config::MediaBackendKind::Local,
            other => {
                return Err(ConfigError::InvalidValue(format!(
                    "BUZZ_MEDIA_BACKEND must be 's3' or 'local', got '{other}'"
                )))
            }
        };
        let media = buzz_media::MediaConfig {
            backend: media_backend,
            local_path: std::env::var("BUZZ_MEDIA_PATH")
                .unwrap_or_else(|_| "./data/media".to_string()),
            s3_endpoint: std::env::var("BUZZ_S3_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            s3_access_key: std::env::var("BUZZ_S3_ACCESS_KEY")
                .unwrap_or_else(|_| "buzz_dev".to_string()),
            s3_secret_key: std::env::var("BUZZ_S3_SECRET_KEY")
                .unwrap_or_else(|_| "buzz_dev_secret".to_string()),
            s3_bucket: std::env::var("BUZZ_S3_BUCKET").unwrap_or_else(|_| "buzz-media".to_string()),
            s3_region: std::env::var("BUZZ_S3_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string()),
            max_image_bytes: std::env::var("BUZZ_MAX_IMAGE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50 * 1024 * 1024),
            max_gif_bytes: std::env::var("BUZZ_MAX_GIF_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10 * 1024 * 1024),
            max_video_bytes: std::env::var("BUZZ_MAX_VIDEO_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500 * 1024 * 1024),
            max_file_bytes: std::env::var("BUZZ_MAX_FILE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100 * 1024 * 1024),
            public_base_url: std::env::var("BUZZ_MEDIA_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:3000/media".to_string()),
            // Per-upload-event records (`_uploads/` moderation side channel).
            // Off by default; coherence between the three knobs is enforced in
            // MediaConfig::validate at startup.
            upload_records_enabled: std::env::var("BUZZ_MEDIA_UPLOAD_RECORDS")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            upload_ip_header: std::env::var("BUZZ_MEDIA_UPLOAD_IP_HEADER")
                .ok()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty()),
            upload_port_header: std::env::var("BUZZ_MEDIA_UPLOAD_PORT_HEADER")
                .ok()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty()),
        };
        let media_max_concurrent_uploads: usize =
            std::env::var("BUZZ_MEDIA_MAX_CONCURRENT_UPLOADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&v| v > 0)
                .unwrap_or(8);
        let media_max_concurrent_uploads_per_pubkey: u32 =
            std::env::var("BUZZ_MEDIA_MAX_CONCURRENT_UPLOADS_PER_PUBKEY")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&v| v > 0)
                .unwrap_or(2)
                .min(u32::try_from(media_max_concurrent_uploads).unwrap_or(u32::MAX));
        let media_uploads_per_minute: u32 = std::env::var("BUZZ_MEDIA_UPLOADS_PER_MINUTE")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(30);

        let require_media_get_auth = std::env::var("BUZZ_REQUIRE_MEDIA_GET_AUTH")
            .map(|v| {
                v == "true"
                    || v == "1"
                    || v.eq_ignore_ascii_case("yes")
                    || v.eq_ignore_ascii_case("on")
            })
            .unwrap_or(false);

        let ephemeral_ttl_override = std::env::var("BUZZ_EPHEMERAL_TTL_OVERRIDE")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .filter(|&v| v > 0);

        if let Some(ttl) = ephemeral_ttl_override {
            warn!(
                "BUZZ_EPHEMERAL_TTL_OVERRIDE={ttl}s — all ephemeral channels will use \
                 this TTL instead of the client-provided value."
            );
        }

        // Git server config
        let git_repo_path = ensure_git_repo_path(
            std::env::var("BUZZ_GIT_REPO_PATH").unwrap_or_else(|_| "./repos".to_string()),
        )?;
        let git_pack_cache_path = ensure_git_path(
            "BUZZ_GIT_PACK_CACHE_PATH",
            std::env::var("BUZZ_GIT_PACK_CACHE_PATH")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| git_repo_path.join(".pack-cache")),
        )?;
        let git_max_pack_bytes: u64 = std::env::var("BUZZ_GIT_MAX_PACK_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500 * 1024 * 1024); // 500 MB
        let git_max_repo_bytes: u64 = std::env::var("BUZZ_GIT_MAX_REPO_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| git_max_pack_bytes.saturating_mul(2)); // 1 GB at defaults
        let git_pack_cache_max_bytes: u64 = std::env::var("BUZZ_GIT_PACK_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| git_max_repo_bytes.saturating_mul(5)); // 5 GB at defaults
        let git_pack_cache_max_concurrent_populations: usize =
            std::env::var("BUZZ_GIT_PACK_CACHE_MAX_CONCURRENT_POPULATIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|value| *value > 0)
                .unwrap_or(2);
        let git_max_repos_per_pubkey: u32 = std::env::var("BUZZ_GIT_MAX_REPOS_PER_PUBKEY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let git_max_concurrent_ops: usize = std::env::var("BUZZ_GIT_MAX_CONCURRENT_OPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);
        let git_hook_hmac_secret: String = std::env::var("BUZZ_GIT_HOOK_HMAC_SECRET")
            .unwrap_or_else(|_| {
                // Generate a random secret if not configured (dev mode).
                let secret: [u8; 32] = rand::random();
                hex::encode(secret)
            });
        let push_executor_key_id =
            std::env::var("BUZZ_PUSH_EXECUTOR_KEY_ID").unwrap_or_else(|_| "relay-v1".to_string());
        if push_executor_key_id.is_empty() || push_executor_key_id.len() > 64 {
            return Err(ConfigError::InvalidValue(
                "BUZZ_PUSH_EXECUTOR_KEY_ID must contain 1..=64 bytes".to_string(),
            ));
        }
        let push_gateway_delivery_url = match std::env::var("BUZZ_PUSH_GATEWAY_DELIVERY_URL") {
            Ok(raw) if raw.trim().is_empty() => None,
            Ok(raw) => Some(parse_push_gateway_delivery_url(&raw)?),
            Err(_) => Some(parse_push_gateway_delivery_url(
                DEFAULT_PUSH_GATEWAY_DELIVERY_URL,
            )?),
        };
        let push_gateway_timeout_millis = match std::env::var("BUZZ_PUSH_GATEWAY_TIMEOUT_MS") {
            Ok(raw) => raw
                .parse::<u64>()
                .ok()
                .filter(|millis| (100..=10_000).contains(millis))
                .ok_or_else(|| {
                    ConfigError::InvalidValue(
                        "BUZZ_PUSH_GATEWAY_TIMEOUT_MS must be an integer in 100..=10000"
                            .to_string(),
                    )
                })?,
            Err(_) => 2_000,
        };
        let push_gateway_timeout = Duration::from_millis(push_gateway_timeout_millis);

        const MAX_POLICY_MARKDOWN_BYTES: usize = 256 * 1024;
        let read_policy_markdown = |name: &str| -> Result<Option<String>, ConfigError> {
            let value = std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            if value
                .as_ref()
                .is_some_and(|value| value.len() > MAX_POLICY_MARKDOWN_BYTES)
            {
                return Err(ConfigError::InvalidValue(format!(
                    "{name} must contain at most {MAX_POLICY_MARKDOWN_BYTES} bytes"
                )));
            }
            Ok(value)
        };
        let terms_markdown = read_policy_markdown("BUZZ_TERMS_OF_SERVICE_MARKDOWN")?;
        let privacy_markdown = read_policy_markdown("BUZZ_PRIVACY_POLICY_MARKDOWN")?;
        let age_attestation_required = parse_optional_bool("BUZZ_AGE_ATTESTATION_REQUIRED")?;
        let audit_enabled = parse_bool("BUZZ_AUDIT_ENABLED", true)?;
        let join_policy = if terms_markdown.is_none()
            && privacy_markdown.is_none()
            && !age_attestation_required
        {
            None
        } else {
            let mut hasher = Sha256::new();
            hasher.update(terms_markdown.as_deref().unwrap_or_default().as_bytes());
            hasher.update([0]);
            hasher.update(privacy_markdown.as_deref().unwrap_or_default().as_bytes());
            hasher.update([0, u8::from(age_attestation_required)]);
            Some(JoinPolicyConfig {
                terms_markdown,
                privacy_markdown,
                age_attestation_required,
                version: hex::encode(hasher.finalize()),
            })
        };

        // Read-only deployment-admin surface. The route is absent when the host is unset.
        let admin = match std::env::var("BUZZ_ADMIN_HOST")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            None => None,
            Some(host) => {
                if host.contains(['/', '\\', '@']) {
                    return Err(ConfigError::InvalidValue(
                        "BUZZ_ADMIN_HOST must be an exact authority".to_string(),
                    ));
                }
                let web_dir = std::env::var("BUZZ_ADMIN_WEB_DIR")
                    .ok()
                    .map(|value| std::path::PathBuf::from(value.trim()))
                    .filter(|value| !value.as_os_str().is_empty());
                if let Some(ref dir) = web_dir {
                    if !dir.join("index.html").is_file() {
                        return Err(ConfigError::InvalidValue(format!(
                            "BUZZ_ADMIN_WEB_DIR={} does not contain index.html",
                            dir.display()
                        )));
                    }
                }
                Some(AdminConfig { host, web_dir })
            }
        };

        // Web UI static file serving
        let web_dir = std::env::var("BUZZ_WEB_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);
        let serve_git_web_gui = std::env::var("BUZZ_SERVE_GIT_WEB_GUI")
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false);

        if let Some(ref dir) = web_dir {
            if !dir.join("index.html").is_file() {
                return Err(ConfigError::InvalidValue(format!(
                    "BUZZ_WEB_DIR={} does not contain index.html",
                    dir.display()
                )));
            }
            tracing::info!("BUZZ_WEB_DIR={} — serving web UI from relay", dir.display());
        }

        // Reject explicitly-configured secrets that are too short.
        // The auto-generated fallback is always 64 hex chars (32 bytes), so this
        // only fires when someone sets BUZZ_GIT_HOOK_HMAC_SECRET to a weak value.
        if std::env::var("BUZZ_GIT_HOOK_HMAC_SECRET").is_ok() && git_hook_hmac_secret.len() < 32 {
            return Err(ConfigError::InvalidValue(
                "BUZZ_GIT_HOOK_HMAC_SECRET must be at least 32 characters (16 bytes hex)"
                    .to_string(),
            ));
        }

        Ok(Self {
            bind_addr,
            database_url,
            read_database_url,
            redis_url,
            solo_profile,
            db_backend,
            sqlite_path,
            messaging_backend,
            state_backend,
            zmq_bind,
            zmq_peers,
            relay_url,
            pairing_relay_url,
            max_connections,
            max_concurrent_handlers,
            send_buffer_size,
            max_frame_bytes,
            slow_client_grace_limit,
            auth,
            require_auth_token,
            cors_origins,
            relay_private_key,
            uds_path,
            health_port,
            metrics_port,
            pubkey_allowlist_enabled,
            require_relay_membership,
            capabilities,
            mesh,
            mesh_demo_echo,
            relay_owner_pubkey,
            relay_operator_api_origin,
            relay_operator_pubkeys,
            allow_nip_oa_auth,
            media,
            media_max_concurrent_uploads,
            media_max_concurrent_uploads_per_pubkey,
            media_uploads_per_minute,
            require_media_get_auth,
            audit_enabled,
            ephemeral_ttl_override,
            git_repo_path,
            git_pack_cache_path,
            git_max_pack_bytes,
            git_max_repo_bytes,
            git_pack_cache_max_bytes,
            git_pack_cache_max_concurrent_populations,
            git_max_repos_per_pubkey,
            git_max_concurrent_ops,
            git_hook_hmac_secret,
            push_executor_key_id,
            push_gateway_delivery_url,
            push_gateway_timeout,
            join_policy,
            admin,
            web_dir,
            serve_git_web_gui,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mutex to serialize tests that mutate environment variables.
    // Parallel env-var mutation causes `defaults_are_valid` to see the invalid
    // value set by `invalid_bind_addr_returns_error`, causing a flaky failure.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn defaults_are_valid() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let config = Config::from_env().expect("default config");
        assert!(config.bind_addr.port() > 0);
        assert!(!config.database_url.is_empty());
        assert!(!config.redis_url.is_empty());
        assert!(config.max_connections > 0);
        assert!(config.send_buffer_size > 0);
        assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        assert!(config.slow_client_grace_limit > 0);
        assert!(
            !config.pubkey_allowlist_enabled,
            "pubkey_allowlist_enabled should default to false"
        );
        assert!(
            !config.require_relay_membership,
            "require_relay_membership should default to false"
        );
        assert!(
            config.relay_owner_pubkey.is_none(),
            "relay_owner_pubkey should default to None"
        );
        assert!(
            config.relay_operator_pubkeys.is_empty(),
            "relay_operator_pubkeys should default empty (provisioning disabled)"
        );
        assert!(
            !config.allow_nip_oa_auth,
            "allow_nip_oa_auth should default to false"
        );
        assert!(
            !config.serve_git_web_gui,
            "serve_git_web_gui should default to false"
        );
        assert!(
            !config.require_media_get_auth,
            "require_media_get_auth should default to false for staged client rollout"
        );
        assert!(
            config.join_policy.is_none(),
            "join_policy should default to None so policy prompts and acceptance receipts are opt-in"
        );
        assert!(
            config.capabilities.huddle_audio,
            "huddle audio should default to on so single-pod (N=1) keeps today's huddle behavior"
        );
        assert!(
            config.capabilities.git,
            "git should default to on so an existing deployment upgrading with untouched env is unchanged"
        );
    }

    #[test]
    fn read_database_url_unset_or_blank_is_none() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let previous = std::env::var_os("READ_DATABASE_URL");

        std::env::remove_var("READ_DATABASE_URL");
        let unset = Config::from_env().expect("config").read_database_url;

        std::env::set_var("READ_DATABASE_URL", "   ");
        let blank = Config::from_env().expect("config").read_database_url;

        std::env::set_var("READ_DATABASE_URL", "postgres://buzz:pw@replica:5432/buzz"); // sadscan:disable np.postgres.1
        let set = Config::from_env().expect("config").read_database_url;

        if let Some(value) = previous {
            std::env::set_var("READ_DATABASE_URL", value);
        } else {
            std::env::remove_var("READ_DATABASE_URL");
        }

        assert_eq!(unset, None, "unset READ_DATABASE_URL must disable routing");
        assert_eq!(blank, None, "blank READ_DATABASE_URL must disable routing");
        assert_eq!(
            set.as_deref(),
            Some("postgres://buzz:pw@replica:5432/buzz") // sadscan:disable np.postgres.1
        );
    }

    #[test]
    fn audit_logging_defaults_on_and_accepts_explicit_off() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let previous = std::env::var_os("BUZZ_AUDIT_ENABLED");
        std::env::remove_var("BUZZ_AUDIT_ENABLED");
        assert!(parse_bool("BUZZ_AUDIT_ENABLED", true).unwrap());
        std::env::set_var("BUZZ_AUDIT_ENABLED", "false");
        assert!(!parse_bool("BUZZ_AUDIT_ENABLED", true).unwrap());
        if let Some(value) = previous {
            std::env::set_var("BUZZ_AUDIT_ENABLED", value);
        } else {
            std::env::remove_var("BUZZ_AUDIT_ENABLED");
        }
    }

    #[test]
    fn audit_logging_rejects_invalid_boolean() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let previous = std::env::var_os("BUZZ_AUDIT_ENABLED");
        std::env::set_var("BUZZ_AUDIT_ENABLED", "sometimes");
        let result = parse_bool("BUZZ_AUDIT_ENABLED", true);
        if let Some(value) = previous {
            std::env::set_var("BUZZ_AUDIT_ENABLED", value);
        } else {
            std::env::remove_var("BUZZ_AUDIT_ENABLED");
        }
        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref message))
                if message.contains("BUZZ_AUDIT_ENABLED")
        ));
    }

    #[test]
    fn join_policy_age_attestation_rejects_invalid_boolean() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let previous = std::env::var_os("BUZZ_AGE_ATTESTATION_REQUIRED");
        std::env::set_var("BUZZ_AGE_ATTESTATION_REQUIRED", "sometimes");
        let result = parse_optional_bool("BUZZ_AGE_ATTESTATION_REQUIRED");
        if let Some(value) = previous {
            std::env::set_var("BUZZ_AGE_ATTESTATION_REQUIRED", value);
        } else {
            std::env::remove_var("BUZZ_AGE_ATTESTATION_REQUIRED");
        }
        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref message))
                if message.contains("BUZZ_AGE_ATTESTATION_REQUIRED")
        ));
    }

    #[test]
    fn rate_limits_can_be_overridden() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_RATE_LIMIT_HUMAN_MESSAGES_PER_MIN", "1001");
        std::env::set_var("BUZZ_RATE_LIMIT_HUMAN_API_CALLS_PER_MIN", "1002");
        std::env::set_var("BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC", "1003");

        let config = Config::from_env().expect("config");

        std::env::remove_var("BUZZ_RATE_LIMIT_HUMAN_MESSAGES_PER_MIN");
        std::env::remove_var("BUZZ_RATE_LIMIT_HUMAN_API_CALLS_PER_MIN");
        std::env::remove_var("BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC");
        assert_eq!(config.auth.rate_limits.human_messages_per_min, 1001);
        assert_eq!(config.auth.rate_limits.human_api_calls_per_min, 1002);
        assert_eq!(config.auth.rate_limits.human_ws_events_per_sec, 1003);
    }

    #[test]
    fn rate_limit_overrides_reject_zero() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC", "0");
        let result = Config::from_env();
        std::env::remove_var("BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC");

        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref message))
                if message.contains("BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC")
        ));
    }

    #[test]
    fn relay_operator_pubkeys_parse_dedupe_and_normalize() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var(
            "RELAY_OPERATOR_PUBKEYS",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        std::env::set_var(
            "RELAY_OPERATOR_API_ORIGIN",
            "http://buzz.mesh.bb-production.com",
        );
        let config = Config::from_env().expect("config");
        std::env::remove_var("RELAY_OPERATOR_PUBKEYS");
        std::env::remove_var("RELAY_OPERATOR_API_ORIGIN");

        assert_eq!(
            config.relay_operator_pubkeys,
            vec![
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            ]
        );
    }

    #[test]
    fn relay_operator_pubkeys_invalid_entry_is_error() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("RELAY_OPERATOR_PUBKEYS", "not-a-pubkey");
        let result = Config::from_env();
        std::env::remove_var("RELAY_OPERATOR_PUBKEYS");

        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref msg)) if msg.contains("RELAY_OPERATOR_PUBKEYS")
        ));
    }

    #[test]
    fn relay_operator_pubkeys_require_api_origin() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var(
            "RELAY_OPERATOR_PUBKEYS",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        std::env::remove_var("RELAY_OPERATOR_API_ORIGIN");
        let result = Config::from_env();
        std::env::remove_var("RELAY_OPERATOR_PUBKEYS");

        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref msg)) if msg.contains("RELAY_OPERATOR_API_ORIGIN is required")
        ));
    }

    #[test]
    fn relay_operator_api_origin_rejects_paths() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("RELAY_OPERATOR_API_ORIGIN", "https://buzz.example/operator");
        let result = Config::from_env();
        std::env::remove_var("RELAY_OPERATOR_API_ORIGIN");

        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref msg)) if msg.contains("must be an http(s) origin")
        ));
    }

    #[test]
    fn push_gateway_defaults_to_buzz_and_can_be_disabled() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let previous = std::env::var_os("BUZZ_PUSH_GATEWAY_DELIVERY_URL");
        std::env::remove_var("BUZZ_PUSH_GATEWAY_DELIVERY_URL");
        let config = Config::from_env().expect("default config");
        assert_eq!(
            config
                .push_gateway_delivery_url
                .as_ref()
                .map(url::Url::as_str),
            Some(DEFAULT_PUSH_GATEWAY_DELIVERY_URL)
        );

        std::env::set_var("BUZZ_PUSH_GATEWAY_DELIVERY_URL", "");
        let config = Config::from_env().expect("disabled push config");
        assert!(config.push_gateway_delivery_url.is_none());

        if let Some(value) = previous {
            std::env::set_var("BUZZ_PUSH_GATEWAY_DELIVERY_URL", value);
        } else {
            std::env::remove_var("BUZZ_PUSH_GATEWAY_DELIVERY_URL");
        }
    }

    #[test]
    fn push_gateway_url_is_exact_and_fail_closed() {
        assert!(parse_push_gateway_delivery_url("https://push.example/v1/deliveries/apns").is_ok());
        for invalid in [
            "http://push.example/v1/deliveries/apns",
            "https://push.example/v1/deliveries/apns/",
            "https://push.example/v1/deliveries/apns?token=x",
            "https://user@push.example/v1/deliveries/apns",
        ] {
            assert!(
                parse_push_gateway_delivery_url(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn invalid_push_gateway_timeout_is_not_silently_defaulted() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_PUSH_GATEWAY_TIMEOUT_MS", "99");
        let result = Config::from_env();
        std::env::remove_var("BUZZ_PUSH_GATEWAY_TIMEOUT_MS");
        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref message))
                if message.contains("BUZZ_PUSH_GATEWAY_TIMEOUT_MS")
        ));
    }

    #[test]
    fn invalid_push_executor_key_id_is_rejected() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_PUSH_EXECUTOR_KEY_ID", "");
        let result = Config::from_env();
        std::env::remove_var("BUZZ_PUSH_EXECUTOR_KEY_ID");
        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref message))
                if message.contains("BUZZ_PUSH_EXECUTOR_KEY_ID")
        ));
    }

    /// Clears every variable that participates in the capability block so a
    /// matrix case only exercises what it sets.
    fn clear_capability_env() {
        std::env::remove_var("BUZZ_PROFILE");
        std::env::remove_var("BUZZ_CAPABILITY_GIT");
        std::env::remove_var("BUZZ_CAPABILITY_HUDDLE_AUDIO");
        std::env::remove_var("BUZZ_HUDDLE_AUDIO_AVAILABLE");
    }

    #[test]
    fn legacy_huddle_audio_available_alias_still_disables_audio() {
        let _guard = ENV_MUTEX.lock().unwrap();
        clear_capability_env();
        std::env::set_var("BUZZ_HUDDLE_AUDIO_AVAILABLE", "false");
        let config = Config::from_env().expect("config");
        clear_capability_env();
        assert!(
            !config.capabilities.huddle_audio,
            "BUZZ_HUDDLE_AUDIO_AVAILABLE=false must disable huddle audio (multi-pod deployments)"
        );
    }

    #[test]
    fn capability_huddle_audio_var_overrides_legacy_alias() {
        let _guard = ENV_MUTEX.lock().unwrap();

        // Canonical variable wins over the legacy alias in both directions.
        clear_capability_env();
        std::env::set_var("BUZZ_HUDDLE_AUDIO_AVAILABLE", "true");
        std::env::set_var("BUZZ_CAPABILITY_HUDDLE_AUDIO", "false");
        let canonical_off = Config::from_env().expect("config");

        clear_capability_env();
        std::env::set_var("BUZZ_HUDDLE_AUDIO_AVAILABLE", "false");
        std::env::set_var("BUZZ_CAPABILITY_HUDDLE_AUDIO", "true");
        let canonical_on = Config::from_env().expect("config");

        clear_capability_env();

        assert!(
            !canonical_off.capabilities.huddle_audio,
            "BUZZ_CAPABILITY_HUDDLE_AUDIO must take precedence over the legacy alias"
        );
        assert!(
            canonical_on.capabilities.huddle_audio,
            "BUZZ_CAPABILITY_HUDDLE_AUDIO=true must re-enable audio the legacy alias disabled"
        );
    }

    #[test]
    fn git_capability_defaults_on_but_off_under_solo_profile() {
        let _guard = ENV_MUTEX.lock().unwrap();

        clear_capability_env();
        let default_profile = Config::from_env().expect("config");

        clear_capability_env();
        std::env::set_var("BUZZ_PROFILE", "solo");
        let solo = Config::from_env().expect("config");

        clear_capability_env();

        assert!(
            default_profile.capabilities.git,
            "git must default on so a served deployment upgrading with untouched env is unchanged"
        );
        assert!(
            !solo.capabilities.git,
            "git must default off under BUZZ_PROFILE=solo — the object store is S3-only"
        );
        assert!(
            solo.capabilities.huddle_audio,
            "solo must keep huddle audio: it is in-process and needs no external service"
        );
    }

    #[test]
    fn explicit_git_capability_overrides_either_profile_default() {
        let _guard = ENV_MUTEX.lock().unwrap();

        clear_capability_env();
        std::env::set_var("BUZZ_CAPABILITY_GIT", "false");
        let default_profile_off = Config::from_env().expect("config");

        clear_capability_env();
        std::env::set_var("BUZZ_PROFILE", "solo");
        std::env::set_var("BUZZ_CAPABILITY_GIT", "true");
        let solo_on = Config::from_env().expect("config");

        clear_capability_env();

        assert!(
            !default_profile_off.capabilities.git,
            "BUZZ_CAPABILITY_GIT=false must turn git off on the default profile"
        );
        assert!(
            solo_on.capabilities.git,
            "BUZZ_CAPABILITY_GIT=true must turn git on under solo (solo + real S3 is legitimate)"
        );
    }

    #[test]
    fn capability_flags_accept_common_spellings_and_reject_typos() {
        let _guard = ENV_MUTEX.lock().unwrap();
        clear_capability_env();

        for truthy in ["true", "1", "on", "yes", "TRUE", " On "] {
            std::env::set_var("BUZZ_CAPABILITY_GIT", truthy);
            let config = Config::from_env().expect("config");
            assert!(config.capabilities.git, "'{truthy}' should parse as true");
        }
        for falsy in ["false", "0", "off", "no", "FALSE", " Off "] {
            std::env::set_var("BUZZ_CAPABILITY_GIT", falsy);
            let config = Config::from_env().expect("config");
            assert!(!config.capabilities.git, "'{falsy}' should parse as false");
        }

        std::env::set_var("BUZZ_CAPABILITY_GIT", "flase");
        let typo = Config::from_env();
        clear_capability_env();
        assert!(
            matches!(
                typo,
                Err(ConfigError::InvalidValue(ref message))
                    if message.contains("BUZZ_CAPABILITY_GIT")
            ),
            "a capability typo must fail the boot loudly, not silently default"
        );
    }

    #[test]
    fn invalid_bind_addr_returns_error() {
        assert!(matches!(
            parse_bind_addr("not-an-addr"),
            Err(ConfigError::InvalidBindAddr(_))
        ));
    }

    #[test]
    fn pairing_relay_url_accepts_websocket_urls_and_rejects_http() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_PAIRING_RELAY_URL", "wss://pairing.buzz.xyz");
        let config = Config::from_env().expect("config");
        assert_eq!(
            config.pairing_relay_url.as_deref(),
            Some("wss://pairing.buzz.xyz")
        );

        std::env::set_var("BUZZ_PAIRING_RELAY_URL", "https://pairing.buzz.xyz");
        let result = Config::from_env();
        std::env::remove_var("BUZZ_PAIRING_RELAY_URL");
        assert!(matches!(
            result,
            Err(ConfigError::InvalidValue(ref msg)) if msg.contains("BUZZ_PAIRING_RELAY_URL")
        ));
    }

    #[test]
    fn max_frame_bytes_can_be_configured() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("BUZZ_MAX_FRAME_BYTES", "262144");
        let config = Config::from_env().expect("config");
        std::env::remove_var("BUZZ_MAX_FRAME_BYTES");
        assert_eq!(config.max_frame_bytes, 262_144);
    }

    #[test]
    fn git_repo_path_is_created_if_missing() {
        let _guard = ENV_MUTEX.lock().unwrap();
        // Pick a path under temp_dir that definitely doesn't exist yet.
        let base = std::env::temp_dir().join(format!(
            "buzz-test-git-repo-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let nested = base.join("nested").join("repos");
        assert!(!nested.exists(), "test precondition: path must not exist");

        std::env::set_var("BUZZ_GIT_REPO_PATH", &nested);
        let result = Config::from_env();
        std::env::remove_var("BUZZ_GIT_REPO_PATH");

        let config = result.expect("config should self-bootstrap missing git_repo_path");
        assert_eq!(config.git_repo_path, nested);
        assert!(
            nested.is_dir(),
            "git_repo_path should exist after config load"
        );

        // Cleanup.
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    #[cfg(unix)]
    fn git_repo_path_unwritable_returns_error() {
        // Try to create a path under a regular file — must fail.
        // Using /dev/null as the parent guarantees create_dir_all fails on unix.
        let bogus = std::path::PathBuf::from("/dev/null/cannot-create-here");
        let result = ensure_git_repo_path(&bogus);
        assert!(
            matches!(result, Err(ConfigError::InvalidValue(ref msg)) if msg.contains("BUZZ_GIT_REPO_PATH")),
            "expected InvalidValue mentioning BUZZ_GIT_REPO_PATH, got {result:?}"
        );
    }
}

#!/usr/bin/env bash
# =============================================================================
# zmq-soak.sh — two-node ZeroMQ static-mesh soak / chaos harness
# =============================================================================
# Boots two relay nodes wired as ZMQ peers of each other, drives cross-node
# message flow, kills and restarts one node, and asserts the documented
# delivery contract holds. This is the Phase 5 §7.4 soak from
# docs/hive-implementation-plan.md — the gate that multi-node ZMQ has to pass
# before it stops being labeled experimental.
#
# Usage:
#   ./scripts/zmq-soak.sh [--duration 30] [--rate 5] [--no-build] [--keep-logs]
#
# Options:
#   --duration <s>   Steady-state phase length in seconds (default: 30)
#   --rate <n>       Events/sec per direction during steady state (default: 5)
#   --no-build       Use an existing relay binary (DEFAULT — see below)
#   --build          Build the relay first (cargo build -p buzz-relay)
#   --profile <p>    Cargo profile / target dir to find the binary (default: release)
#   --keep-logs      Keep the working directory on success (it is always kept
#                    on failure)
#   --min-delivery   Steady-state delivery ratio required to pass (default: 0.99)
#
# -----------------------------------------------------------------------------
# ⚠️  THIS IS A DEVELOPMENT TOPOLOGY, NOT A PRODUCTION ONE.  ⚠️
# -----------------------------------------------------------------------------
# Two relay nodes here share ONE SQLITE FILE over WAL. That is a deliberate
# local-dev shortcut so this soak runs on a laptop with no Postgres, and it is
# NOT a supported production deployment. SQLite is single-writer storage; two
# relay processes writing one file serialize on the write lock and will
# contend under real load. Production multi-node runs on Postgres —
# deploy/compose/zmq-pair.yml is the supported shape.
#
# Set DATABASE_URL to point both nodes at a Postgres instance instead, which
# is the topology you should actually soak before a release:
#
#   DATABASE_URL=postgres://buzz:buzz@localhost:5432/buzz ./scripts/zmq-soak.sh
#
# -----------------------------------------------------------------------------
# Why Redis is required even though this is "the ZMQ soak"
# -----------------------------------------------------------------------------
# ZMQ replaces Redis for pub/sub fan-out only. The relay's shared state (NIP-98
# replay guard + rate limiter) is a cross-node correctness fence, and
# crates/buzz-relay/src/config.rs REJECTS the unsafe combinations outright:
#
#   BUZZ_ZMQ_PEERS set + BUZZ_STATE_BACKEND unset  -> startup error
#   BUZZ_ZMQ_PEERS set + BUZZ_STATE_BACKEND=inproc -> startup error
#
# So a two-node ZMQ mesh is zmq-messaging + redis-state, and this script starts
# its own throwaway redis-server on a non-default port. There is no valid
# Redis-free multi-node configuration to soak.
#
# -----------------------------------------------------------------------------
# Community boundary: why both nodes claim to be localhost:3000
# -----------------------------------------------------------------------------
# The relay derives a connection's community from the HTTP Host header, and the
# boot-time community row from RELAY_URL (host + port). Two nodes on different
# ports with different RELAY_URLs would be two DIFFERENT communities with
# different pubsub topics and would never fan out to each other — the soak
# would "pass" while testing nothing. So both nodes set RELAY_URL to the same
# canonical host, and the driver connects to node B's port while sending
# `Host: localhost:3000`. One community, two processes.
#
# -----------------------------------------------------------------------------
# Delivery contract under test (at-most-once, same as Redis pub/sub)
# -----------------------------------------------------------------------------
# Phase 2 kills node B while node A keeps publishing. Those events are
# permanently lost from B's LIVE fan-out — that is correct, not a bug. They are
# still committed to the shared store, which the soak asserts explicitly. ZMQ
# fan-out is ephemeral; durability is the event store's job.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DURATION=30
RATE=5
SKIP_BUILD=true
CARGO_PROFILE="release"
KEEP_LOGS=false
MIN_DELIVERY=0.99

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --no-build) SKIP_BUILD=true; shift ;;
    --build) SKIP_BUILD=false; shift ;;
    --profile) CARGO_PROFILE="$2"; shift 2 ;;
    --keep-logs) KEEP_LOGS=true; shift ;;
    --min-delivery) MIN_DELIVERY="$2"; shift 2 ;;
    -h|--help) sed -n '2,80p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${BLUE}[zmq-soak]${NC} $*"; }
ok()   { echo -e "${GREEN}[zmq-soak]${NC} $*"; }
warn() { echo -e "${YELLOW}[zmq-soak]${NC} $*"; }
err()  { echo -e "${RED}[zmq-soak]${NC} $*" >&2; }

cd "${REPO_ROOT}"

profile_dir() { case "$1" in dev) echo "debug" ;; *) echo "$1" ;; esac; }

# ── Topology constants ───────────────────────────────────────────────────────
# Every port is offset so a running dev relay / redis on the defaults is not
# disturbed. Node A and node B must differ in ALL of ws/health/metrics/zmq.

CANONICAL_HOST="localhost:3000"          # the one community both nodes serve

A_WS_PORT=3000
A_HEALTH_PORT=8080
A_METRICS_PORT=9102
A_ZMQ_PORT=5559

B_WS_PORT=3001
B_HEALTH_PORT=8081
B_METRICS_PORT=9103
B_ZMQ_PORT=5560

REDIS_PORT=6399                          # not 6379: never touch a dev Redis

WORK_DIR="${REPO_ROOT}/target/zmq-soak"
RELAY_BIN="${REPO_ROOT}/target/$(profile_dir "${CARGO_PROFILE}")/buzz-relay"
DRIVER="${SCRIPT_DIR}/lib/zmq_soak_driver.py"
MARKER="soak-$$-$(date +%s)"

SOAK_FAILED=0
fail() { err "$*"; SOAK_FAILED=1; }

# ── Cleanup ──────────────────────────────────────────────────────────────────
# Kills everything this script started, in reverse order, tolerating anything
# already dead. Runs on success, failure, and Ctrl-C alike.

cleanup() {
  local status=$?
  set +e
  for pidfile in "${WORK_DIR}"/*.pid; do
    [[ -f "${pidfile}" ]] || continue
    local pid
    pid="$(cat "${pidfile}" 2>/dev/null)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null
      for _ in $(seq 1 20); do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.25
      done
      kill -0 "${pid}" 2>/dev/null && kill -9 "${pid}" 2>/dev/null
    fi
    rm -f "${pidfile}"
  done
  if [[ ${status} -eq 0 && ${SOAK_FAILED} -eq 0 && "${KEEP_LOGS}" != "true" ]]; then
    rm -rf "${WORK_DIR}"
  else
    [[ -d "${WORK_DIR}" ]] && err "Logs kept in ${WORK_DIR}"
  fi
  return ${status}
}
trap cleanup EXIT INT TERM

# ── Preflight ────────────────────────────────────────────────────────────────

for tool in python3 curl; do
  command -v "${tool}" >/dev/null 2>&1 || { err "${tool} is required"; exit 1; }
done
[[ -f "${DRIVER}" ]] || { err "traffic driver missing: ${DRIVER}"; exit 1; }

if [[ "${SKIP_BUILD}" == "true" ]]; then
  if [[ ! -x "${RELAY_BIN}" ]]; then
    err "--no-build (default): ${RELAY_BIN} missing or not executable."
    err "Build it first:  cargo build --profile ${CARGO_PROFILE} -p buzz-relay"
    err "Or re-run with --build."
    exit 1
  fi
  log "Using prebuilt relay: ${RELAY_BIN}"
else
  log "Building relay (profile: ${CARGO_PROFILE})..."
  cargo build --profile "${CARGO_PROFILE}" -p buzz-relay
fi

USE_POSTGRES=false
if [[ -n "${DATABASE_URL:-}" ]]; then
  USE_POSTGRES=true
  log "DATABASE_URL is set — soaking the SUPPORTED topology (shared Postgres)."
else
  warn "DATABASE_URL not set — falling back to the shared-SQLite DEV topology."
  warn "Two relays sharing one SQLite file is NOT a supported production shape;"
  warn "it exists so this soak runs without Postgres. See the header."
fi

# Redis is mandatory for multi-node ZMQ (shared state). Prefer an existing
# server the caller points us at; otherwise start a throwaway one.
STARTED_REDIS=false
if [[ -n "${REDIS_URL:-}" ]]; then
  log "Using caller-provided REDIS_URL"
  SOAK_REDIS_URL="${REDIS_URL}"
elif command -v redis-server >/dev/null 2>&1; then
  SOAK_REDIS_URL="redis://127.0.0.1:${REDIS_PORT}"
  STARTED_REDIS=true
else
  err "SKIPPED: multi-node ZMQ requires a shared state backend (Redis), and"
  err "neither REDIS_URL nor a redis-server binary is available."
  err "Install redis-server or export REDIS_URL=redis://host:port."
  exit 0
fi

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/media-a" "${WORK_DIR}/media-b"

if [[ "${STARTED_REDIS}" == "true" ]]; then
  log "Starting throwaway redis-server on :${REDIS_PORT}"
  redis-server --port "${REDIS_PORT}" --save '' --appendonly no \
    --dir "${WORK_DIR}" > "${WORK_DIR}/redis.log" 2>&1 &
  echo $! > "${WORK_DIR}/redis.pid"
  redis_ready=false
  for _ in $(seq 1 40); do
    if redis-cli -p "${REDIS_PORT}" ping 2>/dev/null | grep -q PONG; then
      redis_ready=true; break
    fi
    sleep 0.25
  done
  [[ "${redis_ready}" == "true" ]] || { err "redis-server did not become ready"; cat "${WORK_DIR}/redis.log" >&2; exit 1; }
  ok "Redis ready on :${REDIS_PORT}"
fi

# ── Node boot ────────────────────────────────────────────────────────────────
# start_node <name> <ws> <health> <metrics> <own-zmq-port> <peer-zmq-port> <migrate>
#
# BUZZ_PROFILE=solo supplies the local-FS media + sqlite defaults; the
# messaging/state backends are overridden explicitly because solo defaults to
# in-process messaging, which would give zero cross-node fan-out and a soak
# that passes while testing nothing.

start_node() {
  local name="$1" ws="$2" health="$3" metrics="$4" own_zmq="$5" peer_zmq="$6" migrate="$7"
  local db_env=()
  if [[ "${USE_POSTGRES}" == "true" ]]; then
    db_env=(BUZZ_DB_BACKEND=postgres "DATABASE_URL=${DATABASE_URL}")
  else
    db_env=(BUZZ_DB_BACKEND=sqlite "BUZZ_SQLITE_PATH=${WORK_DIR}/buzz.db")
  fi

  nohup env \
    BUZZ_PROFILE=solo \
    "${db_env[@]}" \
    BUZZ_AUTO_MIGRATE="${migrate}" \
    BUZZ_MESSAGING_BACKEND=zmq \
    BUZZ_STATE_BACKEND=redis \
    "REDIS_URL=${SOAK_REDIS_URL}" \
    "BUZZ_ZMQ_BIND=tcp://127.0.0.1:${own_zmq}" \
    "BUZZ_ZMQ_PEERS=tcp://127.0.0.1:${peer_zmq}" \
    "BUZZ_MEDIA_PATH=${WORK_DIR}/media-${name}" \
    "BUZZ_BIND_ADDR=127.0.0.1:${ws}" \
    "BUZZ_HEALTH_PORT=${health}" \
    "BUZZ_METRICS_PORT=${metrics}" \
    "RELAY_URL=ws://${CANONICAL_HOST}" \
    BUZZ_REQUIRE_AUTH_TOKEN=false \
    BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC=1000 \
    BUZZ_RATE_LIMIT_HUMAN_MESSAGES_PER_MIN=100000 \
    "${RELAY_BIN}" >> "${WORK_DIR}/relay-${name}.log" 2>&1 &
  echo $! > "${WORK_DIR}/relay-${name}.pid"
}

wait_ready() {
  local name="$1" health="$2" attempts="${3:-90}"
  local pidfile="${WORK_DIR}/relay-${name}.pid"
  for _ in $(seq 1 "${attempts}"); do
    if ! kill -0 "$(cat "${pidfile}" 2>/dev/null)" 2>/dev/null; then
      err "Relay ${name} died during startup; last 60 log lines:"
      tail -n 60 "${WORK_DIR}/relay-${name}.log" >&2
      return 1
    fi
    if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${health}/_readiness" || true)" == "200" ]]; then
      return 0
    fi
    sleep 1
  done
  err "Relay ${name} did not become ready; last 60 log lines:"
  tail -n 60 "${WORK_DIR}/relay-${name}.log" >&2
  return 1
}

# Node A migrates; node B starts only after A is ready so they never race the
# same schema (and, on SQLite, never race creating the same file).
log "Starting node A (ws :${A_WS_PORT}, zmq :${A_ZMQ_PORT} -> peer :${B_ZMQ_PORT})"
start_node a "${A_WS_PORT}" "${A_HEALTH_PORT}" "${A_METRICS_PORT}" "${A_ZMQ_PORT}" "${B_ZMQ_PORT}" true
wait_ready a "${A_HEALTH_PORT}" || exit 1
ok "Node A ready"

log "Starting node B (ws :${B_WS_PORT}, zmq :${B_ZMQ_PORT} -> peer :${A_ZMQ_PORT})"
start_node b "${B_WS_PORT}" "${B_HEALTH_PORT}" "${B_METRICS_PORT}" "${B_ZMQ_PORT}" "${A_ZMQ_PORT}" false
wait_ready b "${B_HEALTH_PORT}" || exit 1
ok "Node B ready"

NODE_A_WS="ws://127.0.0.1:${A_WS_PORT}/"
NODE_B_WS="ws://127.0.0.1:${B_WS_PORT}/"
NODE_B_HTTP="http://127.0.0.1:${B_WS_PORT}/query"

run_driver() {
  python3 "${DRIVER}" --host "${CANONICAL_HOST}" --marker "${MARKER}" "$@"
}

# ── Phase 1: steady-state bidirectional fan-out ──────────────────────────────

log "── Phase 1: steady-state cross-node fan-out (${DURATION}s @ ${RATE}/s each way)"
PHASE1_OUT="${WORK_DIR}/phase1.json"
if run_driver steady \
      --node-a "${NODE_A_WS}" --node-b "${NODE_B_WS}" \
      --duration "${DURATION}" --rate "${RATE}" \
      --min-delivery "${MIN_DELIVERY}" > "${PHASE1_OUT}"; then
  ok "Phase 1 passed: $(cat "${PHASE1_OUT}")"
else
  fail "Phase 1 FAILED: $(cat "${PHASE1_OUT}" 2>/dev/null)"
fi

# ── Phase 2: peer kill / restart ─────────────────────────────────────────────

log "── Phase 2: peer kill/restart"

log "Killing node B"
B_PID="$(cat "${WORK_DIR}/relay-b.pid")"
kill "${B_PID}" 2>/dev/null || true
for _ in $(seq 1 40); do kill -0 "${B_PID}" 2>/dev/null || break; sleep 0.25; done
kill -0 "${B_PID}" 2>/dev/null && kill -9 "${B_PID}" 2>/dev/null || true
rm -f "${WORK_DIR}/relay-b.pid"
ok "Node B down"

log "Publishing on A while B is down (these are expected to be lost from B's live fan-out)"
OUTAGE_IDS="${WORK_DIR}/outage-ids.json"
PHASE2A_OUT="${WORK_DIR}/phase2-publish.json"
if run_driver publish \
      --node "${NODE_A_WS}" --count 10 --rate "${RATE}" \
      --label outage --out "${OUTAGE_IDS}" > "${PHASE2A_OUT}"; then
  ok "Published during outage: $(cat "${PHASE2A_OUT}")"
else
  fail "Publishing during outage FAILED: $(cat "${PHASE2A_OUT}" 2>/dev/null)"
fi

log "Restarting node B"
start_node b "${B_WS_PORT}" "${B_HEALTH_PORT}" "${B_METRICS_PORT}" "${B_ZMQ_PORT}" "${A_ZMQ_PORT}" false
if wait_ready b "${B_HEALTH_PORT}"; then
  ok "Node B back up"
else
  fail "Node B did not come back up"
fi

log "Asserting A->B live fan-out resumes"
PHASE2B_OUT="${WORK_DIR}/phase2-resume.json"
if run_driver resume \
      --node-a "${NODE_A_WS}" --node-b "${NODE_B_WS}" --timeout 60 > "${PHASE2B_OUT}"; then
  ok "Fan-out resumed: $(cat "${PHASE2B_OUT}")"
else
  fail "Fan-out did NOT resume: $(cat "${PHASE2B_OUT}" 2>/dev/null)"
fi

# The other half of the at-most-once contract: the outage-window events never
# reached B's live subscribers, but they must still be readable from the store.
log "Asserting outage-window events are durable in the store (read via B)"
PHASE2C_OUT="${WORK_DIR}/phase2-durability.json"
if [[ -f "${OUTAGE_IDS}" ]] && run_driver verify-store \
      --node "${NODE_B_HTTP}" --ids-file "${OUTAGE_IDS}" > "${PHASE2C_OUT}"; then
  ok "Durability held: $(cat "${PHASE2C_OUT}")"
else
  fail "Durability check FAILED: $(cat "${PHASE2C_OUT}" 2>/dev/null)"
fi

# ── Phase 3: report ──────────────────────────────────────────────────────────

log "── Phase 3: report"

# Snapshot the fan-out metrics from both nodes. buzz_multinode_fanout_total is
# the load-bearing one: it counts events a node received FROM A PEER over
# pubsub (local echoes are deduped out), so a non-zero value on each node is
# independent evidence the mesh actually carried traffic.
snapshot_metrics() {
  local name="$1" port="$2"
  echo "--- node ${name} (:${port}) ---"
  curl -s --max-time 5 "http://127.0.0.1:${port}/metrics" \
    | grep -E '^(buzz_multinode_fanout_total|buzz_multinode_fanout_lag_total|buzz_events_received_total|buzz_events_stored_total|buzz_events_rejected_total|buzz_ws_connections_active|buzz_subscriptions_active)' \
    || echo "(no matching metrics returned)"
}

METRICS_OUT="${WORK_DIR}/metrics.txt"
{
  snapshot_metrics a "${A_METRICS_PORT}"
  snapshot_metrics b "${B_METRICS_PORT}"
} > "${METRICS_OUT}" 2>&1
cat "${METRICS_OUT}"

# buzz_multinode_fanout_total only increments on events arriving from a peer,
# so if it is absent or zero on both nodes the mesh never carried anything —
# which would mean the phases above passed for the wrong reason (e.g. both
# nodes reading the same shared database rather than gossiping).
if grep -qE '^buzz_multinode_fanout_total ([1-9][0-9]*|[0-9]+\.[0-9]*[1-9])' "${METRICS_OUT}"; then
  ok "Cross-node fan-out confirmed by buzz_multinode_fanout_total"
else
  fail "buzz_multinode_fanout_total is absent or zero on BOTH nodes — the ZMQ mesh carried no traffic"
fi

echo
echo "================== ZMQ SOAK SUMMARY =================="
printf 'topology     : %s\n' "$([[ "${USE_POSTGRES}" == "true" ]] && echo 'shared Postgres (supported)' || echo 'shared SQLite/WAL (DEV ONLY)')"
printf 'nodes        : A ws :%s / zmq :%s   B ws :%s / zmq :%s\n' "${A_WS_PORT}" "${A_ZMQ_PORT}" "${B_WS_PORT}" "${B_ZMQ_PORT}"
printf 'community    : %s (identical RELAY_URL on both nodes)\n' "${CANONICAL_HOST}"
printf 'state backend: redis @ %s\n' "${SOAK_REDIS_URL}"
echo "------------------------------------------------------"
for f in "${PHASE1_OUT}" "${PHASE2A_OUT}" "${PHASE2B_OUT}" "${PHASE2C_OUT}"; do
  [[ -f "${f}" ]] && cat "${f}"
done
echo "------------------------------------------------------"

if [[ ${SOAK_FAILED} -ne 0 ]]; then
  err "SOAK FAILED"
  exit 1
fi
ok "SOAK PASSED"

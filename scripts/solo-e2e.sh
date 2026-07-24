#!/usr/bin/env bash
# =============================================================================
# solo-e2e.sh — E2E suite against the Solo profile (zero external services)
# =============================================================================
# Boots the relay with BUZZ_DB_BACKEND=sqlite, in-process messaging, and
# local-filesystem media — no Postgres, no Redis, no MinIO — then runs the
# relay-protocol E2E suites against it. This is the standing regression gate
# for the Solo profile: if it passes, the "single binary + local files"
# deployment story works end to end.
#
# Usage:
#   ./scripts/solo-e2e.sh [--profile <cargo-profile>] [--no-build] \
#                         [--nextest-archive <path>]
#
# Options:
#   --profile <profile>        Cargo build/test profile (default: ci)
#   --no-build                 Use an existing target/<profile>/buzz-relay
#                              binary (CI artifact reuse).
#   --nextest-archive <path>   Run the suites from a prebuilt nextest archive
#                              (CI artifact reuse) instead of `cargo test` —
#                              no Rust compilation happens at all.
#
# Suites run (all #[ignore]d tests, forced with --ignored):
#   e2e_relay          relay WebSocket protocol + invites + membership
#   e2e_nostr_interop  NIP-50 search (SQLite FTS5), NIP-10 threads, NIP-17
#   e2e_media          Blossom upload/download on the local-FS media backend
#   e2e_workstream     Hive Workstream kinds (35000-35003 NIP-33 heads +
#                      47001-47030 append-only history)
#
# Not run here (documented gaps, not accidents):
#   e2e_git            git-on-object-storage needs an S3-style backend; the
#                      A3 conformance probe is disabled below for the same
#                      reason. Git on Solo is a later phase.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CARGO_PROFILE="${CARGO_PROFILE:-ci}"
SKIP_BUILD=false
NEXTEST_ARCHIVE=""

# Cargo writes the `dev` profile to target/debug (and `release` to
# target/release); every other profile gets its own directory.
profile_dir() {
  case "$1" in
    dev) echo "debug" ;;
    *) echo "$1" ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      CARGO_PROFILE="$2"
      shift 2
      ;;
    --no-build)
      SKIP_BUILD=true
      shift
      ;;
    --nextest-archive)
      NEXTEST_ARCHIVE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log()   { echo -e "${BLUE}[solo-e2e]${NC} $*"; }
ok()    { echo -e "${GREEN}[solo-e2e]${NC} $*"; }
err()   { echo -e "${RED}[solo-e2e]${NC} $*" >&2; }

cd "${REPO_ROOT}"

# ── Fresh Solo data directory ────────────────────────────────────────────────

SOLO_DIR="${REPO_ROOT}/target/solo-e2e"
RELAY_LOG="${SOLO_DIR}/relay.log"
RELAY_PID_FILE="${SOLO_DIR}/relay.pid"
rm -rf "${SOLO_DIR}"
mkdir -p "${SOLO_DIR}/media"

cleanup() {
  if [[ -f "${RELAY_PID_FILE}" ]]; then
    kill "$(cat "${RELAY_PID_FILE}")" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ── Build relay ──────────────────────────────────────────────────────────────

if [[ "${SKIP_BUILD}" == "true" ]]; then
  if [[ ! -x "./target/$(profile_dir "${CARGO_PROFILE}")/buzz-relay" ]]; then
    err "--no-build: ./target/$(profile_dir "${CARGO_PROFILE}")/buzz-relay missing or not executable"
    exit 1
  fi
  log "Skipping relay build (--no-build); using target/$(profile_dir "${CARGO_PROFILE}")/buzz-relay"
else
  log "Building relay (profile: ${CARGO_PROFILE})..."
  cargo build --profile "${CARGO_PROFILE}" -p buzz-relay
  ok "Relay built"
fi

# ── Start relay: zero external services ──────────────────────────────────────
# BUZZ_PROFILE=solo IS the configuration under test: it defaults the whole
# backend trio (sqlite + inproc messaging + local-FS media), auto-migrates,
# and skips the S3 git A3 conformance probe. The only overrides are test
# plumbing: data paths isolated under target/solo-e2e, and a raised WS
# admission budget because the subscription-limit test opens 1024 REQs in a
# burst to exercise MAX_SUBSCRIPTIONS — impossible under the default
# 10/s x 5s window. The deployment community (host=localhost:3000) is
# auto-ensured at boot from RELAY_URL — no seeding step.

log "Starting Solo relay (--profile solo: sqlite + inproc messaging + local media)..."
nohup env \
  BUZZ_PROFILE=solo \
  BUZZ_SQLITE_PATH="${SOLO_DIR}/buzz.db" \
  BUZZ_MEDIA_PATH="${SOLO_DIR}/media" \
  RELAY_URL=ws://localhost:3000 \
  BUZZ_BIND_ADDR=0.0.0.0:3000 \
  BUZZ_REQUIRE_AUTH_TOKEN=false \
  BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC=1000 \
  "./target/$(profile_dir "${CARGO_PROFILE}")/buzz-relay" > "${RELAY_LOG}" 2>&1 &
echo $! > "${RELAY_PID_FILE}"

log "Waiting for relay readiness..."
ready=false
for _attempt in $(seq 1 60); do
  if ! kill -0 "$(cat "${RELAY_PID_FILE}")" 2>/dev/null; then
    err "Relay process died"
    cat "${RELAY_LOG}"
    exit 1
  fi
  status_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:3000/_readiness || true)
  if [ "${status_code}" = "200" ]; then
    ok "Solo relay is ready at ws://localhost:3000"
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != "true" ]]; then
  err "Relay did not become ready within 60s"
  cat "${RELAY_LOG}"
  exit 1
fi

# ── Run the E2E suites ───────────────────────────────────────────────────────
# DATABASE_URL points the test seed helpers (communities beyond the deployment
# host, relay-member roles) at the relay's own SQLite file — WAL mode allows a
# second writer process alongside the serving relay.

log "Running E2E suites against the Solo relay..."
test_status=0
if [[ -n "${NEXTEST_ARCHIVE}" ]]; then
  env \
    RELAY_URL=ws://localhost:3000 \
    RELAY_HTTP_URL=http://localhost:3000 \
    DATABASE_URL="sqlite:${SOLO_DIR}/buzz.db" \
    cargo nextest run \
      --archive-file "${NEXTEST_ARCHIVE}" \
      -E 'binary(e2e_relay) or binary(e2e_nostr_interop) or binary(e2e_media) or binary(e2e_workstream)' \
      --run-ignored ignored-only || test_status=$?
else
  env \
    RELAY_URL=ws://localhost:3000 \
    RELAY_HTTP_URL=http://localhost:3000 \
    DATABASE_URL="sqlite:${SOLO_DIR}/buzz.db" \
    cargo test --profile "${CARGO_PROFILE}" -p buzz-test-client \
      --test e2e_relay \
      --test e2e_nostr_interop \
      --test e2e_media \
      --test e2e_workstream \
      -- --ignored || test_status=$?
fi

if [[ ${test_status} -ne 0 ]]; then
  err "E2E suites failed (exit ${test_status}); last 100 relay log lines:"
  tail -n 100 "${RELAY_LOG}" >&2
  exit "${test_status}"
fi

ok "Solo profile E2E gate passed (zero external services)"

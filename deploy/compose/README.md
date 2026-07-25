# Buzz Docker Compose deployment

This is the single-node/VPS deployment bundle. It is intentionally separate from
the root `docker-compose.yml`, which remains local development infrastructure.

## Which file do I want?

| File | Services | Shape |
|---|---|---|
| `solo.yml` | 1 (relay only) | Solo profile: SQLite + in-process messaging + local-FS media. No `.env` needed. |
| `postgres-redis.yml` | 3 | Minimal served topology: relay + Postgres + Redis, local-FS media. |
| `compose.yml` | 5 | **Reference production stack**: adds MinIO/S3 media and the git object store. Pairs with `compose.caddy.yml` (TLS) and `compose.dev.yml` (exposed ports, Adminer, Prometheus). |
| `zmq-pair.yml` | 4 | **Experimental**: two relays in a ZeroMQ static mesh over shared Postgres. |

`compose.yml` remains the file `run.sh` drives and the one to use in
production. The others are narrower starting points.

Narrative walkthroughs of the Solo and ZMQ-pair shapes — env var reference,
data layout, backups, scope cuts — live in
[`docs/deploy-solo-quickstart.md`](../../docs/deploy-solo-quickstart.md).

### Solo in one command

```bash
mkdir -p data && sudo chown 1000:1000 data   # image runs as uid 1000
docker compose -f solo.yml up -d
```

### ZeroMQ pair

Note that ZeroMQ replaces Redis for **pub/sub fan-out only** — the relay's
shared state (replay guard, rate limiter) still needs Redis, and the config
layer rejects `BUZZ_STATE_BACKEND=inproc` whenever `BUZZ_ZMQ_PEERS` is set. So
`zmq-pair.yml` still runs Redis by design. Soak a build against this topology
with `./scripts/zmq-soak.sh` before trusting it.

## Quick start

```bash
cd deploy/compose
cp .env.example .env
$EDITOR .env       # replace every CHANGE_ME value
./run.sh start
```

For a public VPS with automatic Let's Encrypt certificates:

```bash
cd deploy/compose
BUZZ_COMPOSE_TLS=true ./run.sh start
```

The bootstrap script should eventually replace manual `.env` editing for normal
users. It is responsible for generating stable secrets and, optionally, an owner
keypair.

## Production notes

- Requires Docker Compose v2.24.4 or newer; the TLS override uses Compose's
  `!reset` tag to remove the direct relay port when Caddy terminates HTTPS.
- Default `BUZZ_IMAGE` tracks `ghcr.io/block/buzz:main` for early testing. Pin it to `ghcr.io/block/buzz:sha-<7>` or a semver release tag for production once available.
- Keep `BUZZ_RELAY_PRIVATE_KEY`, `BUZZ_GIT_HOOK_HMAC_SECRET`, database/Redis,
  and S3 secrets stable across restarts.
- `RELAY_OWNER_PUBKEY` is intentionally not prefixed with `BUZZ_`; it must be a
  64-character hex Nostr pubkey when closed relay mode is enabled.
- `BUZZ_AUTO_MIGRATE` is opt-in. Set `BUZZ_AUTO_MIGRATE=true` or run
  `buzz-admin migrate` before starting the relay when bootstrapping a fresh
  database. Auto-migration requires an image that includes embedded SQLx
  migrations.
- The stack uses Postgres, Redis, MinIO, and a git data volume because
  those are real Buzz dependencies today. Minimal mode can simplify this later.

Run `./run.sh backup-hint` for the backup checklist.

## Validation

Before sharing an install link publicly, verify a fresh install with:

```bash
cd deploy/compose
cp .env.example .env
$EDITOR .env
./run.sh config
./run.sh start
curl -fsS "http://127.0.0.1:$(grep -E '^BUZZ_HTTP_PORT=' .env | cut -d= -f2-)/_liveness"
./run.sh status
```

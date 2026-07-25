# Deploying Buzz — Solo quickstart (and two bigger shapes)

Three ways to run a relay, in increasing order of moving parts:

| Story | Services | When to use it |
|---|---|---|
| [1. Bare binary, Solo profile](#1-bare-binary-solo-profile) | none | Laptop, Raspberry Pi, a single VPS for you and a few friends |
| [2. Docker Compose, Solo profile](#2-docker-compose-solo-profile) | none (one container) | Same, but you would rather manage a container than a systemd unit |
| [3. Compose, ZMQ pair + Postgres](#3-docker-compose-zmq-pair--postgres) | Postgres, Redis | Homelab / small cluster: two relay nodes, no single point of failure in the app tier |

Story 3 is **experimental** — multi-node ZeroMQ is the newest transport in the
tree. See [Multi-node caveats](#multi-node-caveats) before relying on it.

---

## 1. Bare binary, Solo profile

The Solo profile is the zero-dependency deployment: **SQLite** storage,
**in-process** messaging, **local-filesystem** media. No Postgres, no Redis, no
S3, no message broker. One process, one directory.

```bash
cargo build --release -p buzz-relay
./target/release/buzz-relay --profile solo
```

That is the whole thing. On first boot the relay creates `./data/buzz.db`,
applies migrations automatically, and starts serving:

| Port | Purpose |
|---|---|
| 3000 | WebSocket + REST (`BUZZ_BIND_ADDR`) |
| 8080 | `/_liveness`, `/_readiness` (`BUZZ_HEALTH_PORT`) |
| 9102 | Prometheus `/metrics` (`BUZZ_METRICS_PORT`) |

Verify it came up:

```bash
curl -fsS http://127.0.0.1:8080/_readiness && echo READY
```

`--profile solo` is exactly equivalent to setting `BUZZ_PROFILE=solo`; the flag
is mapped onto the environment variable before config loads. Either way it only
changes **defaults** — every individual `BUZZ_*` variable still overrides it, so
you can run "Solo but with Postgres" or "Solo but with S3 media" by setting the
one variable you want to differ.

### Running it as a service

```ini
# /etc/systemd/system/buzz.service
[Unit]
Description=Buzz relay (Solo profile)
After=network-online.target

[Service]
User=buzz
WorkingDirectory=/var/lib/buzz
Environment=BUZZ_PROFILE=solo
Environment=RELAY_URL=wss://buzz.example.com
ExecStart=/usr/local/bin/buzz-relay
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Put a TLS-terminating reverse proxy (Caddy, nginx, Traefik) in front of port
3000 — the relay speaks plaintext HTTP/WS and does not terminate TLS itself.
`deploy/compose/Caddyfile` is a working example.

---

## 2. Docker Compose, Solo profile

```bash
cd deploy/compose
mkdir -p data && sudo chown 1000:1000 data
docker compose -f solo.yml up -d
curl -fsS http://127.0.0.1:8080/_readiness && echo READY
```

`solo.yml` is a single service with no `.env` file and no external
dependencies. The `chown` is needed once because the runtime image runs as
uid/gid 1000 and Docker creates a fresh bind mount owned by root; if you would
rather not think about ownership, switch to the named-volume stanza commented
at the bottom of the file.

Pin the image for anything you care about — `:main` tracks HEAD:

```bash
BUZZ_IMAGE=ghcr.io/block/buzz:sha-1234567 docker compose -f solo.yml up -d
```

The image is built from the repository-root [`Dockerfile`](../Dockerfile) and
published as `ghcr.io/block/buzz`. To build it yourself:

```bash
docker build -t buzz-relay:local .
```

---

## 3. Docker Compose, ZMQ pair + Postgres

Two relay nodes sharing one Postgres event store, fanning out to each other
over a ZeroMQ static mesh instead of Redis pub/sub.

```bash
cd deploy/compose
cp .env.example .env
$EDITOR .env        # set POSTGRES_PASSWORD and REDIS_PASSWORD at minimum
docker compose -f zmq-pair.yml up -d
```

Node A serves on host port 3000, node B on 3001. Put a load balancer in front
of both.

### Why Redis is still in the ZMQ stack

This surprises people, so it is worth stating plainly: **ZeroMQ replaces Redis
for pub/sub fan-out only.** The relay's *shared state* — the NIP-98 replay
guard and the rate limiter — are cross-node correctness fences, and they still
need somewhere shared to live. The config layer refuses to let you get this
wrong:

```
BUZZ_ZMQ_PEERS set, BUZZ_STATE_BACKEND unset    -> startup error (choose explicitly)
BUZZ_ZMQ_PEERS set, BUZZ_STATE_BACKEND=inproc   -> startup error (single-node only)
```

So a multi-node ZMQ deployment is **zmq messaging + redis state**. Dropping
Redis entirely is a single-node (Solo) story, not a cluster one.

### Topology

Each node binds one PUB socket and subscribes to every *other* node's PUB
endpoint. A node short-circuits its own publishes in-process, so it must not
list itself as a peer:

```
relay-a   BUZZ_ZMQ_BIND=tcp://0.0.0.0:5559   BUZZ_ZMQ_PEERS=tcp://relay-b:5559
relay-b   BUZZ_ZMQ_BIND=tcp://0.0.0.0:5559   BUZZ_ZMQ_PEERS=tcp://relay-a:5559
```

Adding a third node means editing `BUZZ_ZMQ_PEERS` on all three: v1 is a static
mesh with **no peer discovery**, no broker mode, and no CURVE encryption. Run
the mesh on a trusted network. Clusters that outgrow a hand-maintained peer
list should stay on the Redis transport.

### Multi-node caveats

- **Same public origin on every node.** The community boundary is host-derived,
  so `RELAY_URL` must be identical across nodes — they are one community served
  by two processes, not two communities.
- **One migrator.** `zmq-pair.yml` sets `BUZZ_AUTO_MIGRATE=true` on node A and
  `false` on node B, and makes B wait for A to become healthy. Two relays racing
  the same migration against one database is how you get a half-applied schema.
- **Fan-out is at-most-once.** Same contract as the Redis pub/sub it replaces:
  if a node is down, live fan-out to it is lost. Those events are still durably
  committed to Postgres, and a reconnecting client backfills them from history.
  "At-most-once" describes the *live delivery path*, not storage.
- **Shared storage is mandatory.** Both nodes must point at the same Postgres.
  Two nodes with two SQLite files is two separate relays that happen to gossip.

Exercise a build against this topology with the soak harness before trusting
it:

```bash
./scripts/zmq-soak.sh --duration 60
```

See [`scripts/zmq-soak.sh`](../scripts/zmq-soak.sh) for what it asserts.

---

## Environment variable reference (Solo profile)

Everything below has a working default; the profile exists so you can run with
none of them set. Listed values are the **Solo** defaults, which differ from
the served-profile defaults where noted.

### Selecting the profile

| Variable | Default | Notes |
|---|---|---|
| `BUZZ_PROFILE` | unset | `solo` selects the whole zero-service backend trio. Same as `--profile solo`. |

### Storage and data

| Variable | Solo default | Notes |
|---|---|---|
| `BUZZ_DB_BACKEND` | `sqlite` | `postgres` on the served profile. |
| `BUZZ_SQLITE_PATH` | `./data/buzz.db` | SQLite backend only. |
| `DATABASE_URL` | — | Postgres backend only; ignored under SQLite. |
| `BUZZ_AUTO_MIGRATE` | `true` | Solo migrates on boot (first boot has no schema at all). Served deployments default to opt-in. |
| `BUZZ_MEDIA_BACKEND` | `local` | `s3` on the served profile. |
| `BUZZ_MEDIA_PATH` | `./data/media` | Local-FS media backend only. |

### Messaging and shared state

| Variable | Solo default | Notes |
|---|---|---|
| `BUZZ_MESSAGING_BACKEND` | `inproc` | `inproc` \| `redis` \| `zmq`. `redis` on the served profile. |
| `BUZZ_STATE_BACKEND` | `inproc` | `inproc` \| `redis`. Must be `redis` once `BUZZ_ZMQ_PEERS` is set. |
| `REDIS_URL` | `redis://localhost:6379` | Only read by the Redis backends. |
| `BUZZ_ZMQ_BIND` | `tcp://0.0.0.0:5559` | This node's own PUB endpoint. |
| `BUZZ_ZMQ_PEERS` | empty | Comma-separated PUB endpoints of the *other* nodes. Never list yourself. |

### Network and identity

| Variable | Default | Notes |
|---|---|---|
| `BUZZ_BIND_ADDR` | `0.0.0.0:3000` | WebSocket + REST. |
| `BUZZ_HEALTH_PORT` | `8080` | `/_liveness`, `/_readiness`. |
| `BUZZ_METRICS_PORT` | `9102` | Prometheus `/metrics`. |
| `RELAY_URL` | `ws://localhost:3000` | Derives the community boundary. Set to your real public URL. |
| `BUZZ_MEDIA_BASE_URL` | `http://localhost:3000/media` | Public URL clients fetch blobs from. |
| `RELAY_OWNER_PUBKEY` | unset | 64-char hex Nostr pubkey. Deliberately **not** `BUZZ_`-prefixed. |
| `BUZZ_RELAY_PRIVATE_KEY` | unset | Relay identity. Generate once and keep it stable. |
| `BUZZ_CORS_ORIGINS` | — | Add your web client's origin when serving browsers. |

### Capabilities

| Variable | Solo default | Notes |
|---|---|---|
| `BUZZ_CAPABILITY_GIT` | `false` | On by default off-Solo. Git hosting uses an S3-backed object store, so it needs S3 media — not available on Solo. |
| `BUZZ_CAPABILITY_HUDDLE_AUDIO` | see config | Huddle audio SFU. |

Full reference for every variable: [`.env.example`](../.env.example).

---

## Data layout

Everything a Solo relay owns lives under one directory:

```
data/
  buzz.db              # SQLite event store — the whole relay state
  buzz.db-wal          # write-ahead log  (exists while running)
  buzz.db-shm          # shared memory    (exists while running)
  media/               # local-FS media (Blossom blobs)
    blobs/{sha256}.{ext}
    blobs/{sha256}.thumb.jpg
    _meta/{community}/{sha256}.json
    _uploads/{community}/{sha256}/{event_id}.json
```

Back up `data/` and you have backed up the relay. There is no external state.

---

## Backups

SQLite runs in WAL mode, which means **copying `buzz.db` alone while the relay
is running gives you a corrupt or stale backup** — the recent writes are in
`buzz.db-wal`. Pick one of these instead.

### Online backup (relay keeps running) — preferred

`sqlite3 .backup` takes a consistent snapshot of a live database:

```bash
sqlite3 data/buzz.db ".backup '/backups/buzz-$(date +%F).db'"
tar czf /backups/media-$(date +%F).tar.gz -C data media
```

### Offline copy (relay stopped)

Stop the relay first, then copy the whole directory — including the `-wal` and
`-shm` files if they are still present:

```bash
systemctl stop buzz          # or: docker compose -f solo.yml stop
cp -a data /backups/buzz-$(date +%F)
systemctl start buzz
```

### Restoring

Stop the relay, replace `data/`, start it. Restore the media directory from the
same point in time as the database — an event referencing a blob that is not
there renders as a broken attachment.

### Verifying a backup

A backup you have never restored is a hypothesis:

```bash
sqlite3 /backups/buzz-2026-07-24.db "PRAGMA integrity_check;"   # expect: ok
```

### Postgres deployments

Stories 2 and 3 above use SQLite and Postgres respectively. For the Postgres
stacks use `pg_dump`/`pg_basebackup` as usual; `deploy/compose/run.sh
backup-hint` prints the checklist for the full production stack.

---

## What Solo deliberately does not do

These are **config-gated off**, not missing by accident. They stay fully
functional on the Postgres/Redis profile; the Solo profile cuts them to keep
the zero-dependency promise honest.

- **Push notifications.** The push gateway and its lease/wake-outbox machinery
  are the most Postgres-specific module in the storage layer. Mobile push for a
  single self-hosted relay is a non-goal for now — Postgres profile only.
- **Mesh / tunnel / device pair-relay.** Inherently multi-relay, with
  Redis-fenced session ownership. Solo is one relay, so there is nothing to
  mesh. `BUZZ_MESH=on` is rejected outright with SQLite storage or a non-Redis
  messaging backend.
- **Read replicas and the replica fence.** Postgres-only by nature.
- **Multiple relay replicas.** One SQLite file means one writer process. Scaling
  out means moving to Postgres — which is story 3, not a Solo tuning knob.
- **Git hosting.** The git object store is S3-backed, so `BUZZ_CAPABILITY_GIT`
  defaults off under Solo.

Reversing any of these is a config change plus the service it needs — none of
them were deleted.

---

## See also

- [`deploy/compose/README.md`](../deploy/compose/README.md) — the full
  production stack (Postgres + Redis + MinIO + Caddy TLS)
- [`scripts/zmq-soak.sh`](../scripts/zmq-soak.sh) — two-node ZMQ soak harness
- [`ARCHITECTURE.md`](../ARCHITECTURE.md) — system design and subsystem boundaries

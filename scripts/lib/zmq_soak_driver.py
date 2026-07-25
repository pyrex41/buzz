#!/usr/bin/env python3
"""Traffic driver for the two-node ZeroMQ soak (scripts/zmq-soak.sh).

Speaks Nostr over WebSocket with no dependencies beyond the standard library
(see scripts/lib/nostr_ws.py for why). The shell script owns process
lifecycle — booting relays, killing and restarting node B — and calls the
subcommands here to generate traffic and assert on what arrives.

Subcommands
-----------
steady        Cross-node fan-out in both directions for a fixed duration.
publish       Publish N events at a node, recording their ids to a file.
              Used to generate traffic while the peer is down.
verify-store  Assert previously published events are readable from a node's
              event store. This is what distinguishes "lost from live pub/sub"
              (expected, at-most-once) from "lost from the database" (a bug).
resume        Assert A->B live fan-out works again after B restarts.

Every subcommand prints a one-line JSON result to stdout and exits non-zero on
assertion failure, so the shell script can both parse and gate on it.

Protocol notes that shape this code
-----------------------------------
* NIP-42 AUTH is mandatory and cannot be disabled. The relay sends an AUTH
  challenge on connect and closes the socket if it is not answered within 5
  seconds, so authenticate before doing anything else.
* The community boundary is derived from the HTTP ``Host`` header. Node B
  listens on a different port but must be addressed with the *canonical*
  host (--host) or it resolves to a different community — or to none at all,
  which is a 404 before the upgrade.
* Events are kind 1 (text note): globally scoped, so no channel or membership
  setup is needed, and *persistent*, so the store-durability assertion in
  ``verify-store`` is meaningful. An ephemeral kind would fan out just as well
  but would prove nothing about durability.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nostr_ws import (  # noqa: E402
    PrivateKey,
    WebSocket,
    build_event,
    http_post_json,
)

KIND_AUTH = 22242
KIND_NOTE = 1


def log(msg: str) -> None:
    print(f"[driver] {msg}", file=sys.stderr, flush=True)


def emit(obj: dict) -> None:
    print(json.dumps(obj, separators=(",", ":")), flush=True)


# ─── connection helpers ─────────────────────────────────────────────────────


def connect(url: str, host: str, key: PrivateKey, label: str) -> WebSocket:
    """Open a WebSocket and complete the NIP-42 AUTH handshake."""
    ws = WebSocket(url, timeout=15.0, host_header=host)

    # The relay pushes ["AUTH", <challenge>] immediately on upgrade.
    challenge = None
    deadline = time.time() + 5.0
    while time.time() < deadline:
        msg = ws.recv_json(timeout=max(0.1, deadline - time.time()))
        if msg and isinstance(msg, list) and msg[0] == "AUTH":
            challenge = msg[1]
            break
    if challenge is None:
        ws.close()
        raise RuntimeError(f"{label}: no AUTH challenge from {url}")

    # The `relay` tag must match the tenant host the relay resolved us to,
    # which is the Host header we sent — not the URL we dialed.
    scheme = "ws"
    auth_event = build_event(
        key,
        KIND_AUTH,
        "",
        [["challenge", challenge], ["relay", f"{scheme}://{host}"]],
    )
    ws.send_json(["AUTH", auth_event])

    deadline = time.time() + 10.0
    while time.time() < deadline:
        msg = ws.recv_json(timeout=max(0.1, deadline - time.time()))
        if msg and isinstance(msg, list) and msg[0] == "OK" and msg[1] == auth_event["id"]:
            if msg[2] is True:
                return ws
            ws.close()
            raise RuntimeError(f"{label}: AUTH rejected: {msg[3] if len(msg) > 3 else ''}")
    ws.close()
    raise RuntimeError(f"{label}: AUTH timed out against {url}")


class Subscriber:
    """A WS connection with an open REQ, draining live events on a thread."""

    def __init__(self, url: str, host: str, key: PrivateKey, label: str, marker: str):
        self.label = label
        self.marker = marker
        self.ws = connect(url, host, key, label)
        self.seen: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.eose = threading.Event()
        self.sub_id = f"soak-{label}"

        # `since` trims stored history; kind 1 is persistent, so without it the
        # REQ replays the backlog before going live. The p-gate also requires
        # an explicit `kinds` — an open-ended filter is rejected outright.
        self.ws.send_json(
            ["REQ", self.sub_id, {"kinds": [KIND_NOTE], "since": int(time.time()) - 1}]
        )
        self._thread = threading.Thread(target=self._drain, daemon=True)
        self._thread.start()

    def _drain(self) -> None:
        while not self._stop.is_set():
            msg = self.ws.recv_json(timeout=0.5)
            if msg is None:
                continue
            if not isinstance(msg, list) or not msg:
                continue
            if msg[0] == "EOSE" and len(msg) > 1 and msg[1] == self.sub_id:
                self.eose.set()
            elif msg[0] == "EVENT" and len(msg) > 2:
                event = msg[2]
                if isinstance(event, dict) and self.marker in (event.get("content") or ""):
                    with self._lock:
                        self.seen.add(event["content"])
            elif msg[0] == "CLOSED":
                log(f"{self.label}: subscription CLOSED: {msg}")
                return

    def wait_ready(self, timeout: float = 15.0) -> bool:
        return self.eose.wait(timeout)

    def snapshot(self) -> set[str]:
        with self._lock:
            return set(self.seen)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)
        self.ws.close()


def publish_note(ws: WebSocket, key: PrivateKey, content: str) -> str:
    event = build_event(key, KIND_NOTE, content)
    ws.send_json(["EVENT", event])
    return event["id"]


def drain_oks(ws: WebSocket, budget: float = 0.02) -> list:
    """Non-blocking-ish sweep of pending OK frames on a publisher socket."""
    out = []
    deadline = time.time() + budget
    while time.time() < deadline:
        msg = ws.recv_json(timeout=max(0.001, deadline - time.time()))
        if msg is None:
            break
        out.append(msg)
    return out


# ─── phases ─────────────────────────────────────────────────────────────────


def cmd_steady(args: argparse.Namespace) -> int:
    """Publish from both nodes, assert each side observes the other's events."""
    key = PrivateKey.from_hex(args.key) if args.key else PrivateKey()
    marker = args.marker

    sub_a = Subscriber(args.node_a, args.host, key, "sub-a", marker)
    sub_b = Subscriber(args.node_b, args.host, key, "sub-b", marker)
    if not sub_a.wait_ready() or not sub_b.wait_ready():
        log("subscriptions did not reach EOSE")
        sub_a.stop()
        sub_b.stop()
        emit({"phase": "steady", "ok": False, "error": "no EOSE"})
        return 1

    pub_a = connect(args.node_a, args.host, key, "pub-a")
    pub_b = connect(args.node_b, args.host, key, "pub-b")

    # ZMQ SUB sockets drop messages published before the subscription is
    # established ("slow joiner"). Warm up and discard before counting, so we
    # measure steady-state delivery rather than connection setup.
    log(f"warmup {args.warmup}s")
    warm_deadline = time.time() + args.warmup
    while time.time() < warm_deadline:
        publish_note(pub_a, key, f"{marker}-warmup-a-{time.time()}")
        publish_note(pub_b, key, f"{marker}-warmup-b-{time.time()}")
        drain_oks(pub_a)
        drain_oks(pub_b)
        time.sleep(0.2)

    sent_a: list[str] = []
    sent_b: list[str] = []
    interval = 1.0 / max(args.rate, 0.001)
    start = time.time()
    end = start + args.duration
    seq = 0
    log(f"steady-state {args.duration}s at {args.rate}/s each direction")
    while time.time() < end:
        seq += 1
        ca = f"{marker}-a2b-{seq}"
        cb = f"{marker}-b2a-{seq}"
        publish_note(pub_a, key, ca)
        publish_note(pub_b, key, cb)
        sent_a.append(ca)
        sent_b.append(cb)
        drain_oks(pub_a)
        drain_oks(pub_b)
        sleep_for = interval - (time.time() - (start + (seq - 1) * interval))
        if sleep_for > 0:
            time.sleep(min(sleep_for, interval))

    # Give in-flight fan-out time to land before snapshotting.
    time.sleep(args.settle)
    seen_by_b = sub_b.snapshot()
    seen_by_a = sub_a.snapshot()
    sub_a.stop()
    sub_b.stop()
    pub_a.close()
    pub_b.close()

    a2b = sum(1 for c in sent_a if c in seen_by_b)
    b2a = sum(1 for c in sent_b if c in seen_by_a)
    a2b_rate = a2b / len(sent_a) if sent_a else 0.0
    b2a_rate = b2a / len(sent_b) if sent_b else 0.0
    ok = (
        bool(sent_a)
        and bool(sent_b)
        and a2b_rate >= args.min_delivery
        and b2a_rate >= args.min_delivery
    )
    emit(
        {
            "phase": "steady",
            "ok": ok,
            "published_a": len(sent_a),
            "published_b": len(sent_b),
            "a_to_b_delivered": a2b,
            "b_to_a_delivered": b2a,
            "a_to_b_rate": round(a2b_rate, 4),
            "b_to_a_rate": round(b2a_rate, 4),
            "min_delivery": args.min_delivery,
            "elapsed_s": round(time.time() - start, 2),
        }
    )
    return 0 if ok else 1


def cmd_publish(args: argparse.Namespace) -> int:
    """Publish N events at one node and record their contents/ids."""
    key = PrivateKey.from_hex(args.key) if args.key else PrivateKey()
    ws = connect(args.node, args.host, key, "publisher")
    records = []
    for i in range(args.count):
        content = f"{args.marker}-{args.label}-{i}"
        eid = publish_note(ws, key, content)
        records.append({"id": eid, "content": content})
        time.sleep(1.0 / max(args.rate, 0.001))

    # Collect OK acks so we know the relay accepted them (they must be durable
    # even though no live subscriber on the downed peer will ever see them).
    accepted = 0
    deadline = time.time() + 5.0
    ids = {r["id"] for r in records}
    acked: set[str] = set()
    while time.time() < deadline and len(acked) < len(records):
        msg = ws.recv_json(timeout=max(0.1, deadline - time.time()))
        if msg and isinstance(msg, list) and msg[0] == "OK" and msg[1] in ids:
            acked.add(msg[1])
            if msg[2] is True:
                accepted += 1
            else:
                log(f"event rejected: {msg}")
    ws.close()

    if args.out:
        with open(args.out, "w") as fh:
            json.dump(records, fh)
    ok = accepted == len(records)
    emit(
        {
            "phase": "publish",
            "ok": ok,
            "label": args.label,
            "published": len(records),
            "accepted": accepted,
            "out": args.out,
        }
    )
    return 0 if ok else 1


def cmd_verify_store(args: argparse.Namespace) -> int:
    """Assert recorded events are readable from a node's store over POST /query.

    This is the other half of the at-most-once contract: live fan-out to a
    downed node is lost, but the events were committed and any client can
    still read them back.
    """
    with open(args.ids_file) as fh:
        records = json.load(fh)
    key = PrivateKey.from_hex(args.key) if args.key else PrivateKey()

    # POST /query is the HTTP form of a Nostr REQ filter. `kinds` is mandatory:
    # an open-ended filter trips the relay's p-gate and returns 403.
    status, body = http_post_json(
        args.node,
        {"kinds": [KIND_NOTE], "ids": [r["id"] for r in records], "limit": 500},
        host_header=args.host,
        extra_headers={"X-Pubkey": key.pubkey_hex},
    )
    found: set[str] = set()
    if isinstance(body, list):
        for ev in body:
            if isinstance(ev, dict) and ev.get("id"):
                found.add(ev["id"])
    elif isinstance(body, dict):
        for ev in body.get("events", []) or []:
            if isinstance(ev, dict) and ev.get("id"):
                found.add(ev["id"])

    want = {r["id"] for r in records}
    missing = sorted(want - found)
    ok = status == 200 and not missing
    emit(
        {
            "phase": "verify-store",
            "ok": ok,
            "http_status": status,
            "expected": len(want),
            "found": len(want & found),
            "missing": len(missing),
            "note": "events published during the peer outage must be durable",
            "body_preview": (body if not isinstance(body, (list, dict)) else None),
        }
    )
    return 0 if ok else 1


def cmd_resume(args: argparse.Namespace) -> int:
    """Assert A->B live fan-out resumes after node B comes back."""
    key = PrivateKey.from_hex(args.key) if args.key else PrivateKey()
    marker = args.marker
    sub_b = Subscriber(args.node_b, args.host, key, "sub-b", marker)
    if not sub_b.wait_ready():
        sub_b.stop()
        emit({"phase": "resume", "ok": False, "error": "subscriber on B never reached EOSE"})
        return 1

    pub_a = connect(args.node_a, args.host, key, "pub-a")

    # ZMQ reconnect uses exponential backoff (1s -> 30s), so a restarted peer
    # is not instantly re-meshed. Retry until the deadline instead of asserting
    # on the first message: the contract is "flow resumes", not "resumes now".
    deadline = time.time() + args.timeout
    seq = 0
    delivered = 0
    sent: list[str] = []
    while time.time() < deadline and delivered == 0:
        seq += 1
        content = f"{marker}-resume-{seq}"
        publish_note(pub_a, key, content)
        sent.append(content)
        drain_oks(pub_a)
        time.sleep(args.interval)
        delivered = len(sub_b.snapshot() & set(sent))

    time.sleep(args.settle)
    delivered = len(sub_b.snapshot() & set(sent))
    sub_b.stop()
    pub_a.close()

    ok = delivered > 0
    emit(
        {
            "phase": "resume",
            "ok": ok,
            "published_after_restart": len(sent),
            "delivered_to_b": delivered,
            "recovery_s": round(len(sent) * args.interval, 1),
            "note": "at-most-once: events published during the outage are NOT replayed",
        }
    )
    return 0 if ok else 1


# ─── CLI ────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description="ZMQ two-node soak traffic driver")
    ap.add_argument("--host", default="localhost:3000", help="canonical community host")
    ap.add_argument("--key", default=None, help="32-byte hex secret key")
    ap.add_argument("--marker", default="soak", help="unique run marker in event content")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("steady", help="bidirectional steady-state fan-out")
    p.add_argument("--node-a", required=True)
    p.add_argument("--node-b", required=True)
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("--rate", type=float, default=5.0, help="events/sec per direction")
    p.add_argument("--warmup", type=float, default=2.0)
    p.add_argument("--settle", type=float, default=3.0)
    p.add_argument("--min-delivery", type=float, default=0.99)
    p.set_defaults(func=cmd_steady)

    p = sub.add_parser("publish", help="publish N events at one node")
    p.add_argument("--node", required=True)
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--rate", type=float, default=5.0)
    p.add_argument("--label", default="outage")
    p.add_argument("--out", default=None, help="write [{id,content}] JSON here")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("verify-store", help="assert events are durable in the store")
    p.add_argument("--node", required=True, help="http:// URL of the node to query")
    p.add_argument("--ids-file", required=True)
    p.set_defaults(func=cmd_verify_store)

    p = sub.add_parser("resume", help="assert A->B fan-out resumes after restart")
    p.add_argument("--node-a", required=True)
    p.add_argument("--node-b", required=True)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--settle", type=float, default=3.0)
    p.set_defaults(func=cmd_resume)

    args = ap.parse_args()
    try:
        return args.func(args)
    except Exception as exc:  # surface as a parseable failure, not a traceback
        log(f"{type(exc).__name__}: {exc}")
        emit({"phase": args.cmd, "ok": False, "error": f"{type(exc).__name__}: {exc}"})
        return 1


if __name__ == "__main__":
    sys.exit(main())

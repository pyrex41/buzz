"""Dependency-free Nostr-over-WebSocket client for local test harnesses.

Why this exists: the ZMQ soak harness (``scripts/zmq-soak.sh``) has to observe
*live pub/sub fan-out* across two relay nodes. Reading history back over
``POST /query`` would prove only that both nodes share a database — it would
pass even with the mesh completely broken. Observing fan-out requires a real
WebSocket subscriber, and the soak has to run on a bare box: no ``websocat``,
no ``pip install``, no compiled test client.

So this module implements, against the Python standard library only:

* BIP-340 Schnorr signing over secp256k1 (``PrivateKey``) — enough to mint
  valid Nostr events. Correctness is pinned by the official BIP-340 test
  vectors in ``scripts/lib/test_nostr_ws.py``.
* A minimal RFC 6455 client (``WebSocket``) — handshake, masked client frames,
  fragmentation, ping/pong, close. Pinned by a loopback test against a
  hand-rolled server in the same test file.

Neither is production crypto or a production WebSocket stack. The signer is
straightforward constant-time-agnostic Python and makes no attempt to resist
side channels; the socket layer speaks only the subset of RFC 6455 the relay
uses. This is test tooling, deliberately kept in ``scripts/lib`` rather than
shipped anywhere a real client could import it.

Run the self-tests with::

    python3 scripts/lib/test_nostr_ws.py
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import socket
import struct
import time
from typing import Any, Iterator

# ─── secp256k1 / BIP-340 ────────────────────────────────────────────────────
# Reference implementation shape, following BIP-340's Python appendix. Field
# and group order for secp256k1.

P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
G = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)


def _tagged_hash(tag: str, msg: bytes) -> bytes:
    tag_hash = hashlib.sha256(tag.encode()).digest()
    return hashlib.sha256(tag_hash + tag_hash + msg).digest()


def _point_add(
    p1: tuple[int, int] | None, p2: tuple[int, int] | None
) -> tuple[int, int] | None:
    if p1 is None:
        return p2
    if p2 is None:
        return p1
    if p1[0] == p2[0] and p1[1] != p2[1]:
        return None
    if p1 == p2:
        lam = (3 * p1[0] * p1[0] * pow(2 * p1[1], P - 2, P)) % P
    else:
        lam = ((p2[1] - p1[1]) * pow(p2[0] - p1[0], P - 2, P)) % P
    x3 = (lam * lam - p1[0] - p2[0]) % P
    return (x3, (lam * (p1[0] - x3) - p1[1]) % P)


def _point_mul(p: tuple[int, int] | None, n: int) -> tuple[int, int] | None:
    r = None
    for i in range(256):
        if (n >> i) & 1:
            r = _point_add(r, p)
        p = _point_add(p, p)
    return r


def _lift_x(x: int) -> tuple[int, int] | None:
    """Recover the even-Y point with the given x coordinate (BIP-340)."""
    if x >= P:
        return None
    y_sq = (pow(x, 3, P) + 7) % P
    y = pow(y_sq, (P + 1) // 4, P)
    if pow(y, 2, P) != y_sq:
        return None
    return (x, y if y % 2 == 0 else P - y)


class PrivateKey:
    """A secp256k1 secret key that can produce BIP-340 Schnorr signatures."""

    def __init__(self, secret: bytes | None = None) -> None:
        if secret is None:
            secret = secrets.token_bytes(32)
        if len(secret) != 32:
            raise ValueError("secret key must be 32 bytes")
        d0 = int.from_bytes(secret, "big")
        if not (1 <= d0 < N):
            raise ValueError("secret key out of range")
        self._d0 = d0
        self._secret = secret
        point = _point_mul(G, d0)
        assert point is not None  # d0 in [1, N) => never the point at infinity
        self._point = point

    @classmethod
    def from_hex(cls, hex_str: str) -> "PrivateKey":
        return cls(bytes.fromhex(hex_str))

    @property
    def hex(self) -> str:
        return self._secret.hex()

    @property
    def pubkey_hex(self) -> str:
        """x-only public key (32 bytes hex) — the Nostr ``pubkey`` field."""
        return format(self._point[0], "064x")

    def sign(self, msg: bytes, aux_rand: bytes | None = None) -> str:
        """BIP-340 Schnorr signature over a 32-byte message, hex encoded."""
        if len(msg) != 32:
            raise ValueError("BIP-340 message must be 32 bytes")
        if aux_rand is None:
            aux_rand = secrets.token_bytes(32)
        if len(aux_rand) != 32:
            raise ValueError("aux_rand must be 32 bytes")

        # Negate the secret if P has odd Y, so the x-only pubkey is canonical.
        d = self._d0 if self._point[1] % 2 == 0 else N - self._d0
        px = self._point[0]

        t = d ^ int.from_bytes(_tagged_hash("BIP0340/aux", aux_rand), "big")
        rand = _tagged_hash(
            "BIP0340/nonce",
            t.to_bytes(32, "big") + px.to_bytes(32, "big") + msg,
        )
        k0 = int.from_bytes(rand, "big") % N
        if k0 == 0:
            raise RuntimeError("failed to generate nonce (k0 == 0)")

        r_point = _point_mul(G, k0)
        assert r_point is not None
        k = k0 if r_point[1] % 2 == 0 else N - k0
        rx = r_point[0]

        e = (
            int.from_bytes(
                _tagged_hash(
                    "BIP0340/challenge",
                    rx.to_bytes(32, "big") + px.to_bytes(32, "big") + msg,
                ),
                "big",
            )
            % N
        )
        sig = rx.to_bytes(32, "big") + ((k + e * d) % N).to_bytes(32, "big")
        return sig.hex()


def schnorr_verify(msg: bytes, pubkey_hex: str, sig_hex: str) -> bool:
    """Verify a BIP-340 signature. Used by the self-tests, not the harness."""
    if len(msg) != 32:
        return False
    sig = bytes.fromhex(sig_hex)
    if len(sig) != 64:
        return False
    pubkey = int(pubkey_hex, 16)
    point = _lift_x(pubkey)
    if point is None:
        return False
    r = int.from_bytes(sig[:32], "big")
    s = int.from_bytes(sig[32:], "big")
    if r >= P or s >= N:
        return False
    e = (
        int.from_bytes(
            _tagged_hash(
                "BIP0340/challenge",
                sig[:32] + pubkey.to_bytes(32, "big") + msg,
            ),
            "big",
        )
        % N
    )
    r_point = _point_add(_point_mul(G, s), _point_mul(point, N - e))
    if r_point is None or r_point[1] % 2 != 0 or r_point[0] != r:
        return False
    return True


# ─── Nostr events ───────────────────────────────────────────────────────────


def event_id(pubkey: str, created_at: int, kind: int, tags: list, content: str) -> str:
    """NIP-01 event id: sha256 over the canonical serialization array."""
    serialized = json.dumps(
        [0, pubkey, created_at, kind, tags, content],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_event(
    key: PrivateKey,
    kind: int,
    content: str,
    tags: list | None = None,
    created_at: int | None = None,
) -> dict:
    """Build a fully signed Nostr event."""
    tags = tags or []
    created_at = created_at if created_at is not None else int(time.time())
    pubkey = key.pubkey_hex
    eid = event_id(pubkey, created_at, kind, tags, content)
    return {
        "id": eid,
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": key.sign(bytes.fromhex(eid)),
    }


# ─── Minimal RFC 6455 client ────────────────────────────────────────────────

_OP_CONT, _OP_TEXT, _OP_BIN, _OP_CLOSE, _OP_PING, _OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA
_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketError(RuntimeError):
    pass


class WebSocket:
    """A blocking WebSocket client speaking the subset of RFC 6455 we need."""

    def __init__(
        self, url: str, timeout: float = 10.0, host_header: str | None = None
    ) -> None:
        """Connect to ``url``.

        ``host_header`` overrides the HTTP ``Host:`` sent during the upgrade.
        The relay resolves a connection's community from that header, so a
        second node listening on a different port must still be addressed as
        the community's canonical host to land in the same community.
        """
        host, port, path = _parse_ws_url(url)
        self.url = url
        self.host_header = host_header or f"{host}:{port}"
        self._buf = b""
        self._closed = False
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._handshake(path)

    # -- handshake --
    def _handshake(self, path: str) -> None:
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {self.host_header}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        self._sock.sendall(req.encode())

        # Read headers up to the blank line, keeping any frame bytes that
        # arrived in the same TCP segment.
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise WebSocketError("connection closed during handshake")
            self._buf += chunk
        head, self._buf = self._buf.split(b"\r\n\r\n", 1)
        lines = head.decode("latin-1").split("\r\n")
        if "101" not in lines[0]:
            raise WebSocketError(f"handshake failed: {lines[0]}")

        expect = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        accept = None
        for line in lines[1:]:
            name, _, value = line.partition(":")
            if name.strip().lower() == "sec-websocket-accept":
                accept = value.strip()
        if accept != expect:
            raise WebSocketError("bad Sec-WebSocket-Accept")

    # -- framing --
    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self._closed:
            raise WebSocketError("send on closed socket")
        header = bytearray([0x80 | opcode])
        length = len(payload)
        # Client frames MUST be masked (RFC 6455 §5.3).
        if length < 126:
            header.append(0x80 | length)
        elif length < (1 << 16):
            header.append(0x80 | 126)
            header += struct.pack("!H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", length)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self._sock.sendall(bytes(header) + masked)

    def _recv_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise WebSocketError("connection closed by peer")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _recv_frame(self) -> tuple[int, bool, bytes]:
        b0, b1 = self._recv_exact(2)
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]
        mask = self._recv_exact(4) if masked else None
        payload = self._recv_exact(length) if length else b""
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, fin, payload

    # -- public API --
    def send(self, text: str) -> None:
        self._send_frame(_OP_TEXT, text.encode("utf-8"))

    def send_json(self, obj: Any) -> None:
        self.send(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))

    def recv(self, timeout: float | None = None) -> str | None:
        """Receive one text message. Returns None on timeout or clean close.

        Control frames (ping/close) are handled transparently; fragmented
        messages are reassembled.
        """
        if timeout is not None:
            self._sock.settimeout(timeout)
        parts: list[bytes] = []
        try:
            while True:
                opcode, fin, payload = self._recv_frame()
                if opcode == _OP_PING:
                    self._send_frame(_OP_PONG, payload)
                    continue
                if opcode == _OP_PONG:
                    continue
                if opcode == _OP_CLOSE:
                    self._closed = True
                    return None
                if opcode in (_OP_TEXT, _OP_BIN, _OP_CONT):
                    parts.append(payload)
                    if fin:
                        return b"".join(parts).decode("utf-8", "replace")
                    continue
                raise WebSocketError(f"unexpected opcode {opcode}")
        except (socket.timeout, TimeoutError):
            return None
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._closed = True
            return None

    def recv_json(self, timeout: float | None = None) -> Any | None:
        raw = self.recv(timeout=timeout)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def drain(self, deadline: float) -> Iterator[Any]:
        """Yield decoded messages until the wall-clock ``deadline`` passes."""
        while time.time() < deadline:
            remaining = max(0.05, deadline - time.time())
            msg = self.recv_json(timeout=remaining)
            if msg is None:
                if self._closed:
                    return
                continue
            yield msg

    def close(self) -> None:
        if not self._closed:
            try:
                self._send_frame(_OP_CLOSE, b"\x03\xe8")
            except Exception:
                pass
            self._closed = True
        try:
            self._sock.close()
        except Exception:
            pass

    def __enter__(self) -> "WebSocket":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def http_post_json(
    url: str,
    payload: Any,
    host_header: str | None = None,
    extra_headers: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> tuple[int, Any]:
    """POST JSON over a plain socket and return ``(status, decoded_body)``.

    Deliberately not urllib: this must never be routed through an ambient
    ``HTTP(S)_PROXY``, and it needs the same ``Host:`` override trick the
    WebSocket client uses to address a community by its canonical host.
    Returns the body decoded as JSON, or the raw text if it is not JSON.
    """
    host, port, path = _parse_ws_url(url)
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {
        "Host": host_header or f"{host}:{port}",
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "Connection": "close",
    }
    headers.update(extra_headers or {})
    head = f"POST {path} HTTP/1.1\r\n" + "".join(
        f"{k}: {v}\r\n" for k, v in headers.items()
    )
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        sock.settimeout(timeout)
        sock.sendall(head.encode() + b"\r\n" + body)
        buf = b""
        while True:
            try:
                chunk = sock.recv(65536)
            except (socket.timeout, TimeoutError):
                break
            if not chunk:
                break
            buf += chunk
    finally:
        sock.close()

    if b"\r\n\r\n" not in buf:
        raise WebSocketError(f"malformed HTTP response from {url}")
    raw_head, raw_body = buf.split(b"\r\n\r\n", 1)
    lines = raw_head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    if any(
        line.lower().startswith("transfer-encoding:") and "chunked" in line.lower()
        for line in lines[1:]
    ):
        raw_body = _dechunk(raw_body)
    text = raw_body.decode("utf-8", "replace")
    try:
        return status, json.loads(text)
    except json.JSONDecodeError:
        return status, text


def _dechunk(body: bytes) -> bytes:
    out = b""
    while True:
        line, sep, rest = body.partition(b"\r\n")
        if not sep:
            break
        try:
            size = int(line.split(b";")[0], 16)
        except ValueError:
            break
        if size == 0:
            break
        out += rest[:size]
        body = rest[size:].lstrip(b"\r\n")
    return out


def _parse_ws_url(url: str) -> tuple[str, int, str]:
    if url.startswith("ws://"):
        rest = url[5:]
    elif url.startswith("http://"):
        rest = url[7:]
    else:
        raise ValueError(f"only ws:// (plaintext) URLs are supported here: {url}")
    hostport, slash, path = rest.partition("/")
    path = ("/" + path) if slash else "/"
    host, _, port_s = hostport.partition(":")
    return host, int(port_s) if port_s else 80, path

#!/usr/bin/env python3
"""Offline self-tests for scripts/lib/nostr_ws.py.

These run with no relay, no network, and no dependencies — the point is that
the soak harness's crypto and framing are pinned by something executable
before they are ever pointed at a relay.

    python3 scripts/lib/test_nostr_ws.py

Covered:
  * BIP-340 Schnorr signing against the official test vectors (sign + verify).
  * Sign/verify round-trip on random keys and messages.
  * NIP-01 event id derivation and self-consistency of build_event().
  * RFC 6455 client framing against a loopback echo server: short, 126-byte,
    and 64KiB payloads, server fragmentation, and ping/pong handling.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import struct
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nostr_ws import (  # noqa: E402
    PrivateKey,
    WebSocket,
    build_event,
    event_id,
    schnorr_verify,
)

FAILURES: list[str] = []


def check(cond: bool, label: str) -> None:
    if cond:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}")
        FAILURES.append(label)


# ─── BIP-340 official test vectors ──────────────────────────────────────────
# From the BIP-340 reference vectors (index, seckey, pubkey, aux, msg, sig).
VECTORS = [
    (
        "0000000000000000000000000000000000000000000000000000000000000003",
        "F9308A019258C31049344F85F89D5229B531C845836F99B08601F113BCE036F9",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "E907831F80848D1069A5371B402410364BDF1C5F8307B0084C55F1CE2DCA8215"
        "25F66A4A85EA8B71E482A74F382D2CE5EBEEE8FDB2172F477DF4900D310536C0",
    ),
    (
        "B7E151628AED2A6ABF7158809CF4F3C762E7160F38B4DA56A784D9045190CFEF",
        "DFF1D77F2A671C5F36183726DB2341BE58FEAE1DA2DECED843240F7B502BA659",
        "0000000000000000000000000000000000000000000000000000000000000001",
        "243F6A8885A308D313198A2E03707344A4093822299F31D0082EFA98EC4E6C89",
        "6896BD60EEAE296DB48A229FF71DFE071BDE413E6D43F917DC8DCF8C78DE3341"
        "8906D11AC976ABCCB20B091292BFF4EA897EFCB639EA871CFA95F6DE339E4B0A",
    ),
    (
        "C90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B14E5C9",
        "DD308AFEC5777E13121FA72B9CC1B7CC0139715309B086C960E18FD969774EB8",
        "C87AA53824B4D7AE2EB035A2B5BBBCCC080E76CDC6D1692C4B0B62D798E6D906",
        "7E2D58D8B3BCDF1ABADEC7829054F90DDA9805AAB56C77333024B9D0A508B75C",
        "5831AAEED7B44BB74E5EAB94BA9D4294C49BCF2A60728D8B4C200F50DD313C1B"
        "AB745879A5AD954A72C45A91C3A51D3C7ADEA98D82F8481E0E1E03674A6F3FB7",
    ),
    (
        "0B432B2677937381AEF05BB02A66ECD012773062CF3FA2549E44F58ED2401710",
        "25D1DFF95105F5253C4022F628A996AD3A0D95FBF21D468A1B33F8C160D8F517",
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "7EB0509757E246F19449885651611CB965ECC1A187DD51B64FDA1EDC9637D5EC"
        "97582B9CB13DB3933705B32BA982AF5AF25FD78881EBB32771FC5922EFC66EA3",
    ),
]


def test_bip340_vectors() -> None:
    print("BIP-340 official vectors")
    for i, (sec, pub, aux, msg, sig) in enumerate(VECTORS):
        key = PrivateKey.from_hex(sec)
        check(key.pubkey_hex == pub.lower(), f"vector {i}: pubkey derivation")
        got = key.sign(bytes.fromhex(msg), bytes.fromhex(aux))
        check(got == sig.lower(), f"vector {i}: signature")
        check(
            schnorr_verify(bytes.fromhex(msg), pub.lower(), sig.lower()),
            f"vector {i}: verify",
        )


def test_roundtrip() -> None:
    print("Sign/verify round-trip")
    for i in range(5):
        key = PrivateKey()
        msg = hashlib.sha256(f"round-trip-{i}".encode()).digest()
        sig = key.sign(msg)
        check(schnorr_verify(msg, key.pubkey_hex, sig), f"random key {i} verifies")
        bad = hashlib.sha256(f"tampered-{i}".encode()).digest()
        check(
            not schnorr_verify(bad, key.pubkey_hex, sig),
            f"random key {i} rejects wrong message",
        )


def test_event_building() -> None:
    print("NIP-01 event construction")
    key = PrivateKey.from_hex(
        "B7E151628AED2A6ABF7158809CF4F3C762E7160F38B4DA56A784D9045190CFEF"
    )
    ev = build_event(key, 9, "hello", [["h", "abc"]], created_at=1700000000)
    recomputed = event_id(
        ev["pubkey"], ev["created_at"], ev["kind"], ev["tags"], ev["content"]
    )
    check(ev["id"] == recomputed, "event id is a pure function of the fields")
    check(len(ev["id"]) == 64, "event id is 32 bytes hex")
    check(len(ev["sig"]) == 128, "signature is 64 bytes hex")
    check(
        schnorr_verify(bytes.fromhex(ev["id"]), ev["pubkey"], ev["sig"]),
        "event signature verifies against the id",
    )
    # Canonical serialization must not add spaces — the relay hashes the exact
    # NIP-01 form, so a stray space changes the id and the event is rejected.
    check(
        event_id("aa" * 32, 1, 1, [], "x")
        == hashlib.sha256(
            json.dumps([0, "aa" * 32, 1, 1, [], "x"], separators=(",", ":")).encode()
        ).hexdigest(),
        "canonical serialization is compact",
    )


# ─── Loopback WebSocket server ──────────────────────────────────────────────

_GUID = "258EAFA5-E914-47DA-95CA-5AB0DC85B11C"


def _server_send(conn: socket.socket, opcode: int, payload: bytes, fin: bool = True) -> None:
    """Send an unmasked server frame (RFC 6455 forbids masking server->client)."""
    header = bytearray([(0x80 if fin else 0x00) | opcode])
    n = len(payload)
    if n < 126:
        header.append(n)
    elif n < (1 << 16):
        header.append(126)
        header += struct.pack("!H", n)
    else:
        header.append(127)
        header += struct.pack("!Q", n)
    conn.sendall(bytes(header) + payload)


def _server_recv(conn: socket.socket, buf: bytearray) -> tuple[int, bytes]:
    def need(n: int) -> bytes:
        while len(buf) < n:
            chunk = conn.recv(65536)
            if not chunk:
                raise ConnectionError("client closed")
            buf.extend(chunk)
        out = bytes(buf[:n])
        del buf[:n]
        return out

    b0, b1 = need(2)
    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", need(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", need(8))[0]
    mask = need(4) if masked else b""
    payload = need(length) if length else b""
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def _echo_server(sock: socket.socket, saw_masked: list[bool]) -> None:
    """Handshake, then echo text frames back with a few special behaviors."""
    conn, _ = sock.accept()
    conn.settimeout(10)
    buf = bytearray()
    try:
        while b"\r\n\r\n" not in buf:
            buf.extend(conn.recv(4096))
        head, rest = bytes(buf).split(b"\r\n\r\n", 1)
        buf = bytearray(rest)
        key = ""
        for line in head.decode("latin-1").split("\r\n")[1:]:
            name, _, value = line.partition(":")
            if name.strip().lower() == "sec-websocket-key":
                key = value.strip()
        accept = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        conn.sendall(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode()
        )

        while True:
            # Peek at the mask bit to assert the client masks its frames.
            while len(buf) < 2:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf.extend(chunk)
            saw_masked.append(bool(buf[1] & 0x80))

            opcode, payload = _server_recv(conn, buf)
            if opcode == 0x8:  # close
                return
            if opcode in (0x9, 0xA):
                # Control frames are not echoed. The client's pong (sent in
                # reply to our ping) arrives here; echoing it back would look
                # like an extra data message to the client.
                continue
            text = payload.decode("utf-8", "replace")
            if text == "__ping__":
                # Exercise the client's transparent ping handling, then reply.
                _server_send(conn, 0x9, b"pingpayload")
                _server_send(conn, 0x1, b"after-ping")
            elif text == "__fragment__":
                _server_send(conn, 0x1, b"frag-one|", fin=False)
                _server_send(conn, 0x0, b"frag-two", fin=True)
            else:
                _server_send(conn, 0x1, payload)
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def test_websocket() -> None:
    print("RFC 6455 client framing (loopback)")
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]

    saw_masked: list[bool] = []
    thread = threading.Thread(target=_echo_server, args=(sock, saw_masked), daemon=True)
    thread.start()

    try:
        with WebSocket(f"ws://127.0.0.1:{port}/", timeout=10) as ws:
            ws.send("hello")
            check(ws.recv(timeout=5) == "hello", "short text echo")

            # 126 bytes crosses into the 16-bit length encoding.
            medium = "m" * 126
            ws.send(medium)
            check(ws.recv(timeout=5) == medium, "126-byte payload (16-bit length)")

            # 64 KiB crosses into the 64-bit length encoding.
            large = "L" * 65536
            ws.send(large)
            check(ws.recv(timeout=5) == large, "64KiB payload (64-bit length)")

            ws.send_json({"a": 1, "b": ["x", "y"]})
            check(ws.recv_json(timeout=5) == {"a": 1, "b": ["x", "y"]}, "JSON round-trip")

            ws.send("__fragment__")
            check(ws.recv(timeout=5) == "frag-one|frag-two", "fragmented message reassembly")

            ws.send("__ping__")
            check(ws.recv(timeout=5) == "after-ping", "ping handled transparently")

            check(ws.recv(timeout=0.3) is None, "recv times out cleanly with no data")
            check(all(saw_masked) and len(saw_masked) > 0, "all client frames were masked")
    finally:
        try:
            sock.close()
        except Exception:
            pass


def main() -> int:
    test_bip340_vectors()
    test_roundtrip()
    test_event_building()
    test_websocket()
    print()
    if FAILURES:
        print(f"FAILED ({len(FAILURES)}): " + ", ".join(FAILURES))
        return 1
    print("all self-tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

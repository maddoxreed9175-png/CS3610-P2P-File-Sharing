"""Provided protocol helpers. Keep this file unchanged in your submission."""
import hashlib
import json
import socket
import struct
import threading
import time
from pathlib import Path

CHUNK_SIZE = 65536
MAX_FILE_SIZE = 8 * 1024 * 1024
MAX_FRAME = 131072
TIMEOUT = 2.0
HEARTBEAT_SECONDS = 5
PEER_TTL = 15


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError("Connection closed before the complete frame arrived")
        data.extend(part)
    return bytes(data)


def send_message(sock, message):
    payload = json.dumps(message, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if not 1 <= len(payload) <= MAX_FRAME:
        raise ValueError("Invalid frame length")
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def receive_message(sock):
    length = struct.unpack("!I", recv_exact(sock, 4))[0]
    if not 1 <= length <= MAX_FRAME:
        raise ValueError("Invalid frame length")
    message = json.loads(recv_exact(sock, length).decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("JSON message must be an object")
    return message


def rpc(host, port, request):
    """Client side: one request and one response per connection; raises on transport errors."""
    with socket.create_connection((host, port), timeout=TIMEOUT) as sock:
        sock.settimeout(TIMEOUT)
        send_message(sock, request)
        return receive_message(sock)


def error(code):
    return {"ok": False, "error": code}


class UploadLimiter:
    """Emulates a peer's upload capacity u_i (slide 3): one chunk at a time at a fixed rate."""

    def __init__(self, kib_per_second=None):
        self.seconds_per_byte = 1 / (kib_per_second * 1024) if kib_per_second else 0
        self.lock = threading.Lock()

    def send(self, sock, message, size):
        with self.lock:
            time.sleep(size * self.seconds_per_byte)
            send_message(sock, message)


def chunk_length(manifest, index):
    if type(index) is not int or not 0 <= index < len(manifest["chunk_hashes"]):
        raise ValueError("Invalid chunk index")
    return min(CHUNK_SIZE, manifest["size"] - index * CHUNK_SIZE)


def validate_chunk(manifest, index, data):
    return len(data) == chunk_length(manifest, index) and sha256(data) == manifest["chunk_hashes"][index]


def load_manifest(path):
    m = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(m, dict):
        raise ValueError("Manifest must be an object")
    required = {"version", "name", "size", "chunk_size", "file_id", "chunk_hashes"}
    if set(m) != required or type(m["version"]) is not int or m["version"] != 1:
        raise ValueError("Invalid manifest schema/version")
    name = m["name"]
    if not isinstance(name, str) or not name or name in {".", ".."} or any(c in name for c in '/\\\x00'):
        raise ValueError("Unsafe filename")
    if type(m["size"]) is not int or not 1 <= m["size"] <= MAX_FILE_SIZE:
        raise ValueError("File size must be between 1 byte and 8 MiB")
    if type(m["chunk_size"]) is not int or m["chunk_size"] != CHUNK_SIZE:
        raise ValueError("Unsupported chunk size")
    hashes = m["chunk_hashes"]
    if not isinstance(hashes, list) or len(hashes) != (m["size"] + CHUNK_SIZE - 1) // CHUNK_SIZE:
        raise ValueError("Incorrect chunk count")
    for digest in [m["file_id"], *hashes]:
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise ValueError("Invalid SHA-256 digest")
    return m

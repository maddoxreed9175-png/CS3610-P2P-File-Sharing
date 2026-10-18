"""Student implementation template. See README.md and PROTOCOL.md.

Complete the TODOs in Peer and main(). 
"""
import argparse
import base64
import binascii
import os
import random
import signal
import socketserver
import sys
import threading
import time
import traceback
from pathlib import Path
from common import (CHUNK_SIZE, HEARTBEAT_SECONDS, TIMEOUT, UploadLimiter, error, load_manifest,
                    receive_message, rpc, send_message, sha256, validate_chunk)


class Peer:
    def __init__(self, args):
        self.args = args
        self.manifest = load_manifest(args.manifest)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.done = threading.Event()
        self.chunks = {}  # Example representation: {index: verified bytes}
        self.neighbors = {}  # {peer_id: (host, port)}
        fid = self.manifest["file_id"]
        n = len(self.manifest["chunk_hashes"])
        self.cache_dir = Path(args.output_dir) / ".chunks" / fid
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        # Recover cached chunks; ignore (and delete) any that fail validation.
        for i in range(n):
            path = self.cache_dir / f"{i}.chunk"
            try:
                data = path.read_bytes()
            except OSError:
                continue
            if validate_chunk(self.manifest, i, data):
                self.chunks[i] = data
            else:
                print(f"REJECTED cached chunk {i}: failed validation", flush=True)
                try:
                    path.unlink()
                except OSError:
                    pass
        if self.chunks:
            print(f"Recovered {len(self.chunks)} cached chunk(s)", flush=True)

        # Initial peer: verify the whole file and every chunk before making anything available.
        if args.entire_file is not None:
            try:
                data = Path(args.entire_file).read_bytes()
            except OSError as e:
                print(f"ERROR: cannot read --entire-file: {e}", file=sys.stderr, flush=True)
                sys.exit(1)
            if len(data) != self.manifest["size"] or sha256(data) != fid:
                print("ERROR: --entire-file does not match the manifest", file=sys.stderr, flush=True)
                sys.exit(1)
            pieces = {}
            for i in range(n):
                piece = data[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
                if not validate_chunk(self.manifest, i, piece):
                    print(f"ERROR: chunk {i} of --entire-file does not match the manifest",
                          file=sys.stderr, flush=True)
                    sys.exit(1)
                pieces[i] = piece
            for i, piece in pieces.items():
                if i not in self.chunks:
                    path = self.cache_dir / f"{i}.chunk"
                    tmp = path.with_name(path.name + ".tmp")
                    tmp.write_bytes(piece)
                    os.replace(tmp, path)
            self.chunks = pieces

    def handle_request(self, request):
        # Server side (like the TCP server from the socket programming lecture): answer one neighbor's request.
        op, fid = request.get("op"), request.get("file_id")
        if op not in ("HAVE", "GET_CHUNK") or not isinstance(fid, str):
            return error("BAD_REQUEST")
        if fid != self.manifest["file_id"]:
            return error("WRONG_FILE")
        if op == "HAVE":
            with self.lock:
                return {"ok": True, "chunks": sorted(self.chunks)}
        index = request.get("index")
        if type(index) is not int or not 0 <= index < len(self.manifest["chunk_hashes"]):
            return error("BAD_REQUEST")
        with self.lock:
            data = self.chunks.get(index)
        if data is None:
            return error("NO_CHUNK")
        return {"ok": True, "index": index, "data": base64.b64encode(data).decode("ascii")}

    def tracker_loop(self):
        # REGISTER every 5 s (even after completion); PEERS every second while incomplete.
        fid = self.manifest["file_id"]
        pid_self = self.args.peer_id
        last_register = last_peers = float("-inf")
        tracker_up = True
        while not self.stop.is_set():
            now = time.monotonic()
            try:
                if now - last_register >= HEARTBEAT_SECONDS:
                    last_register = now
                    rpc(self.args.tracker_host, self.args.tracker_port,
                        {"op": "REGISTER", "file_id": fid, "peer_id": pid_self,
                         "host": "127.0.0.1", "port": self.args.port})
                if not self.done.is_set() and now - last_peers >= 1.0:
                    last_peers = now
                    reply = rpc(self.args.tracker_host, self.args.tracker_port,
                                {"op": "PEERS", "file_id": fid, "peer_id": pid_self})
                    if reply.get("ok") is not True or not isinstance(reply.get("peers"), list):
                        raise ValueError("bad PEERS reply")
                    found = {}
                    for p in reply["peers"]:
                        if not isinstance(p, dict):
                            continue
                        pid, host, port = p.get("peer_id"), p.get("host"), p.get("port")
                        if (isinstance(pid, str) and pid != pid_self and host == "127.0.0.1"
                                and type(port) is int and 1024 <= port <= 65535):
                            found[pid] = (host, port)
                    with self.lock:
                        self.neighbors = found
                if not tracker_up:
                    print("Tracker reachable again", flush=True)
                    tracker_up = True
            except (OSError, EOFError, ValueError, TypeError) as e:
                # Keep serving and keep the neighbors we already know; retry soon.
                if tracker_up:
                    print(f"Tracker unreachable ({e!r}); keeping known neighbors and retrying", flush=True)
                    tracker_up = False
                last_register = now - HEARTBEAT_SECONDS + 1
                last_peers = now
            except Exception:
                traceback.print_exc(file=sys.stdout)
                sys.stdout.flush()
                self.stop.wait(1)
            self.stop.wait(0.5)

    def download_loop(self):
        # Client side: HAVE snapshots, rarest first, verify, store, reassemble.
        manifest = self.manifest
        fid = manifest["file_id"]
        n = len(manifest["chunk_hashes"])
        have_cache = {}  # {peer_id: (monotonic time, set of indices)}
        bad_until = {}   # {peer_id: monotonic time before which the neighbor is avoided}
        while not self.stop.is_set():
            try:
                # Reassemble in index order and check the whole-file hash once everything is here.
                with self.lock:
                    complete = len(self.chunks) == n
                    pieces = [self.chunks[i] for i in range(n)] if complete else None
                if complete:
                    data = b"".join(pieces)
                    if len(data) == manifest["size"] and sha256(data) == fid:
                        out = Path(self.args.output_dir) / manifest["name"]
                        tmp = out.with_name(out.name + ".tmp")
                        tmp.write_bytes(data)
                        os.replace(tmp, out)
                        self.done.set()
                        print(f"COMPLETE {fid} {out}", flush=True)
                        return  # the server and tracker_loop keep running
                    print("ERROR: assembled file failed whole-file verification; discarding chunks", flush=True)
                    with self.lock:
                        self.chunks.clear()
                    for i in range(n):
                        try:
                            (self.cache_dir / f"{i}.chunk").unlink()
                        except OSError:
                            pass
                    continue

                with self.lock:
                    neighbors = dict(self.neighbors)
                    missing = [i for i in range(n) if i not in self.chunks]
                now = time.monotonic()
                for pid in list(have_cache):
                    if pid not in neighbors:
                        del have_cache[pid]

                # Refresh stale HAVE snapshots (spec: at least every 5 s; we use 1 s).
                for pid, (host, port) in neighbors.items():
                    if bad_until.get(pid, 0) > now:
                        continue
                    cached = have_cache.get(pid)
                    if cached is not None and now - cached[0] < 1.0:
                        continue
                    try:
                        reply = rpc(host, port, {"op": "HAVE", "file_id": fid})
                    except (OSError, EOFError, ValueError) as e:
                        print(f"RETRY: HAVE from {pid} failed ({e!r})", flush=True)
                        bad_until[pid] = time.monotonic() + 2.0
                        have_cache[pid] = (time.monotonic(), set())
                        continue
                    listed = reply.get("chunks")
                    if reply.get("ok") is not True or not isinstance(listed, list):
                        print(f"REJECTED HAVE reply from {pid}: {reply.get('error', 'malformed')}", flush=True)
                        bad_until[pid] = time.monotonic() + 2.0
                        have_cache[pid] = (time.monotonic(), set())
                        continue
                    have_cache[pid] = (time.monotonic(),
                                       {i for i in listed if type(i) is int and 0 <= i < n})

                # Who holds each missing chunk?
                now = time.monotonic()
                holders = {i: [] for i in missing}
                for pid, (_, have) in have_cache.items():
                    if bad_until.get(pid, 0) > now:
                        continue
                    for i in have:
                        if i in holders:
                            holders[i].append(pid)
                candidates = {i: h for i, h in holders.items() if h}
                if not candidates:
                    self.stop.wait(random.uniform(1, 2))  # nobody has what we need: wait, then ask again
                    continue

                # Rarest first (ties random); holders tried in random order to spread requests.
                rarest = min(len(h) for h in candidates.values())
                index = random.choice([i for i, h in candidates.items() if len(h) == rarest])
                order = candidates[index][:]
                random.shuffle(order)
                got = False
                for pid in order:
                    host, port = neighbors[pid]
                    try:
                        reply = rpc(host, port, {"op": "GET_CHUNK", "file_id": fid, "index": index})
                    except (OSError, EOFError, ValueError) as e:
                        print(f"RETRY: chunk {index} from {pid} failed ({e!r})", flush=True)
                        bad_until[pid] = time.monotonic() + 2.0
                        continue
                    if reply.get("ok") is not True:
                        code = reply.get("error")
                        print(f"RETRY: chunk {index} from {pid} returned {code}", flush=True)
                        if code == "NO_CHUNK":
                            have_cache[pid][1].discard(index)
                        else:
                            bad_until[pid] = time.monotonic() + 2.0
                        continue
                    data = None
                    value = reply.get("data")
                    if type(reply.get("index")) is int and reply["index"] == index and isinstance(value, str):
                        try:
                            data = base64.b64decode(value, validate=True)
                        except (binascii.Error, ValueError):
                            data = None
                    if data is None or not validate_chunk(manifest, index, data):
                        print(f"REJECTED chunk {index} from {pid}: bad index, encoding, length or hash", flush=True)
                        bad_until[pid] = time.monotonic() + 10.0
                        continue
                    path = self.cache_dir / f"{index}.chunk"
                    tmp = path.with_name(path.name + ".tmp")
                    try:
                        tmp.write_bytes(data)
                        os.replace(tmp, path)
                    except OSError as e:
                        print(f"WARNING: could not cache chunk {index}: {e!r}", flush=True)
                    with self.lock:
                        self.chunks[index] = data
                    print(f"RECEIVED chunk {index} from {pid}", flush=True)
                    got = True
                    break
                if not got:
                    self.stop.wait(0.2)
            except Exception:
                # Worker exceptions are logged, never hidden; the loop keeps going.
                traceback.print_exc(file=sys.stdout)
                sys.stdout.flush()
                self.stop.wait(1)


class PeerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Handler(socketserver.BaseRequestHandler):
    """Provided; keep unchanged. Applies the experiment and test flags to your replies."""

    def handle(self):
        self.request.settimeout(TIMEOUT)
        args = self.server.peer.args
        try:
            reply = self.server.peer.handle_request(receive_message(self.request))
        except (ValueError, TypeError, EOFError, OSError):
            reply = error("BAD_REQUEST")
        size = 0
        if reply.get("ok") is True and isinstance(reply.get("data"), str):
            if args.no_upload:
                reply = error("NO_CHUNK")
            else:
                size = len(reply["data"]) * 3 // 4
                if args.corrupt_uploads:
                    reply = {**reply, "data": base64.b64encode(b"corrupt" * 16).decode("ascii")}
        elif reply.get("ok") is True and "chunks" in reply and args.no_upload:
            reply = {**reply, "chunks": []}
        try:
            self.server.limiter.send(self.request, reply, size)
        except OSError:
            pass


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--peer-id", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--tracker-host", default="127.0.0.1", choices=["127.0.0.1"])
    p.add_argument("--tracker-port", type=int, default=9000)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--entire-file", type=Path, help="start with the entire file (the initial peer)")
    p.add_argument("--upload-kibps", type=float, help="experiment: limit upload rate u_i in KiB/s")
    p.add_argument("--no-upload", action="store_true", help="experiment: act as a client-server client")
    p.add_argument("--corrupt-uploads", action="store_true", help="testing: send corrupted chunk data")
    args = p.parse_args()
    for port in (args.port, args.tracker_port):
        if not 1024 <= port <= 65535:
            p.error("Ports must be between 1024 and 65535")
    if not 1 <= len(args.peer_id) <= 64 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in args.peer_id):
        p.error("Peer IDs use 1–64 ASCII letters, digits, hyphens, or underscores")
    if args.upload_kibps is not None and args.upload_kibps <= 0:
        p.error("--upload-kibps must be positive")
    return args


def main():
    args = parse_args()
    # SIGTERM raises KeyboardInterrupt so the orderly-shutdown path (UNREGISTER) also runs on terminate.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    peer = Peer(args)
    with PeerServer(("127.0.0.1", args.port), Handler) as server:
        server.peer = peer
        server.limiter = UploadLimiter(args.upload_kibps)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(f"READY {args.peer_id} {args.port}", flush=True)
        # The server is listening, so it is now safe to register with the tracker.
        threading.Thread(target=peer.tracker_loop, daemon=True).start()
        threading.Thread(target=peer.download_loop, daemon=True).start()
        try:
            while not peer.stop.wait(0.5):  # wait here until Ctrl+C / SIGTERM
                pass
        except KeyboardInterrupt:
            pass
        finally:
            peer.stop.set()
            try:  # best-effort UNREGISTER
                rpc(args.tracker_host, args.tracker_port,
                    {"op": "UNREGISTER", "file_id": peer.manifest["file_id"], "peer_id": args.peer_id})
            except (OSError, EOFError, ValueError):
                pass
            server.shutdown()


if __name__ == "__main__":
    main()

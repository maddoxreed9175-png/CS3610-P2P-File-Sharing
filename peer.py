"""Student implementation template. See README.md and PROTOCOL.md.

Complete the TODOs in Peer and main(). 
"""
import argparse
import base64
import socketserver
import threading
from pathlib import Path
from common import TIMEOUT, UploadLimiter, error, load_manifest, receive_message, send_message


class Peer:
    def __init__(self, args):
        self.args = args
        self.manifest = load_manifest(args.manifest)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.chunks = {}  # Example representation: {index: verified bytes}
        # TODO: create output/cache directories; verify any cached chunks.
        # TODO: if --entire-file is provided, validate size and whole-file hash,
        # split it into chunks, and validate each chunk before making it available.

    def handle_request(self, request):
        # Server side (like the TCP server from the socket programming lecture): answer one neighbor's request.
        # TODO: validate op and file_id, then implement HAVE and GET_CHUNK.
        # HAVE reports only verified chunks; GET_CHUNK serves only such chunks.
        # Return the exact success/error schemas in PROTOCOL.md.
        return error("BAD_REQUEST")

    def tracker_loop(self):
        # TODO: REGISTER now and every 5 seconds. Query PEERS repeatedly.
        # Do not register before the server socket is listening.
        # Catch timeouts and tracker outages; retry without stopping uploads.
        raise NotImplementedError("Implement tracker registration and peer discovery")

    def download_loop(self):
        # Client side: request chunks from neighbors.
        # TODO: ask each neighbor HAVE, request missing chunks rarest first,
        # validate base64 + index + length + SHA-256, then store verified chunks.
        # TODO: try another neighbor on timeout, bad data, or NO_CHUNK (churn).
        # If the tracker is down, keep using the neighbors you already know.
        # TODO: assemble in index order and check the whole-file hash.
        # Keep the server running after completion; never prompt for input.
        raise NotImplementedError("Implement verified chunk downloading")


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
    peer = Peer(args)
    with PeerServer(("127.0.0.1", args.port), Handler) as server:
        server.peer = peer
        server.limiter = UploadLimiter(args.upload_kibps)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(f"READY {args.peer_id} {args.port}", flush=True)
        # TODO: start tracker and download workers; coordinate shutdown.
        # A complete implementation should wait here until Ctrl+C and attempt
        # UNREGISTER on shutdown. Worker exceptions must be logged, not hidden.
        try:
            peer.tracker_loop()
        finally:
            peer.stop.set()
            server.shutdown()


if __name__ == "__main__":
    main()

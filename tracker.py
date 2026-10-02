"""Provided localhost tracker: tracks the peers participating in each torrent (slide 7), never chunks."""
import argparse
import socketserver
import threading
import time
from common import PEER_TTL, TIMEOUT, error, receive_message, send_message


class Tracker(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address):
        super().__init__(address, Handler)
        self.peers = {}
        self.lock = threading.Lock()

    def dispatch(self, msg):
        op, fid, pid = msg.get("op"), msg.get("file_id"), msg.get("peer_id")
        if not isinstance(fid, str) or len(fid) != 64 or any(c not in '0123456789abcdef' for c in fid):
            return error("BAD_REQUEST")
        if not isinstance(pid, str) or not 1 <= len(pid) <= 64 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in pid):
            return error("BAD_REQUEST")
        if op not in {"REGISTER", "PEERS", "UNREGISTER"}:
            return error("BAD_REQUEST")
        now = time.monotonic()
        with self.lock:
            self.peers = {k: v for k, v in self.peers.items() if now - v["seen"] < PEER_TTL}
            key = (fid, pid)
            if op == "REGISTER":
                port = msg.get("port")
                if msg.get("host") != "127.0.0.1" or type(port) is not int or not 1024 <= port <= 65535:
                    return error("BAD_REQUEST")
                self.peers[key] = {"peer_id": pid, "host": "127.0.0.1", "port": port, "seen": now}
                return {"ok": True, "ttl": PEER_TTL}
            if op == "UNREGISTER":
                self.peers.pop(key, None)
                return {"ok": True}
            peers = [{f: v[f] for f in ("peer_id", "host", "port")}
                     for k, v in self.peers.items() if k[0] == fid and k != key]
            peers.sort(key=lambda v: v["peer_id"])
            return {"ok": True, "peers": peers}


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(TIMEOUT)
        try:
            response = self.server.dispatch(receive_message(self.request))
        except (ValueError, EOFError, OSError, TypeError):
            response = error("BAD_REQUEST")
        try:
            send_message(self.request, response)
        except OSError:
            pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9000)
    args = p.parse_args()
    if not 1024 <= args.port <= 65535:
        p.error("Port must be between 1024 and 65535")
    with Tracker(("127.0.0.1", args.port)) as server:
        print(f"TRACKER_READY 127.0.0.1:{args.port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()

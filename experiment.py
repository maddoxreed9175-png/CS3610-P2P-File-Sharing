"""Provided tool: measure file distribution time, client-server vs P2P (slides 3-6).

Launches the tracker, one initial peer with the entire file, and N downloading
peers, all running your peer.py with the same upload capacity. In client-server
mode the downloaders never upload, so the initial peer acts as the only server.
Distribution time is measured from launching the downloaders until the last
one prints COMPLETE. One row per run is appended to the CSV file.
"""
import argparse
import csv
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from common import load_manifest, rpc


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["client-server", "p2p"], required=True)
    p.add_argument("--peers", type=int, required=True, help="N, the number of downloading peers")
    p.add_argument("--file", type=Path, required=True)
    p.add_argument("--upload-kibps", type=float, default=512, help="u_s = u_i for every peer (KiB/s)")
    p.add_argument("--peer", type=Path, default=HERE / "peer.py", help="peer program to run")
    p.add_argument("--base-port", type=int, default=9500)
    p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--csv", type=Path, default=Path("results.csv"))
    args = p.parse_args()
    if not 1 <= args.peers <= 20 or not 1024 <= args.base_port <= 65535 - args.peers - 1:
        p.error("Use 1-20 peers and a free port range")
    work = Path(tempfile.mkdtemp(prefix="experiment-"))
    manifest = work / "manifest.json"
    subprocess.run([sys.executable, str(HERE / "make_manifest.py"), str(args.file), "--out", str(manifest)],
                   check=True, stdout=subprocess.DEVNULL)
    m = load_manifest(manifest)
    processes, finished = [], {}

    def launch(label, command, watch=False):
        log = open(work / f"{label}.log", "w", encoding="utf-8")
        proc = subprocess.Popen([sys.executable, "-u", *command], stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        processes.append(proc)

        def reader():
            for line in proc.stdout:
                log.write(f"{time.monotonic() - start:9.3f} {line}")
                if watch and line.startswith("COMPLETE") and label not in finished:
                    finished[label] = time.monotonic() - start
            log.close()
        threading.Thread(target=reader, daemon=True).start()

    def peer(label, port, extra):
        return [str(args.peer), "--peer-id", label, "--port", str(port), "--tracker-port", str(args.base_port),
                "--manifest", str(manifest), "--output-dir", str(work / label),
                "--upload-kibps", str(args.upload_kibps), *extra]

    start = time.monotonic()
    try:
        launch("tracker", [str(HERE / "tracker.py"), "--port", str(args.base_port)])
        launch("initial", peer("initial", args.base_port + 1, ["--entire-file", str(args.file)]))
        deadline = time.monotonic() + 10
        while True:  # wait until the initial peer is registered with the tracker
            try:
                reply = rpc("127.0.0.1", args.base_port, {"op": "PEERS", "file_id": m["file_id"], "peer_id": "probe"})
                if reply.get("peers"):
                    break
            except (OSError, EOFError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise SystemExit(f"Initial peer did not register; see {work / 'initial.log'}")
            time.sleep(0.1)
        start = time.monotonic()
        extra = ["--no-upload"] if args.mode == "client-server" else []
        labels = [f"peer{i + 1}" for i in range(args.peers)]
        for i, label in enumerate(labels):
            launch(label, peer(label, args.base_port + 2 + i, extra), watch=True)
        while len(finished) < len(labels) and time.monotonic() - start < args.timeout:
            time.sleep(0.1)
    finally:
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
    times = [finished.get(label) for label in labels]
    done = all(t is not None for t in times)
    distribution = max(times) if done else None
    new = not args.csv.exists()
    with args.csv.open("a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["mode", "N", "F_bytes", "upload_kibps", "distribution_time_s", "per_peer_times_s", "logs"])
        w.writerow([args.mode, args.peers, m["size"], args.upload_kibps,
                    f"{distribution:.2f}" if done else "incomplete",
                    " ".join(f"{t:.2f}" if t is not None else "-" for t in times), str(work)])
    print(f"{args.mode} N={args.peers}: distribution time "
          + (f"{distribution:.2f} s" if done else f"incomplete after {args.timeout:.0f} s"))
    print(f"Logs (timestamped): {work}")
    return 0 if done else 1


if __name__ == "__main__":
    raise SystemExit(main())

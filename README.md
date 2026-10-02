# Assignment 1 — BitTorrent Lite starter

Read the Canvas assignment page and `PROTOCOL.md` before coding. You implement `peer.py`. The other Python files are provided and must stay unchanged:

| File | Use |
|---|---|
| `peer.py` | Your peer. Complete the TODOs; keep the CLI and the provided `Handler`. |
| `common.py` | Message framing, `rpc`, hashing, manifest loading, upload rate limiter. |
| `tracker.py` | The tracker. |
| `make_manifest.py` | Creates the manifest for a file. |
| `experiment.py` | Part B: measures distribution time, client-server vs P2P. |
| `PROTOCOL.md` | The exact message formats every peer must follow. |
| `REPORT_TEMPLATE.docx` | Word template for your report (Part B and the AI-use note). Save your copy as `REPORT.docx`. |

Python 3.10 or newer, standard library only. Where the executable is named `python`, use that instead of `python3`.

## Run a small torrent

Create a test file (about 256 KiB, five chunks, the last one short) and its manifest:
```sh
python3 -c "from pathlib import Path; Path('demo.bin').write_bytes(bytes(range(256))*1025)"
python3 make_manifest.py demo.bin --out demo.json
```

Then use one terminal per process:
```sh
python3 tracker.py --port 9000
python3 peer.py --peer-id initial --port 9101 --manifest demo.json --output-dir initial-data --entire-file demo.bin
python3 peer.py --peer-id alice --port 9102 --manifest demo.json --output-dir alice-data
python3 peer.py --peer-id bob --port 9103 --manifest demo.json --output-dir bob-data
```

Check the bytes, not just the log:
```sh
python3 -c "from pathlib import Path; assert Path('demo.bin').read_bytes()==Path('alice-data/demo.bin').read_bytes(); print('Files match')"
```

Peers keep running after COMPLETE; stop each with Ctrl+C. Every peer needs its own port, ID, and output directory. Chunks must travel over the network only; never read another peer's directory.

To watch upload-while-downloading happen, slow every peer down with `--upload-kibps 64`.

## Suggested order

1. Load the manifest; recover cached chunks; verify `--entire-file`.
2. Implement HAVE and GET_CHUNK. Test them with `common.rpc` before writing any downloader.
3. Register with the tracker every 5 seconds and ask it for PEERS.
4. Download one missing chunk from one neighbor, verify it, store it. Repeat.
5. Add rarest first, retries on churn, reassembly, and staying in the torrent after COMPLETE.

## Part B command example

```sh
python3 -c "import os; open('big.bin','wb').write(os.urandom(4*1024*1024))"
python3 experiment.py --mode client-server --peers 4 --file big.bin
python3 experiment.py --mode p2p --peers 4 --file big.bin
```

Each run appends a row to `results.csv` and prints the folder holding timestamped logs for every peer.

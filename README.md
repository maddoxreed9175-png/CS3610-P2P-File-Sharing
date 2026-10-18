# Assignment 1 — BitTorrent Lite

## Run a small torrent

Create a test file (about 256 KiB, five chunks, the last one short) and its manifest:

```sh
python3 -c "from pathlib import Path; Path('demo.bin').write\\\_bytes(bytes(range(256))\\\*1025)"
python3 make\\\_manifest.py demo.bin --out demo.json
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
python3 -c "from pathlib import Path; assert Path('demo.bin').read\\\_bytes()==Path('alice-data/demo.bin').read\\\_bytes(); print('Files match')"
```

Peers keep running after COMPLETE; stop each with Ctrl+C . Every peer needs its own port, ID, and output directory. Chunks must travel over the network only; never read another peer's directory. Delete the output directories between runs, otherwise cached chunks make the download finish instantly.

To watch upload-while-downloading happen, slow every peer down with `--upload-kibps 64`.

## Part B command example

```sh
python3 -c "import os; open('big.bin','wb').write(os.urandom(4\\\*1024\\\*1024))"
python3 experiment.py --mode client-server --peers 4 --file big.bin
python3 experiment.py --mode p2p --peers 4 --file big.bin
```

Each run appends a row to `results.csv` and prints the folder holding timestamped logs for every peer.

## Known limitations

* **Initial peer is overloaded.** Every peer picks a random holder and HAVE snapshots can be a second slow, so requests are not steered away from the initial peer.
* **Startup delay.** The download loop starts before the tracker has returned any neighbors, so it sleeps 1–2 s before the first request.


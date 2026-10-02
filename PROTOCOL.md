# BitTorrent Lite protocol v1 — required for every submission

This is a teaching protocol modeled on the BitTorrent description in the P2P Architecture slides (slides 7–10). It is not the real BitTorrent wire protocol. Every submission follows this contract exactly, so that any student's peer can exchange chunks with any other student's peer and with the instructor's reference peer.

Use Python 3.10+ and the standard library only. All tests run on one computer using `127.0.0.1`.

## Terms

| Term | Meaning in this assignment |
|---|---|
| **torrent** | The group of peers exchanging chunks of one file (slide 7). Identified by `file_id`. |
| **chunk** | One 64 KiB (65,536-byte) part of the file. The last chunk may be shorter. (Slide 7 mentions 256 Kb chunks; this assignment uses 64 KiB.) |
| **tracker** | Tracks the peers participating in the torrent (slide 7). It knows addresses only, never chunks. |
| **neighbors** | The peers your peer learned about from the tracker and exchanges chunks with (slide 8). |
| **churn** | Peers may come and go at any time (slide 8). |
| **initial peer** | A peer started with `--entire-file`. It plays the server's role from slides 3–5. |
| **manifest** | A JSON file every peer receives before joining: the file's size, chunk size, and SHA-256 hash for the whole file and for each chunk. Real BitTorrent uses a `.torrent` file for this. |

## Framing and connections

Every TCP connection carries exactly one request and one response. The side that connects is the client; the side that accepts is the server, as in the TCP client and server examples from the socket programming lecture. Every peer is both: its server socket answers neighbors, and it opens client sockets to request chunks.

Each message is a UTF-8 JSON object preceded by a 4-byte unsigned big-endian payload length. The length excludes the prefix. Valid payload size is 1–131072 bytes. Use `common.rpc`, `send_message`, and `receive_message`; TCP is a byte stream, so one `recv` need not return a full message. The client closes the connection after reading the response. Use a 2-second socket timeout. JSON field order is irrelevant.

Success responses have `"ok": true`. Errors have `{"ok":false,"error":"CODE"}`. Peer error codes:

- `BAD_REQUEST`: missing, malformed, or unknown operation. Out-of-range, negative, noninteger, or Boolean indices are also BAD_REQUEST.
- `WRONG_FILE`: a well-formed request for a different `file_id`.
- `NO_CHUNK`: a valid index, but your peer does not currently have that chunk.

A malformed request must never stop your server. Unknown extra request fields can be ignored.

## Manifest and storage

`make_manifest.py` creates the manifest: `version` (1), `name` (file basename), `size` (bytes), `chunk_size` (65536), `file_id` (SHA-256 of the entire file), and `chunk_hashes` (SHA-256 of each chunk, in order). Load it with `common.load_manifest`.

Files are nonempty and at most 8 MiB. Chunk indices start at 0. Chunk i begins at byte `i * 65536`. No padding is added: a 150,000-byte file has three chunks of 65,536, 65,536, and 18,928 bytes.

Store each verified chunk at `<output-dir>/.chunks/<file_id>/<index>.chunk`. On startup, validate cached chunks before announcing them; ignore invalid ones and download replacements. Write the completed file to `<output-dir>/<manifest name>` only after reassembling the chunks in index order and checking the whole-file hash.

## Command line

```text
python3 peer.py --peer-id alice --port 9101 --tracker-host 127.0.0.1 --tracker-port 9000 \
    --manifest demo.json --output-dir alice-data [--entire-file demo.bin]
```

Tracker host and port default to `127.0.0.1:9000`. Peer IDs use 1–64 letters, digits, hyphens, or underscores. Each peer in a test has a unique ID, port, and output directory.

With `--entire-file`, verify the file against the manifest before announcing its chunks; if it does not match, print an error and exit with a nonzero status. Without it, recover valid cached chunks and download the rest.

Three more flags are handled by the provided `Handler` in `peer.py`. Keep them working; do not implement them yourself:

- `--upload-kibps R` limits your upload rate to R KiB/s (the u<sub>i</sub> of slide 3). Used by `experiment.py`.
- `--no-upload` makes your peer announce and serve nothing, like a client in client-server distribution. Used by `experiment.py`.
- `--corrupt-uploads` makes your peer send corrupted chunk data. Used for testing integrity checks.

Print `READY <peer-id> <port>` once the server socket is listening. Print `COMPLETE <file_id> <output-path>` after whole-file verification. Log each chunk received (index and neighbor peer ID), each rejected chunk, and each retry.

## Tracker operations

Start the server socket before registering. Send REGISTER immediately and every 5 seconds while running, including after completion. A registration expires 15 seconds after its last refresh. While incomplete, send PEERS at least every 5 seconds so late joiners are found. If the tracker is unreachable, keep serving and keep exchanging chunks with the neighbors you already know, and keep retrying the tracker. UNREGISTER on orderly shutdown is best effort.

REGISTER request and response:
```json
{"op":"REGISTER","file_id":"<64-character digest>","peer_id":"alice","host":"127.0.0.1","port":9101}
{"ok":true,"ttl":15}
```
PEERS request and response (excludes the requester and expired registrations):
```json
{"op":"PEERS","file_id":"<64-character digest>","peer_id":"alice"}
{"ok":true,"peers":[{"peer_id":"bob","host":"127.0.0.1","port":9102}]}
```
UNREGISTER uses the same fields as PEERS with `"op":"UNREGISTER"`; the response is `{"ok":true}`. An empty peer list is normal: wait and retry.

## Peer operations

HAVE asks a neighbor for the list of chunks it has (slide 9):
```json
{"op":"HAVE","file_id":"<64-character digest>"}
{"ok":true,"chunks":[0,2,5]}
```
Return sorted indices of verified chunks only. An empty list is valid. HAVE is a snapshot; a later GET_CHUNK can still fail.

GET_CHUNK requests one chunk:
```json
{"op":"GET_CHUNK","file_id":"<64-character digest>","index":2}
{"ok":true,"index":2,"data":"<base64 of the exact chunk bytes>"}
```
Encode with `base64.b64encode(data).decode("ascii")`. Before storing or announcing a received chunk, check: `ok` is true, the returned index matches, `base64.b64decode(value, validate=True)` succeeds, and `common.validate_chunk` passes (length and SHA-256).

Check a request in this order and reply with the first error that applies:

1. Unknown `op`, or `file_id` missing or not a string → `BAD_REQUEST`.
2. `file_id` differs from yours → `WRONG_FILE`.
3. GET_CHUNK only: `index` missing, not an integer, a Boolean, or out of range → `BAD_REQUEST`.
4. GET_CHUNK only: you do not have that chunk yet → `NO_CHUNK`.

## Requesting chunks

- **Upload while downloading.** Your server must serve every verified chunk immediately, before your peer has the entire file (slide 8). Protect shared state with a lock; never hold the lock while waiting on the network.
- **Rarest first.** While incomplete, ask each neighbor HAVE at least every 5 seconds, since neighbors gain chunks as the torrent progresses. Request the missing chunk held by the fewest neighbors first (slide 9). Break ties randomly.
- **Spread requests.** When several neighbors have a chunk, do not always pick the same one.
- **Churn.** On a timeout, disconnect, bad data, or NO_CHUNK, keep the chunk missing and try another neighbor. If no neighbor has a needed chunk, wait 1–2 seconds and ask again; do not busy-loop and never claim completion.
- **Remain in the torrent.** After COMPLETE, keep registering and serving until interrupted (slide 8: altruistically remain).

A single download thread requesting one chunk at a time is sufficient.

## Out of scope

Choking, optimistic unchoking, and tit-for-tat (slides 9–10); real `.torrent` files, magnet links, DHT, NAT traversal, encryption, GUI, and Internet deployment.

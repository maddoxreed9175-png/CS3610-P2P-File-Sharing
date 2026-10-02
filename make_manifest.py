"""Provided tool: create the manifest every peer in the torrent receives before launch."""
import argparse
import json
from pathlib import Path
from common import CHUNK_SIZE, MAX_FILE_SIZE, sha256


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    data = args.file.read_bytes()
    if not 1 <= len(data) <= MAX_FILE_SIZE:
        parser.error("Use a nonempty file no larger than 8 MiB")
    manifest = {"version": 1, "name": args.file.name, "size": len(data),
                "chunk_size": CHUNK_SIZE, "file_id": sha256(data),
                "chunk_hashes": [sha256(data[i:i + CHUNK_SIZE]) for i in range(0, len(data), CHUNK_SIZE)]}
    args.out.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(manifest["file_id"])


if __name__ == "__main__":
    main()

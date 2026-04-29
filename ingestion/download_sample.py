"""
Download one hour of GitHub Archive data and print the raw JSON schema.
Usage: .venv/bin/python ingestion/download_sample.py
"""

import gzip
import json
import os
import requests

SAMPLE_URL = "https://data.gharchive.org/2024-01-01-0.json.gz"
RAW_DIR = "data/raw"
OUTPUT_FILE = os.path.join(RAW_DIR, "2024-01-01-0.json.gz")
PREVIEW_RECORDS = 3


def download(url: str, dest: str) -> None:
    if os.path.exists(dest):
        print(f"Already downloaded: {dest}")
        return
    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"  {pct:.1f}%", end="\r")
    print(f"\nSaved to {dest} ({downloaded / 1_000_000:.1f} MB)")


def explore(path: str, n: int = PREVIEW_RECORDS) -> None:
    print(f"\n--- First {n} records ---")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            record = json.loads(line)
            print(f"\nRecord {i + 1}:")
            print(f"  type  : {record.get('type')}")
            print(f"  id    : {record.get('id')}")
            print(f"  actor : {record.get('actor', {}).get('login')}")
            print(f"  repo  : {record.get('repo', {}).get('name')}")
            print(f"  created_at: {record.get('created_at')}")
            print(f"  top-level keys: {list(record.keys())}")

    print("\n--- Event type distribution (first 5000 records) ---")
    counts: dict[str, int] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 5000:
                break
            event_type = json.loads(line).get("type", "unknown")
            counts[event_type] = counts.get(event_type, 0) + 1
    for event_type, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {event_type:<35} {count:>5}")


if __name__ == "__main__":
    os.makedirs(RAW_DIR, exist_ok=True)
    download(SAMPLE_URL, OUTPUT_FILE)
    explore(OUTPUT_FILE)

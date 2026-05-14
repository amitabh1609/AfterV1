# Failure Scenario 3: Corrupted Downloads from Infrastructure Sleep

## Problem

During multi-hour ingestion runs, Docker Desktop's Resource Saver mode put the MinIO container to sleep mid-download. When the process resumed and tried to parse a `.json.gz` file that had been partially written before the container died, Python's `gzip.open` read zero lines — not a `GzipError`, but silently an empty iterator. The downstream `pd.DataFrame(records)` created an empty frame which caused a `KeyError: 'created_at'` when the schema cast ran on an empty DataFrame. This killed the entire pipeline process with a traceback rather than skipping the bad file.

## How It Was Detected

The error appeared in the terminal during an overnight ingestion run:

```
KeyError: 'created_at'
  File "ingestion/ingest_incremental.py", line 287, in parse_to_bronze_arrow
    df["created_at"] = pd.to_datetime(...)
```

Cross-referencing the manifest showed the file was not marked done but the `.json.gz` existed on disk. Inspecting it with `zcat data/raw/2024-01-01-6.json.gz | wc -l` returned 0, confirming the file was a corrupt empty archive.

## How the Pipeline Reacted

After this failure, two defences were added:

**1. Atomic downloads** — the script writes to a `.gz.tmp` file and only renames to the final `.json.gz` path after the HTTP response is fully consumed. If the process dies mid-download, the `.tmp` file is cleaned up and the final file never appears on disk:

```python
tmp = dest.with_suffix(".gz.tmp")
# ... stream to tmp ...
tmp.rename(dest)   # atomic on POSIX
```

**2. Empty-file guard** — after parsing, an explicit check raises `RuntimeError` before any schema operations run:

```python
if not records:
    raise RuntimeError(f"No records parsed from {path} — file may be empty or corrupted")
```

The caller catches `RuntimeError`, deletes the corrupt file, and skips to the next hour. On the next run, the missing file is re-downloaded cleanly.

## Recovery Steps

The pipeline self-recovers on the next run. To trigger it:

```bash
# Check which files are corrupt (exist on disk but not in manifest as "done")
python3 -c "
import json, gzip
from pathlib import Path
manifest = json.load(open('data/processed/manifest.json'))
done = {k for k,v in manifest.items() if v.get('status')=='done'}
for f in Path('data/raw').glob('*.json.gz'):
    if f.name not in done:
        try:
            with gzip.open(f,'rt') as fh:
                lines = sum(1 for _ in fh)
            print(f'{f.name}: {lines} lines (OK)')
        except Exception as e:
            print(f'{f.name}: CORRUPT — {e}')
"

# Resume — corrupted files are auto-deleted and re-downloaded
.venv/bin/python ingestion/ingest_incremental.py
```

## What I Learned

Atomicity is not just about database transactions — it applies to every file write in a pipeline. A file that exists on disk but is not fully written is worse than a file that does not exist at all, because it passes existence checks but fails content checks in hard-to-debug ways. The `.tmp` rename pattern costs one line of code and eliminates an entire class of corrupt-state failures.

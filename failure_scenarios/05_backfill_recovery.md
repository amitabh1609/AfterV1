# Failure Scenario 5: Backfill Recovery Using the Manifest

## Problem

During a 24-hour ingestion run for `2024-01-01`, the Docker daemon went to sleep three separate times due to Docker Desktop's Resource Saver mode. Each time, the MinIO container stopped accepting connections mid-write. The PyIceberg multipart upload in progress failed with:

```
botocore.exceptions.ClientError: The specified multipart upload does not exist.
```

The S3 multipart upload protocol requires the upload to be completed or explicitly aborted within a timeout window. When Docker stopped the MinIO container mid-write, the upload ID was lost and could not be completed. PyIceberg raised the error, the Python process crashed, and the manifest was not updated (because `mark_done` only runs after both Bronze and Silver writes succeed).

This left the pipeline in a state where some files were partially written to Iceberg (Bronze rows appended but Silver not yet written, or both written but manifest not updated) and others were not written at all.

## How It Was Detected

After Docker was restarted, running the ingestion script again immediately showed the problem: it attempted to re-ingest files that had already been partially written, resulting in duplicate Bronze rows for those hours. The Silver deduplication step (`drop_duplicates(subset="id")`) caught and discarded the duplicates silently — which was correct behaviour but also meant the Bronze table accumulated some duplicate rows.

## How the Pipeline Reacted

The manifest-based state tracking was specifically designed for this failure mode. `mark_done()` writes to the manifest only after both Bronze and Silver writes are confirmed by PyIceberg returning successfully. If the process crashes before `mark_done` runs, the file is treated as unprocessed on the next run.

For Silver, the per-file deduplication ensures that re-ingesting a file that was partially written to Bronze does not produce duplicate Silver rows. Bronze does accumulate duplicates in this scenario — which is an accepted trade-off for Bronze (raw landing zone, not an analytical layer).

For Gold, `recompute_gold()` always drops and recreates both Gold tables from all Silver rows, so Gold is always consistent regardless of how many times Bronze was written.

## Recovery Steps

```bash
# Step 1: Restart Docker and MinIO
open -a Docker
# wait ~60 seconds for Docker to start
docker start minio

# Step 2: Resume ingestion — the manifest skips already-completed files
.venv/bin/python ingestion/ingest_incremental.py

# Step 3: Verify no gaps in manifest
python3 -c "
import json
m = json.load(open('data/processed/manifest.json'))
from datetime import date
expected = set()
for d in ['2024-01-01', '2024-01-02', '2024-01-03']:
    for h in range(24):
        expected.add(f'{d}-{h}.json.gz')
done = {k for k,v in m.items() if v.get('status')=='done'}
missing = expected - done
print(f'Missing: {sorted(missing) or \"none — all done\"}')"

# Step 4: If Bronze has duplicates but Silver is clean, that's acceptable.
# If you want a pristine Bronze, run with --fresh (rewrites everything):
.venv/bin/python ingestion/ingest_incremental.py --fresh
```

## What I Learned

A pipeline that can only run to completion cleanly is not a production pipeline — it is a script. The manifest pattern costs almost nothing to implement (one JSON file, one atomic write) and turns an unrecoverable crash into a resumable operation. The key design insight is that `mark_done` must be the last line that runs, not the first, and that the writes it guards must be idempotent or deduplicated at the next layer. If you cannot guarantee atomicity at the write layer, guarantee it at the read layer.

# Failure Scenario 1: Missing Fields (Quarantine Routing)

## Problem

GitHub Archive events occasionally arrive with a null or empty `id` or `type` field. This happens because the GH Archive captures raw API responses, and GitHub's own API has historically returned incomplete payloads during high-traffic periods or for deprecated event types. An event with no `id` cannot be deduplicated. An event with no `type` cannot be routed to the correct Silver transform logic. Either case silently corrupts downstream aggregations if the row is allowed through.

## How It Was Detected

During the first Silver transform run, a manual spot-check of row counts between Bronze and Silver showed a small consistent discrepancy — Bronze had 153,644 rows for hour 0 but Silver only had 153,619. The 25-row gap was the first signal. Adding an explicit `missing_id` and `missing_type` mask to the transform function (`ingestion/ingest_incremental.py`, `transform_to_silver_and_quarantine`) made the discrepancy visible and auditable.

## How the Pipeline Reacted

The transform function applies three vectorised boolean masks before any data reaches Silver:

```python
missing_id   = df["id"].isna() | (df["id"] == "")
missing_type = df["type"].isna() | (df["type"] == "")
malformed    = df["payload"].apply(_is_malformed_payload)
bad_mask     = missing_id | missing_type | malformed
```

Rows matching `bad_mask` are routed to `silver.quarantine` with an `error_reason` column set to `missing_id`, `missing_type`, or `malformed_payload` in priority order. The quarantine table is an append-only Iceberg table with the full raw record stored as JSON so any row can be re-processed once the root cause is fixed.

## Recovery Steps

```bash
# Inspect what was quarantined
.venv/bin/python - <<'EOF'
from pyiceberg.catalog.sql import SqlCatalog
import os
from dotenv import load_dotenv
load_dotenv()

catalog = SqlCatalog("local", **{
    "uri": "sqlite:///data/processed/iceberg_catalog.db",
    "warehouse": f"s3://{os.getenv('S3_BUCKET')}",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    "s3.endpoint": os.getenv("S3_ENDPOINT"),
    "s3.access-key-id": os.getenv("S3_ACCESS_KEY"),
    "s3.secret-access-key": os.getenv("S3_SECRET_KEY"),
    "s3.path-style-access": "true",
})
df = catalog.load_table("silver.quarantine").scan().to_pandas()
print(df.groupby("error_reason").size())
print(df[["source_file","raw_id","error_reason"]].head(10))
EOF

# If GitHub fixes the upstream issue and re-publishes the file,
# remove the affected entries from the manifest and re-run:
# (edit data/processed/manifest.json to remove the file entry)
.venv/bin/python ingestion/ingest_incremental.py --dates 2024-01-01
```

## What I Learned

A raw row count match between source and destination is not a data quality check — it is only true if you are also routing bad rows somewhere observable. The quarantine table pattern forces you to account for every input row: it either lands in Silver or in the quarantine, and the two counts must sum to Bronze. This constraint makes silent data loss structurally impossible.

# Failure Scenario 4: Iceberg Schema Evolution and Field ID Mismatch

## Problem

After the Silver table was created and populated with 3.8 million rows, a schema evolution demo (`ingestion/schema_evolution.py`) added a new column `is_bot` (Boolean, field ID 17) using PyIceberg's `update_schema()`. This is exactly what Iceberg is designed for — a metadata-only operation that requires zero data rewrite. The problem arose because the main ingestion script (`ingest_incremental.py`) still had a hardcoded 16-field Arrow schema for Silver writes. When it tried to append new rows with a 16-field Arrow table to a table whose Iceberg schema now had 17 fields, PyIceberg raised a schema mismatch error.

A secondary issue occurred earlier in development: the original Silver schema defined `id` as `required=True` (NestedField with `required=True`). PyArrow's `Table.from_pandas` marks all string columns as nullable by default. When PyIceberg validated the Arrow schema against the Iceberg schema, the nullability mismatch caused every append to fail with a cryptic type error rather than a clear "field is required but nullable" message.

## How It Was Detected

The `required=True` mismatch surfaced immediately on the first Silver append attempt:

```
pyarrow.lib.ArrowInvalid: Schema at index 0 was different
```

The `is_bot` schema mismatch would surface on any subsequent append after `schema_evolution.py` was run, with a similar schema validation error from PyIceberg.

## How the Pipeline Reacted

**For the `required=True` issue:** The fix was to remove `required=True` from the `id` NestedField definition, drop the existing (empty) table, and recreate it. Since no data had been written yet, no backfill was needed.

**For the `is_bot` evolution:** The correct approach is to update the Arrow schema in `ingest_incremental.py` to include the new column (with `None` values for rows ingested before the column existed), or to read the current Iceberg schema dynamically and build the Arrow schema from it rather than hardcoding field definitions. In this project, the schema evolution demo is intentionally kept separate from the main pipeline to isolate the demonstration — but in production, any schema change must be coordinated across all writers.

## Recovery Steps

```bash
# If Silver appends fail after a schema evolution, check current Iceberg schema:
.venv/bin/python - <<'EOF'
from pyiceberg.catalog.sql import SqlCatalog
import os; from dotenv import load_dotenv; load_dotenv()
catalog = SqlCatalog("local", **{
    "uri": "sqlite:///data/processed/iceberg_catalog.db",
    "warehouse": f"s3://{os.getenv('S3_BUCKET')}",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    "s3.endpoint": os.getenv("S3_ENDPOINT"),
    "s3.access-key-id": os.getenv("S3_ACCESS_KEY"),
    "s3.secret-access-key": os.getenv("S3_SECRET_KEY"),
    "s3.path-style-access": "true",
})
tbl = catalog.load_table("silver.github_events")
for f in tbl.schema().fields:
    print(f"  [{f.field_id:>2}] {f.name:<20} {str(f.field_type)}")
EOF

# If you need to add the new column to the Arrow schema in ingest_incremental.py,
# add it with None values for backward compatibility:
# pa.field("is_bot", pa.bool_())  → value: None for all existing rows
```

## What I Learned

Iceberg's schema evolution guarantee ("metadata-only, no rewrite") is only half the story. The other half is that every writer in the system must be updated to produce data compatible with the new schema. In a real team, this means schema changes require a coordinated release across all writers — not just an ALTER TABLE equivalent. Iceberg makes the table format compatible; it does not make your application code compatible automatically.

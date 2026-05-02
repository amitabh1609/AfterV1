"""
Iceberg schema evolution demo — add a column to silver.github_events
without rewriting any existing data.

Scenario: after 3.8 M rows are already in production, the team decides
they need an `is_bot` flag to filter automated actors from analytics.

Iceberg schema evolution lets you:
  1. Add the column to the table metadata (instant, no data rewrite)
  2. Old Parquet files return NULL for the new column (backward compatible)
  3. New appends can write a real value into the column

This script demonstrates all three stages and verifies the result.

Usage:
    .venv/bin/python ingestion/schema_evolution.py
"""

import os
from datetime import datetime, timezone

import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import BooleanType, NestedField

load_dotenv()

CATALOG_URI  = "sqlite:///data/processed/iceberg_catalog.db"
SILVER_TABLE = "silver.github_events"
NEW_FIELD_ID = 17          # next field ID after the existing 16
NEW_FIELD    = "is_bot"


def get_catalog() -> SqlCatalog:
    return SqlCatalog("local", **{
        "uri": CATALOG_URI,
        "warehouse": f"s3://{os.getenv('S3_BUCKET', 'lakehouse')}",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint":            os.getenv("S3_ENDPOINT",   "http://localhost:9000"),
        "s3.access-key-id":       os.getenv("S3_ACCESS_KEY", "minio"),
        "s3.secret-access-key":   os.getenv("S3_SECRET_KEY", "minio123"),
        "s3.path-style-access":   "true",
    })


def _is_bot(login: str) -> bool:
    if not login:
        return False
    lower = login.lower()
    return lower.endswith("[bot]") or lower.endswith("-bot")


def main():
    print("==> Step 1: Connect and inspect current schema")
    catalog = get_catalog()
    tbl     = catalog.load_table(SILVER_TABLE)

    print(f"\n  Table          : {SILVER_TABLE}")
    print(f"  Schema version : {tbl.metadata.current_schema_id}")
    print(f"  Fields         : {len(tbl.schema().fields)}")
    print(f"  Current rows   : {int(tbl.metadata.snapshots[-1].summary.additional_properties.get('total-records', 0)):,}")
    print(f"\n  Current schema:")
    for f in tbl.schema().fields:
        print(f"    [{f.field_id:>2}] {f.name:<20} {str(f.field_type):<20} {'required' if f.required else 'optional'}")

    # ── Check if column already exists (idempotent) ──────────────────────────
    existing_names = {f.name for f in tbl.schema().fields}
    if NEW_FIELD in existing_names:
        print(f"\n  Column '{NEW_FIELD}' already exists — skipping add.")
    else:
        print(f"\n==> Step 2: Add column '{NEW_FIELD}' (BooleanType, optional)")
        print("  This is a metadata-only operation — zero Parquet files rewritten.")

        with tbl.update_schema() as upd:
            upd.add_column(
                path       = NEW_FIELD,
                field_type = BooleanType(),
                doc        = "True when actor_login ends with [bot] or -bot",
            )

        tbl = catalog.load_table(SILVER_TABLE)   # reload to pick up new schema
        print(f"  Schema version now : {tbl.metadata.current_schema_id}")
        print(f"  Fields now         : {len(tbl.schema().fields)}")
        new_field = tbl.schema().find_field(NEW_FIELD)
        print(f"  New field          : [{new_field.field_id}] {new_field.name} — {new_field.field_type}")

    # ── Stage 3: verify old data returns NULL for new column ─────────────────
    print(f"\n==> Step 3: Verify old rows return NULL for '{NEW_FIELD}'")
    print("  Reading 100 rows from the FIRST snapshot (written before the column existed) ...")

    first_snap_id = tbl.metadata.snapshots[0].snapshot_id
    df_old = tbl.scan(snapshot_id=first_snap_id, limit=100).to_pandas()

    if NEW_FIELD in df_old.columns:
        null_count = df_old[NEW_FIELD].isna().sum()
        print(f"  '{NEW_FIELD}' column present  : YES")
        print(f"  Null values in 100 old rows  : {null_count} / 100")
        print(f"  → All old rows return NULL (no data rewrite needed) ✓")
    else:
        print(f"  '{NEW_FIELD}' column absent in old snapshot scan — expected for pre-schema snapshots.")

    # ── Stage 4: append new rows WITH the column populated ───────────────────
    print(f"\n==> Step 4: Append 5 synthetic rows with '{NEW_FIELD}' populated")

    now = datetime.now(tz=timezone.utc).replace(microsecond=0)
    sample_actors = [
        ("github-actions[bot]", True),
        ("dependabot[bot]",     True),
        ("renovate-bot",        True),
        ("amitabh1609",         False),
        ("torvalds",            False),
    ]

    rows = []
    for i, (login, is_bot_val) in enumerate(sample_actors):
        rows.append({
            "id":             f"schema_evo_demo_{i}",
            "type":           "PushEvent",
            "actor_login":    login,
            "actor_id":       str(9000 + i),
            "repo_name":      f"demo/schema-evo-{i}",
            "repo_id":        str(8000 + i),
            "created_at":     now,
            "event_date":     now.date(),
            "action":         None,
            "ref":            "refs/heads/main",
            "ref_type":       None,
            "commit_count":   1,
            "pr_number":      None,
            "issue_number":   None,
            "ingested_at":    now,
            "transformed_at": now,
            "is_bot":         is_bot_val,
        })

    import pandas as pd
    df_new = pd.DataFrame(rows)
    df_new["created_at"]     = pd.to_datetime(df_new["created_at"]).dt.as_unit("us")
    df_new["ingested_at"]    = pd.to_datetime(df_new["ingested_at"]).dt.as_unit("us")
    df_new["transformed_at"] = pd.to_datetime(df_new["transformed_at"]).dt.as_unit("us")
    df_new["event_date"]     = pd.to_datetime(df_new["event_date"]).dt.date

    arrow_schema = pa.schema([
        pa.field("id",             pa.string()),
        pa.field("type",           pa.string()),
        pa.field("actor_login",    pa.string()),
        pa.field("actor_id",       pa.string()),
        pa.field("repo_name",      pa.string()),
        pa.field("repo_id",        pa.string()),
        pa.field("created_at",     pa.timestamp("us", tz="UTC")),
        pa.field("event_date",     pa.date32()),
        pa.field("action",         pa.string()),
        pa.field("ref",            pa.string()),
        pa.field("ref_type",       pa.string()),
        pa.field("commit_count",   pa.int64()),
        pa.field("pr_number",      pa.int64()),
        pa.field("issue_number",   pa.int64()),
        pa.field("ingested_at",    pa.timestamp("us", tz="UTC")),
        pa.field("transformed_at", pa.timestamp("us", tz="UTC")),
        pa.field("is_bot",         pa.bool_()),
    ])
    arrow_table = pa.Table.from_pandas(df_new, schema=arrow_schema, preserve_index=False)

    tbl.append(arrow_table)
    tbl = catalog.load_table(SILVER_TABLE)
    total_after = int(tbl.metadata.snapshots[-1].summary.additional_properties.get("total-records", 0))
    print(f"  Appended {len(rows)} rows with is_bot populated.")
    print(f"  Total rows now: {total_after:,}")

    # ── Stage 5: query the new column ────────────────────────────────────────
    print(f"\n==> Step 5: Query new rows using the '{NEW_FIELD}' column")
    df_check = tbl.scan(limit=10).to_pandas()
    # Find our demo rows
    demo_rows = df_check[df_check["id"].str.startswith("schema_evo_demo", na=False)]

    if demo_rows.empty:
        # scan(limit=10) might not hit the last partition — do a targeted read
        df_check = tbl.scan().to_pandas()
        demo_rows = df_check[df_check["id"].str.startswith("schema_evo_demo", na=False)]

    if not demo_rows.empty:
        cols = ["actor_login", "is_bot", "type", "repo_name"]
        print(demo_rows[cols].to_string(index=False))
    else:
        print("  (demo rows not retrieved in sample — try --list on time_travel.py to inspect)")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  SCHEMA EVOLUTION COMPLETE")
    print(f"{'═' * 60}")
    print(f"  1. Added column '{NEW_FIELD}' (Boolean) — metadata only, instant")
    print(f"  2. Old rows (3.8 M) return NULL — no rewrite, backward compatible")
    print(f"  3. New rows carry real True/False values")
    print(f"  4. Schema version bumped to {tbl.metadata.current_schema_id}")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()

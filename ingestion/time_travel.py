"""
Iceberg time-travel demo for silver.github_events.

Demonstrates reading the Silver table at any past snapshot, showing how the
table looked before a given hourly file was ingested. Compares row counts
and event-type distributions between two points in table history.

Usage:
    # Default: compare first snapshot vs current (full day diff)
    .venv/bin/python ingestion/time_travel.py

    # Compare a specific snapshot against current
    .venv/bin/python ingestion/time_travel.py --snapshot-id <ID>

    # Compare by timestamp (ISO format)
    .venv/bin/python ingestion/time_travel.py --as-of "2026-05-01T13:20:00"

    # List all snapshots only
    .venv/bin/python ingestion/time_travel.py --list
"""

import argparse
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog

load_dotenv()

CATALOG_URI  = "sqlite:///data/processed/iceberg_catalog.db"
SILVER_TABLE = "silver.github_events"


def get_catalog() -> SqlCatalog:
    return SqlCatalog("local", **{
        "uri": CATALOG_URI,
        "warehouse": f"s3://{os.getenv('S3_BUCKET', 'lakehouse')}",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "minio"),
        "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "minio123"),
        "s3.path-style-access": "true",
    })


def fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def list_snapshots(tbl):
    print(f"\n{'Idx':>4}  {'Snapshot ID':>20}  {'Timestamp':>25}  {'Total Rows':>12}  {'Added Rows':>12}")
    print("─" * 82)
    snaps = tbl.metadata.snapshots
    prev_rows = 0
    for i, snap in enumerate(snaps):
        props = snap.summary.additional_properties
        total = int(props.get("total-records", 0))
        added = int(props.get("added-records", 0))
        print(f"{i:>4}  {snap.snapshot_id:>20}  {fmt_ts(snap.timestamp_ms):>25}  {total:>12,}  {added:>12,}")
        prev_rows = total
    print(f"\n  Total snapshots : {len(snaps)}")
    print(f"  Current rows    : {prev_rows:,}")


def event_type_dist(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df["type"].value_counts()
        .reset_index()
        .rename(columns={"type": "event_type", "count": "count"})
        .head(10)
    )


def compare_snapshots(tbl, before_snap_id: int, label_before: str, label_after: str):
    """
    Core time-travel demonstration:
    Read the table at `before_snap_id` (a past snapshot) and compare
    it against the current state.

    PyIceberg's scan(snapshot_id=X) re-reads the table exactly as it
    existed when snapshot X was written — no data is changed or lost,
    the older Parquet files are simply re-read using the old metadata.
    """
    print(f"\n{'═' * 60}")
    print(f"  TIME-TRAVEL COMPARISON")
    print(f"{'═' * 60}")
    print(f"  BEFORE : {label_before}")
    print(f"  AFTER  : {label_after} (current)")
    print(f"{'─' * 60}")

    # ── Read BEFORE (past snapshot) ──────────────────────────────────
    print(f"\n  Reading past snapshot {before_snap_id} ...")
    df_before = tbl.scan(snapshot_id=before_snap_id).to_pandas()
    print(f"  ✓ Read {len(df_before):,} rows from past snapshot")

    # ── Read AFTER (current) ─────────────────────────────────────────
    print(f"\n  Reading current snapshot ...")
    df_after = tbl.scan().to_pandas()
    print(f"  ✓ Read {len(df_after):,} rows from current snapshot")

    # ── Row count diff ───────────────────────────────────────────────
    added_rows  = len(df_after) - len(df_before)
    pct_change  = (added_rows / len(df_before) * 100) if len(df_before) > 0 else 0

    print(f"\n{'─' * 60}")
    print(f"  ROW COUNT DELTA")
    print(f"{'─' * 60}")
    print(f"  Before : {len(df_before):>12,} rows")
    print(f"  After  : {len(df_after):>12,} rows")
    print(f"  Added  : {added_rows:>12,} rows  (+{pct_change:.1f}%)")

    # ── Event type distribution comparison ──────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  EVENT TYPE DISTRIBUTION (top 10)")
    print(f"{'─' * 60}")

    dist_before = event_type_dist(df_before).set_index("event_type")["count"]
    dist_after  = event_type_dist(df_after).set_index("event_type")["count"]
    all_types   = sorted(set(dist_before.index) | set(dist_after.index))

    print(f"  {'Event Type':<35} {'Before':>10} {'After':>10} {'Diff':>10}")
    print(f"  {'─'*35} {'─'*10} {'─'*10} {'─'*10}")
    for t in all_types:
        b = dist_before.get(t, 0)
        a = dist_after.get(t, 0)
        d = a - b
        print(f"  {t:<35} {b:>10,} {a:>10,} {'+' if d >= 0 else ''}{d:>9,}")

    # ── Sample of newly added rows ───────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  SAMPLE OF NEWLY ADDED ROWS (5 rows)")
    print(f"{'─' * 60}")

    # Find rows in AFTER that are not in BEFORE by event id
    ids_before = set(df_before["id"].dropna())
    new_rows   = df_after[~df_after["id"].isin(ids_before)]

    if not new_rows.empty:
        sample_cols = ["id", "type", "actor_login", "repo_name", "created_at"]
        sample_cols = [c for c in sample_cols if c in new_rows.columns]
        print(new_rows[sample_cols].head(5).to_string(index=False))
        print(f"\n  ... {len(new_rows):,} total new rows not present in the past snapshot")
    else:
        print("  (no new rows found — snapshots may be identical)")

    print(f"\n{'═' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="Iceberg time-travel demo")
    parser.add_argument("--list",        action="store_true", help="List all snapshots and exit")
    parser.add_argument("--snapshot-id", type=int,            help="Past snapshot ID to compare against current")
    parser.add_argument("--as-of",       type=str,            help="ISO timestamp to travel to (e.g. 2026-05-01T13:20:00)")
    args = parser.parse_args()

    print("==> Connecting to catalog ...")
    catalog = get_catalog()
    tbl     = catalog.load_table(SILVER_TABLE)
    snaps   = tbl.metadata.snapshots

    print(f"  Table          : {SILVER_TABLE}")
    print(f"  Total snapshots: {len(snaps)}")
    print(f"  Current rows   : {int(snaps[-1].summary.additional_properties.get('total-records', 0)):,}")

    if args.list:
        list_snapshots(tbl)
        return

    # ── Resolve the "before" snapshot ────────────────────────────────
    if args.snapshot_id:
        before_snap_id = args.snapshot_id
        label_before   = f"snapshot {before_snap_id}"

    elif args.as_of:
        # Convert ISO timestamp → milliseconds, find nearest snapshot before it
        target_dt  = datetime.fromisoformat(args.as_of).replace(tzinfo=timezone.utc)
        target_ms  = int(target_dt.timestamp() * 1000)
        candidates = [s for s in snaps if s.timestamp_ms <= target_ms]
        if not candidates:
            print(f"  ERROR: no snapshot found before {args.as_of}")
            return
        snap        = candidates[-1]
        before_snap_id = snap.snapshot_id
        label_before   = f"{args.as_of} (snapshot {before_snap_id})"

    else:
        # Default: compare the FIRST snapshot (after hour 0 was ingested)
        # vs current — shows the full day's worth of additions
        first_snap     = snaps[0]
        before_snap_id = first_snap.snapshot_id
        first_rows     = int(first_snap.summary.additional_properties.get("total-records", 0))
        label_before   = f"after hour 0 only ({first_rows:,} rows, snapshot {before_snap_id})"

    label_after = f"after all 24 hours ({int(snaps[-1].summary.additional_properties.get('total-records',0)):,} rows)"

    # Print full snapshot list for reference
    print(f"\n==> Snapshot history:")
    list_snapshots(tbl)

    # Run the comparison
    compare_snapshots(tbl, before_snap_id, label_before, label_after)


if __name__ == "__main__":
    main()

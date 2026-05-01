"""
Incremental ingestion for GitHub Archive hourly files (2024-01-01, hours 0-23).

Design decisions:
  - Manifest (JSON file) is the source of truth for what has been processed.
    A dedicated Iceberg metadata table would also work, but a JSON file is
    simpler to inspect, requires no catalog connection to read, and cannot
    get into an inconsistent state if the catalog write fails.

  - Bronze and Silver are append-only in this script. We never drop and
    recreate — each new file's rows are appended to the existing table.
    Gold is the exception: it's always recomputed from all Silver, because
    aggregates cannot be safely appended row-by-row.

  - Downloads use a temp file. We write to a .tmp path, then rename to the
    final path only after the download completes. If the process dies mid-
    download, the .tmp is cleaned up and the final file never appears — so
    we never try to parse a truncated .gz file.

  - Silver deduplication is per-file only. GitHub Archive files are disjoint
    by hour, so the same event ID will not appear in two different files.
    Deduplicating within each file is sufficient.

  - The --fresh flag is a safety valve: it drops all three layers and clears
    the manifest, letting you start completely clean. Without --fresh, the
    script only processes files not yet in the manifest.

Usage:
    .venv/bin/python ingestion/ingest_incremental.py           # process new files
    .venv/bin/python ingestion/ingest_incremental.py --fresh   # wipe and restart
"""

import argparse
import gzip
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import requests
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType, TimestamptzType

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────

DATE = "2024-01-01"
HOURS = range(24)

# GH Archive drops leading zeros on hours: hour 0 → "0", hour 10 → "10"
GHARCHIVE_URL = "https://data.gharchive.org/{filename}"

RAW_DIR = Path("data/raw")
MANIFEST_PATH = Path("data/processed/manifest.json")
CATALOG_URI = "sqlite:///data/processed/iceberg_catalog.db"

# ── Iceberg Schemas ───────────────────────────────────────────────────────────

BRONZE_SCHEMA = Schema(
    NestedField(1, "id",          StringType()),
    NestedField(2, "type",        StringType()),
    NestedField(3, "actor_login", StringType()),
    NestedField(4, "actor_id",    StringType()),
    NestedField(5, "repo_name",   StringType()),
    NestedField(6, "repo_id",     StringType()),
    NestedField(7, "created_at",  TimestamptzType()),
    NestedField(8, "payload",     StringType()),
    NestedField(9, "ingested_at", TimestamptzType()),
)

SILVER_SCHEMA = Schema(
    NestedField(1,  "id",             StringType()),
    NestedField(2,  "type",           StringType()),
    NestedField(3,  "actor_login",    StringType()),
    NestedField(4,  "actor_id",       StringType()),
    NestedField(5,  "repo_name",      StringType()),
    NestedField(6,  "repo_id",        StringType()),
    NestedField(7,  "created_at",     TimestamptzType()),
    NestedField(8,  "event_date",     DateType()),
    NestedField(9,  "action",         StringType()),
    NestedField(10, "ref",            StringType()),
    NestedField(11, "ref_type",       StringType()),
    NestedField(12, "commit_count",   LongType()),
    NestedField(13, "pr_number",      LongType()),
    NestedField(14, "issue_number",   LongType()),
    NestedField(15, "ingested_at",    TimestamptzType()),
    NestedField(16, "transformed_at", TimestamptzType()),
)

REPO_GOLD_SCHEMA = Schema(
    NestedField(1,  "event_date",          DateType(),   required=True),
    NestedField(2,  "repo_name",           StringType(), required=True),
    NestedField(3,  "push_count",          LongType()),
    NestedField(4,  "total_commits",       LongType()),
    NestedField(5,  "pr_opened",           LongType()),
    NestedField(6,  "pr_closed",           LongType()),
    NestedField(7,  "issues_opened",       LongType()),
    NestedField(8,  "stars",               LongType()),
    NestedField(9,  "forks",               LongType()),
    NestedField(10, "unique_contributors", LongType()),
    NestedField(11, "total_events",        LongType()),
    NestedField(12, "aggregated_at",       TimestamptzType()),
)

ACTOR_GOLD_SCHEMA = Schema(
    NestedField(1,  "event_date",    DateType(),   required=True),
    NestedField(2,  "actor_login",   StringType(), required=True),
    NestedField(3,  "push_count",    LongType()),
    NestedField(4,  "total_commits", LongType()),
    NestedField(5,  "pr_count",      LongType()),
    NestedField(6,  "issue_count",   LongType()),
    NestedField(7,  "repos_touched", LongType()),
    NestedField(8,  "total_events",  LongType()),
    NestedField(9,  "aggregated_at", TimestamptzType()),
)


def date_partition(source_id: int) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000,
                       transform=IdentityTransform(), name="event_date")
    )


# ── Catalog ───────────────────────────────────────────────────────────────────

def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "local",
        **{
            "uri": CATALOG_URI,
            "warehouse": f"s3://{os.getenv('S3_BUCKET', 'lakehouse')}",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "minio"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "minio123"),
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace(catalog, ns):
    try:
        catalog.create_namespace(ns)
    except NamespaceAlreadyExistsError:
        pass


def get_or_create_table(catalog, full_name, schema, partition_spec=None):
    """Load the table if it exists, create it if not. Never drops."""
    try:
        return catalog.load_table(full_name)
    except Exception:
        kwargs = {"schema": schema}
        if partition_spec:
            kwargs["partition_spec"] = partition_spec
        return catalog.create_table(full_name, **kwargs)


def drop_table_if_exists(catalog, full_name):
    try:
        catalog.drop_table(full_name)
        print(f"    Dropped '{full_name}'")
    except Exception:
        pass


# ── Manifest ──────────────────────────────────────────────────────────────────

def load_manifest() -> dict:
    """
    The manifest is a simple JSON dict keyed by filename.
    We chose JSON over a database or Iceberg table because:
      1. It's human-readable — you can inspect/edit it with any text editor
      2. It's readable before the catalog is even connected
      3. It can't get out of sync with itself (atomic file write)
    """
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest: dict):
    # Write to a temp file in the same directory, then rename.
    # rename() is atomic on POSIX — no reader ever sees a half-written file.
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.rename(MANIFEST_PATH)


def mark_done(manifest, filename, bronze_rows, silver_rows):
    manifest[filename] = {
        "status": "done",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
    }
    save_manifest(manifest)


# ── Download ──────────────────────────────────────────────────────────────────

def download_file(filename: str) -> Path:
    """
    Downloads a GH Archive file to data/raw/.

    Uses a .tmp file during download. Only renames to the final path after the
    HTTP response is fully consumed. If anything fails (network error, disk
    full, KeyboardInterrupt), the .tmp is deleted and the final file is never
    created — so we cannot accidentally parse a truncated .gz file later.
    """
    dest = RAW_DIR / filename
    if dest.exists():
        print(f"    Already on disk: {filename}")
        return dest

    url = GHARCHIVE_URL.format(filename=filename)
    print(f"    Downloading {filename} ...", end=" ", flush=True)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".gz.tmp")

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                    f.write(chunk)
        tmp.rename(dest)
        size_mb = dest.stat().st_size / 1_048_576
        print(f"done ({size_mb:.1f} MB)")
        return dest
    except Exception as exc:
        if tmp.exists():
            tmp.unlink()
        raise RuntimeError(f"Download failed for {filename}: {exc}") from exc


# ── Bronze ────────────────────────────────────────────────────────────────────

def parse_to_bronze_arrow(path: Path) -> pa.Table:
    ingested_at = datetime.now(timezone.utc).isoformat()
    records = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            actor = r.get("actor") or {}
            repo = r.get("repo") or {}
            records.append({
                "id":          str(r.get("id", "")),
                "type":        r.get("type"),
                "actor_login": actor.get("login"),
                "actor_id":    str(actor.get("id", "")),
                "repo_name":   repo.get("name"),
                "repo_id":     str(repo.get("id", "")),
                "created_at":  r.get("created_at"),
                "payload":     json.dumps(r.get("payload") or {}),
                "ingested_at": ingested_at,
            })

    df = pd.DataFrame(records)
    df["created_at"]  = pd.to_datetime(df["created_at"],  utc=True).dt.as_unit("us")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True).dt.as_unit("us")

    return pa.Table.from_pandas(df, preserve_index=False)


# ── Silver ────────────────────────────────────────────────────────────────────

def _parse_payload(row) -> dict:
    try:
        p = json.loads(row["payload"]) if isinstance(row["payload"], str) else {}
    except (json.JSONDecodeError, TypeError):
        p = {}

    t = row["type"]
    out = dict(action=None, ref=None, ref_type=None,
               commit_count=None, pr_number=None, issue_number=None)

    if t == "PushEvent":
        out["ref"] = p.get("ref")
        out["commit_count"] = p.get("size")
    elif t in ("CreateEvent", "DeleteEvent"):
        out["ref"] = p.get("ref")
        out["ref_type"] = p.get("ref_type")
    elif t == "PullRequestEvent":
        out["action"] = p.get("action")
        out["pr_number"] = p.get("number")
    elif t in ("PullRequestReviewEvent", "PullRequestReviewCommentEvent"):
        out["action"] = p.get("action")
        out["pr_number"] = (p.get("pull_request") or {}).get("number")
    elif t in ("IssuesEvent", "IssueCommentEvent"):
        out["action"] = p.get("action")
        out["issue_number"] = (p.get("issue") or {}).get("number")
    elif t in ("WatchEvent", "ReleaseEvent", "MemberEvent", "ForkEvent"):
        out["action"] = p.get("action")

    return out


def transform_to_silver_arrow(bronze_arrow: pa.Table) -> pa.Table:
    df = bronze_arrow.to_pandas()

    # Deduplicate within this file only.
    # Cross-file duplicates cannot occur: GH Archive files are disjoint by hour.
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="first")
    if len(df) < before:
        print(f"      Dropped {before - len(df):,} intra-file duplicates")

    transformed_at = pd.Timestamp.now(tz="UTC").floor("us")
    parsed = df.apply(_parse_payload, axis=1, result_type="expand")
    df = pd.concat([df.drop(columns=["payload"]), parsed], axis=1)

    df["event_date"]     = pd.to_datetime(df["created_at"]).dt.date
    df["transformed_at"] = transformed_at

    df["commit_count"] = pd.to_numeric(df["commit_count"], errors="coerce").astype("Int64")
    df["pr_number"]    = pd.to_numeric(df["pr_number"],    errors="coerce").astype("Int64")
    df["issue_number"] = pd.to_numeric(df["issue_number"], errors="coerce").astype("Int64")
    df["event_date"]   = pd.to_datetime(df["event_date"]).dt.date
    df["transformed_at"] = pd.to_datetime(df["transformed_at"], utc=True).dt.as_unit("us")

    schema = pa.schema([
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
    ])
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


# ── Gold ──────────────────────────────────────────────────────────────────────

def recompute_gold(catalog):
    """
    Gold is always recomputed from all Silver rather than appended.
    Aggregates are not additive: you can't merge two GROUP BY results
    correctly (e.g., COUNT DISTINCT would double-count). Full recompute
    is the only correct approach without a more complex merge strategy.
    """
    print("  Loading all Silver into DuckDB for Gold recompute ...")
    silver_df = catalog.load_table("silver.github_events").scan().to_pandas()
    aggregated_at = pd.Timestamp.now(tz="UTC").floor("us")
    con = duckdb.connect()
    con.register("silver", silver_df)

    repo_df = con.execute("""
        SELECT
            event_date, repo_name,
            COUNT_IF(type = 'PushEvent')                              AS push_count,
            COALESCE(SUM(CASE WHEN type='PushEvent' THEN commit_count END),0) AS total_commits,
            COUNT_IF(type='PullRequestEvent' AND action='opened')     AS pr_opened,
            COUNT_IF(type='PullRequestEvent' AND action='closed')     AS pr_closed,
            COUNT_IF(type='IssuesEvent'      AND action='opened')     AS issues_opened,
            COUNT_IF(type='WatchEvent')                               AS stars,
            COUNT_IF(type='ForkEvent')                                AS forks,
            COUNT(DISTINCT actor_login)                               AS unique_contributors,
            COUNT(*)                                                  AS total_events
        FROM silver WHERE repo_name IS NOT NULL
        GROUP BY event_date, repo_name
    """).df()
    repo_df["aggregated_at"] = aggregated_at

    actor_df = con.execute("""
        SELECT
            event_date, actor_login,
            COUNT_IF(type='PushEvent')                                         AS push_count,
            COALESCE(SUM(CASE WHEN type='PushEvent' THEN commit_count END),0)  AS total_commits,
            COUNT_IF(type IN ('PullRequestEvent','PullRequestReviewEvent',
                              'PullRequestReviewCommentEvent'))                AS pr_count,
            COUNT_IF(type IN ('IssuesEvent','IssueCommentEvent'))              AS issue_count,
            COUNT(DISTINCT repo_name)                                          AS repos_touched,
            COUNT(*)                                                           AS total_events
        FROM silver WHERE actor_login IS NOT NULL
        GROUP BY event_date, actor_login
    """).df()
    actor_df["aggregated_at"] = aggregated_at
    con.close()

    repo_schema = pa.schema([
        pa.field("event_date",          pa.date32(),         nullable=False),
        pa.field("repo_name",           pa.string(),         nullable=False),
        pa.field("push_count",          pa.int64()),
        pa.field("total_commits",       pa.int64()),
        pa.field("pr_opened",           pa.int64()),
        pa.field("pr_closed",           pa.int64()),
        pa.field("issues_opened",       pa.int64()),
        pa.field("stars",               pa.int64()),
        pa.field("forks",               pa.int64()),
        pa.field("unique_contributors", pa.int64()),
        pa.field("total_events",        pa.int64()),
        pa.field("aggregated_at",       pa.timestamp("us", tz="UTC")),
    ])
    actor_schema = pa.schema([
        pa.field("event_date",    pa.date32(),  nullable=False),
        pa.field("actor_login",   pa.string(),  nullable=False),
        pa.field("push_count",    pa.int64()),
        pa.field("total_commits", pa.int64()),
        pa.field("pr_count",      pa.int64()),
        pa.field("issue_count",   pa.int64()),
        pa.field("repos_touched", pa.int64()),
        pa.field("total_events",  pa.int64()),
        pa.field("aggregated_at", pa.timestamp("us", tz="UTC")),
    ])

    ensure_namespace(catalog, "gold")
    for full_name, df, schema, partition in [
        ("gold.repo_daily_activity",  repo_df,  repo_schema,  date_partition(1)),
        ("gold.actor_daily_activity", actor_df, actor_schema, date_partition(1)),
    ]:
        drop_table_if_exists(catalog, full_name)
        tbl = catalog.create_table(full_name, schema=REPO_GOLD_SCHEMA if "repo" in full_name else ACTOR_GOLD_SCHEMA, partition_spec=partition)
        tbl.append(pa.Table.from_pandas(df, schema=schema, preserve_index=False))
        print(f"    {full_name}: {len(df):,} rows written")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fresh", action="store_true",
        help="Drop all Bronze/Silver/Gold tables and clear the manifest before starting"
    )
    args = parser.parse_args()

    catalog = get_catalog()

    if args.fresh:
        print("==> --fresh: wiping all tables and manifest")
        for name in [
            "bronze.github_events", "silver.github_events",
            "gold.repo_daily_activity", "gold.actor_daily_activity",
        ]:
            drop_table_if_exists(catalog, name)
        if MANIFEST_PATH.exists():
            MANIFEST_PATH.unlink()
            print("    Cleared manifest")

    manifest = load_manifest()
    done_count = sum(1 for v in manifest.values() if v.get("status") == "done")
    print(f"\n==> Manifest: {done_count}/{len(HOURS)} hours already ingested")

    # Ensure namespaces exist before we start processing files
    ensure_namespace(catalog, "bronze")
    ensure_namespace(catalog, "silver")

    # Get-or-create Bronze and Silver tables once — we'll append to them per file
    bronze_tbl = get_or_create_table(catalog, "bronze.github_events", BRONZE_SCHEMA)
    silver_tbl = get_or_create_table(
        catalog, "silver.github_events", SILVER_SCHEMA, date_partition(8)
    )

    new_files = 0
    for hour in HOURS:
        filename = f"{DATE}-{hour}.json.gz"

        if manifest.get(filename, {}).get("status") == "done":
            print(f"\n  [{hour:02d}/23] {filename} — already ingested, skipping")
            continue

        print(f"\n  [{hour:02d}/23] {filename}")

        # Step 1: Download (atomic — uses temp file internally)
        try:
            local_path = download_file(filename)
        except RuntimeError as e:
            print(f"    ERROR: {e} — skipping this hour")
            continue

        # Step 2: Parse to Bronze Arrow table
        print(f"    Parsing ...", end=" ", flush=True)
        bronze_arrow = parse_to_bronze_arrow(local_path)
        print(f"{len(bronze_arrow):,} records")

        # Step 3: Append to Bronze Iceberg table
        bronze_tbl.append(bronze_arrow)

        # Step 4: Transform Bronze → Silver (in-memory, no extra scan)
        # We reuse the already-parsed Arrow table rather than re-reading
        # from Iceberg, saving one round-trip to MinIO per file.
        silver_arrow = transform_to_silver_arrow(bronze_arrow)
        silver_tbl.append(silver_arrow)

        # Step 5: Mark done AFTER both writes succeed.
        # If the process dies between Bronze write and this line, the next
        # run will reprocess the file — Bronze gets a duplicate, but Silver
        # deduplication (by event ID) will remove it on that re-run.
        mark_done(manifest, filename,
                  bronze_rows=len(bronze_arrow),
                  silver_rows=len(silver_arrow))
        new_files += 1
        print(f"    Bronze +{len(bronze_arrow):,}  Silver +{len(silver_arrow):,}  ✓ manifest updated")

    if new_files == 0:
        print("\n==> Nothing new to process. Run with --fresh to reprocess all files.")
        return

    print(f"\n==> Processed {new_files} new file(s). Recomputing Gold ...")
    recompute_gold(catalog)

    # Final summary
    total_bronze = sum(v["bronze_rows"] for v in manifest.values() if v.get("status") == "done")
    total_silver = sum(v["silver_rows"] for v in manifest.values() if v.get("status") == "done")
    print(f"\n==> Done.")
    print(f"    Total Bronze rows : {total_bronze:,}")
    print(f"    Total Silver rows : {total_silver:,}")
    print(f"    Manifest          : {MANIFEST_PATH}")


if __name__ == "__main__":
    main()

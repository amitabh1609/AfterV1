"""
Silver transform: reads bronze.github_events, deduplicates, parses payload
into typed columns, and writes to silver.github_events on MinIO.
Usage: .venv/bin/python ingestion/silver_transform.py
"""

import json
import os

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

load_dotenv()

CATALOG_URI = "sqlite:///data/processed/iceberg_catalog.db"
BRONZE_TABLE = "bronze.github_events"
SILVER_NS = "silver"
SILVER_TABLE = "silver.github_events"

ICEBERG_SCHEMA = Schema(
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

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=8, field_id=1000, transform=IdentityTransform(), name="event_date"),
)


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


def get_or_create_table(catalog: SqlCatalog):
    try:
        catalog.create_namespace(SILVER_NS)
        print(f"  Created namespace '{SILVER_NS}'")
    except NamespaceAlreadyExistsError:
        print(f"  Namespace '{SILVER_NS}' already exists")

    try:
        catalog.drop_table(SILVER_TABLE)
        print(f"  Dropped stale table '{SILVER_TABLE}'")
    except Exception:
        pass

    table = catalog.create_table(SILVER_TABLE, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)
    print(f"  Created table '{SILVER_TABLE}'")
    return table


def _parse_payload(row) -> dict:
    try:
        p = json.loads(row["payload"]) if isinstance(row["payload"], str) else {}
    except (json.JSONDecodeError, TypeError):
        p = {}

    t = row["type"]
    result = {
        "action":       None,
        "ref":          None,
        "ref_type":     None,
        "commit_count": None,
        "pr_number":    None,
        "issue_number": None,
    }

    if t == "PushEvent":
        result["ref"]          = p.get("ref")
        result["commit_count"] = p.get("size")

    elif t in ("CreateEvent", "DeleteEvent"):
        result["ref"]      = p.get("ref")
        result["ref_type"] = p.get("ref_type")

    elif t == "PullRequestEvent":
        result["action"]    = p.get("action")
        result["pr_number"] = p.get("number")

    elif t in ("PullRequestReviewEvent", "PullRequestReviewCommentEvent"):
        result["action"]    = p.get("action")
        pr = p.get("pull_request") or {}
        result["pr_number"] = pr.get("number")

    elif t in ("IssuesEvent", "IssueCommentEvent"):
        result["action"] = p.get("action")
        issue = p.get("issue") or {}
        result["issue_number"] = issue.get("number")

    elif t in ("WatchEvent", "ReleaseEvent", "MemberEvent", "ForkEvent"):
        result["action"] = p.get("action")

    return result


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Deduplicate by event id (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="first")
    dupes = before - len(df)
    if dupes:
        print(f"  Removed {dupes:,} duplicate rows")

    transformed_at = pd.Timestamp.now(tz="UTC").floor("us")

    parsed = df.apply(_parse_payload, axis=1, result_type="expand")
    df = pd.concat([df.drop(columns=["payload"]), parsed], axis=1)

    df["event_date"]     = df["created_at"].dt.date
    df["transformed_at"] = transformed_at

    # Ensure correct dtypes
    df["commit_count"]   = pd.to_numeric(df["commit_count"], errors="coerce").astype("Int64")
    df["pr_number"]      = pd.to_numeric(df["pr_number"],    errors="coerce").astype("Int64")
    df["issue_number"]   = pd.to_numeric(df["issue_number"], errors="coerce").astype("Int64")
    df["event_date"]     = pd.to_datetime(df["event_date"]).dt.date
    df["transformed_at"] = pd.to_datetime(df["transformed_at"], utc=True).dt.as_unit("us")

    return df


def to_arrow(df: pd.DataFrame) -> pa.Table:
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


def main():
    print("==> Step 1: Connect to catalog")
    catalog = get_catalog()

    print("\n==> Step 2: Read bronze table")
    bronze = catalog.load_table(BRONZE_TABLE)
    df = bronze.scan().to_pandas()
    print(f"  Bronze rows : {len(df):,}")

    print("\n==> Step 3: Transform")
    df = transform(df)
    print(f"  Silver rows : {len(df):,}")

    print("\n==> Step 4: Create silver table")
    silver = get_or_create_table(catalog)

    print("\n==> Step 5: Write to silver table")
    arrow = to_arrow(df)
    silver.append(arrow)
    print("  Write complete.")

    print("\n==> Step 6: Verify")
    result = silver.scan().to_pandas()
    print(f"  Rows in Iceberg : {len(result):,}")
    print(f"\n  Event type breakdown:")
    print(result["type"].value_counts().to_string())
    print(f"\n  Sample PushEvent rows:")
    push = result[result["type"] == "PushEvent"][["actor_login", "repo_name", "ref", "commit_count"]].head(3)
    print(push.to_string(index=False))
    print(f"\n  Sample PullRequestEvent rows:")
    pr = result[result["type"] == "PullRequestEvent"][["actor_login", "repo_name", "action", "pr_number"]].head(3)
    print(pr.to_string(index=False))


if __name__ == "__main__":
    main()

"""
Gold aggregation: reads silver.github_events and writes two analytical tables:
  - gold.repo_daily_activity   : per-repo per-day event counts
  - gold.actor_daily_activity  : per-actor per-day event counts
Usage: .venv/bin/python ingestion/gold_aggregate.py
"""

import os

import duckdb
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
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
SILVER_TABLE = "silver.github_events"
GOLD_NS = "gold"

# ── Schemas ─────────────────────────────────────────────────────────────────

REPO_SCHEMA = Schema(
    NestedField(1,  "event_date",           DateType(),      required=True),
    NestedField(2,  "repo_name",            StringType(),    required=True),
    NestedField(3,  "push_count",           LongType()),
    NestedField(4,  "total_commits",        LongType()),
    NestedField(5,  "pr_opened",            LongType()),
    NestedField(6,  "pr_closed",            LongType()),
    NestedField(7,  "issues_opened",        LongType()),
    NestedField(8,  "stars",                LongType()),
    NestedField(9,  "forks",                LongType()),
    NestedField(10, "unique_contributors",  LongType()),
    NestedField(11, "total_events",         LongType()),
    NestedField(12, "aggregated_at",        TimestamptzType()),
)

ACTOR_SCHEMA = Schema(
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

DATE_PARTITION = lambda source_id: PartitionSpec(
    PartitionField(source_id=source_id, field_id=1000, transform=IdentityTransform(), name="event_date")
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


def recreate_table(catalog, full_name, schema, partition_spec):
    try:
        catalog.drop_table(full_name)
        print(f"  Dropped stale '{full_name}'")
    except Exception:
        pass
    t = catalog.create_table(full_name, schema=schema, partition_spec=partition_spec)
    print(f"  Created '{full_name}'")
    return t


def build_repo_daily(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            event_date,
            repo_name,
            COUNT_IF(type = 'PushEvent')                                AS push_count,
            COALESCE(SUM(CASE WHEN type = 'PushEvent'
                         THEN commit_count END), 0)                     AS total_commits,
            COUNT_IF(type = 'PullRequestEvent' AND action = 'opened')   AS pr_opened,
            COUNT_IF(type = 'PullRequestEvent' AND action = 'closed')   AS pr_closed,
            COUNT_IF(type = 'IssuesEvent'      AND action = 'opened')   AS issues_opened,
            COUNT_IF(type = 'WatchEvent')                               AS stars,
            COUNT_IF(type = 'ForkEvent')                                AS forks,
            COUNT(DISTINCT actor_login)                                 AS unique_contributors,
            COUNT(*)                                                    AS total_events
        FROM silver
        WHERE repo_name IS NOT NULL
        GROUP BY event_date, repo_name
        ORDER BY event_date, total_events DESC
    """).df()


def build_actor_daily(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            event_date,
            actor_login,
            COUNT_IF(type = 'PushEvent')                                AS push_count,
            COALESCE(SUM(CASE WHEN type = 'PushEvent'
                         THEN commit_count END), 0)                     AS total_commits,
            COUNT_IF(type IN ('PullRequestEvent',
                              'PullRequestReviewEvent',
                              'PullRequestReviewCommentEvent'))         AS pr_count,
            COUNT_IF(type IN ('IssuesEvent', 'IssueCommentEvent'))      AS issue_count,
            COUNT(DISTINCT repo_name)                                   AS repos_touched,
            COUNT(*)                                                    AS total_events
        FROM silver
        WHERE actor_login IS NOT NULL
        GROUP BY event_date, actor_login
        ORDER BY event_date, total_events DESC
    """).df()


def to_arrow_repo(df: pd.DataFrame, aggregated_at) -> pa.Table:
    df["aggregated_at"] = aggregated_at
    schema = pa.schema([
        pa.field("event_date",          pa.date32(),              nullable=False),
        pa.field("repo_name",           pa.string(),              nullable=False),
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
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def to_arrow_actor(df: pd.DataFrame, aggregated_at) -> pa.Table:
    df["aggregated_at"] = aggregated_at
    schema = pa.schema([
        pa.field("event_date",    pa.date32(),             nullable=False),
        pa.field("actor_login",   pa.string(),             nullable=False),
        pa.field("push_count",    pa.int64()),
        pa.field("total_commits", pa.int64()),
        pa.field("pr_count",      pa.int64()),
        pa.field("issue_count",   pa.int64()),
        pa.field("repos_touched", pa.int64()),
        pa.field("total_events",  pa.int64()),
        pa.field("aggregated_at", pa.timestamp("us", tz="UTC")),
    ])
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def main():
    aggregated_at = pd.Timestamp.now(tz="UTC").floor("us")

    print("==> Step 1: Connect to catalog")
    catalog = get_catalog()

    print("\n==> Step 2: Load silver into DuckDB")
    silver_tbl = catalog.load_table(SILVER_TABLE)
    silver_df = silver_tbl.scan().to_pandas()
    print(f"  Loaded {len(silver_df):,} silver rows")

    con = duckdb.connect()
    con.register("silver", silver_df)

    print("\n==> Step 3: Build gold.repo_daily_activity")
    repo_df = build_repo_daily(con)
    print(f"  {len(repo_df):,} repo-day rows")

    print("\n==> Step 4: Build gold.actor_daily_activity")
    actor_df = build_actor_daily(con)
    print(f"  {len(actor_df):,} actor-day rows")

    try:
        catalog.create_namespace(GOLD_NS)
        print(f"\n  Created namespace '{GOLD_NS}'")
    except NamespaceAlreadyExistsError:
        print(f"\n  Namespace '{GOLD_NS}' already exists")

    print("\n==> Step 5: Write gold.repo_daily_activity")
    repo_tbl = recreate_table(
        catalog, "gold.repo_daily_activity",
        REPO_SCHEMA, DATE_PARTITION(source_id=1)
    )
    repo_tbl.append(to_arrow_repo(repo_df, aggregated_at))
    print("  Write complete.")

    print("\n==> Step 6: Write gold.actor_daily_activity")
    actor_tbl = recreate_table(
        catalog, "gold.actor_daily_activity",
        ACTOR_SCHEMA, DATE_PARTITION(source_id=1)
    )
    actor_tbl.append(to_arrow_actor(actor_df, aggregated_at))
    print("  Write complete.")

    print("\n==> Step 7: Spot-check results")

    print("\n  Top 10 repos by total events:")
    top_repos = con.execute("""
        SELECT repo_name, total_events, push_count, stars, forks
        FROM repo_df
        ORDER BY total_events DESC LIMIT 10
    """).df()
    print(top_repos.to_string(index=False))

    print("\n  Top 10 actors by commits pushed:")
    top_actors = con.execute("""
        SELECT actor_login, total_commits, push_count, repos_touched
        FROM actor_df
        ORDER BY total_commits DESC LIMIT 10
    """).df()
    print(top_actors.to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()

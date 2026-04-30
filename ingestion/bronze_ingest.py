"""
Bronze ingestion: reads GitHub Archive .json.gz and writes raw events
to an Iceberg table (bronze.github_events) on local MinIO.
Usage: .venv/bin/python ingestion/bronze_ingest.py
"""

import gzip
import json
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

load_dotenv()

RAW_FILE = "data/raw/2024-01-01-0.json.gz"
CATALOG_URI = "sqlite:///data/processed/iceberg_catalog.db"
NAMESPACE = "bronze"
TABLE_NAME = "github_events"
FULL_TABLE = f"{NAMESPACE}.{TABLE_NAME}"

ICEBERG_SCHEMA = Schema(
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
        catalog.create_namespace(NAMESPACE)
        print(f"  Created namespace '{NAMESPACE}'")
    except NamespaceAlreadyExistsError:
        print(f"  Namespace '{NAMESPACE}' already exists")

    try:
        catalog.drop_table(FULL_TABLE)
        print(f"  Dropped stale table '{FULL_TABLE}'")
    except Exception:
        pass
    try:
        table = catalog.create_table(FULL_TABLE, schema=ICEBERG_SCHEMA)
        print(f"  Created table '{FULL_TABLE}'")
    except TableAlreadyExistsError:
        table = catalog.load_table(FULL_TABLE)
        print(f"  Loaded existing table '{FULL_TABLE}'")
    return table


def read_raw_to_arrow(path: str) -> pa.Table:
    print(f"  Parsing {path} ...")
    records = []
    ingested_at = datetime.now(timezone.utc).isoformat()

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

    print(f"  Parsed {len(records):,} records")

    df = pd.DataFrame(records)
    df["created_at"]  = pd.to_datetime(df["created_at"],  utc=True).dt.as_unit("us")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True).dt.as_unit("us")

    return pa.Table.from_pandas(df, preserve_index=False)


def main():
    print("==> Step 1: Connect to Iceberg catalog")
    catalog = get_catalog()

    print("\n==> Step 2: Create namespace + table if needed")
    table = get_or_create_table(catalog)

    print("\n==> Step 3: Read raw GitHub Archive file")
    arrow_table = read_raw_to_arrow(RAW_FILE)

    print(f"\n==> Step 4: Write {len(arrow_table):,} rows to '{FULL_TABLE}'")
    table.append(arrow_table)
    print("  Write complete.")

    print("\n==> Step 5: Verify — scan table back")
    df = table.scan().to_pandas()
    print(f"  Rows in Iceberg : {len(df):,}")
    print(f"  Columns         : {list(df.columns)}")
    print(f"\n  Sample (5 rows):")
    print(df[["type", "actor_login", "repo_name", "created_at"]].head(5).to_string(index=False))


if __name__ == "__main__":
    main()

"""
Dagster Software-Defined Assets for the GitHub Archive lakehouse pipeline.

Asset lineage:
    bronze_github_events
         └── silver_github_events
         └── silver_quarantine
                  └── gold_tables

Each asset is daily-partitioned: materialising partition "2024-01-02" ingests
all 24 hourly files for that date through Bronze → Silver → Gold.

Run via Dagster UI or CLI:
    dagster dev -f dagster/__init__.py           # local UI on port 3000
    dagster asset materialize -f dagster/__init__.py --select '*'
    dagster asset materialize -f dagster/__init__.py \
        --select '*' --partition 2024-01-02
"""

import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Output,
    asset,
)

# Allow importing from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.ingest_incremental import (
    BRONZE_SCHEMA,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMA,
    date_partition,
    download_file,
    ensure_namespace,
    get_catalog,
    get_or_create_table,
    mark_done,
    load_manifest,
    parse_to_bronze_arrow,
    recompute_gold,
    save_manifest,
    transform_to_silver_and_quarantine,
)

PARTITIONS = DailyPartitionsDefinition(start_date="2024-01-01")
HOURS = range(24)


@asset(
    partitions_def=PARTITIONS,
    group_name="bronze",
    description="Raw GitHub Archive events ingested into the Bronze Iceberg table.",
)
def bronze_github_events(context: AssetExecutionContext):
    day = context.partition_key          # e.g. "2024-01-01"
    catalog = get_catalog()
    ensure_namespace(catalog, "bronze")
    tbl = get_or_create_table(catalog, "bronze.github_events", BRONZE_SCHEMA)

    manifest   = load_manifest()
    total_rows = 0
    files_done = 0

    for hour in HOURS:
        filename = f"{day}-{hour}.json.gz"
        if manifest.get(filename, {}).get("status") == "done":
            context.log.info(f"  {filename} already in manifest, skipping")
            files_done += 1
            total_rows += manifest[filename]["bronze_rows"]
            continue

        context.log.info(f"  Downloading {filename}")
        local_path = download_file(filename)

        context.log.info(f"  Parsing {filename}")
        bronze_arrow = parse_to_bronze_arrow(local_path)
        tbl.append(bronze_arrow)

        total_rows += len(bronze_arrow)
        files_done += 1
        context.log.info(f"  {filename}: {len(bronze_arrow):,} rows appended to Bronze")

    return Output(
        value={"day": day, "files": files_done, "bronze_rows": total_rows},
        metadata={"bronze_rows": total_rows, "files_processed": files_done},
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[bronze_github_events],
    group_name="silver",
    description="Cleaned, typed, deduplicated events in the Silver Iceberg table.",
)
def silver_github_events(context: AssetExecutionContext):
    day = context.partition_key
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")
    silver_tbl = get_or_create_table(
        catalog, "silver.github_events", SILVER_SCHEMA, date_partition(8)
    )

    manifest    = load_manifest()
    total_silver = 0

    for hour in HOURS:
        filename = f"{day}-{hour}.json.gz"
        if manifest.get(filename, {}).get("status") == "done":
            total_silver += manifest[filename].get("silver_rows", 0)
            continue

        local_path   = Path("data/raw") / filename
        bronze_arrow = parse_to_bronze_arrow(local_path)
        silver_arrow, _, dupe_count = transform_to_silver_and_quarantine(
            bronze_arrow, source_file=filename
        )
        silver_tbl.append(silver_arrow)
        total_silver += len(silver_arrow)
        context.log.info(
            f"  {filename}: {len(silver_arrow):,} silver rows"
            + (f"  ({dupe_count} dupes dropped)" if dupe_count else "")
        )

    return Output(
        value={"day": day, "silver_rows": total_silver},
        metadata={"silver_rows": total_silver},
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[bronze_github_events],
    group_name="silver",
    description="Bad records routed to quarantine with error reason and source file.",
)
def silver_quarantine(context: AssetExecutionContext):
    day = context.partition_key
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")
    q_tbl = get_or_create_table(catalog, "silver.quarantine", QUARANTINE_SCHEMA)

    manifest      = load_manifest()
    total_q       = 0

    for hour in HOURS:
        filename = f"{day}-{hour}.json.gz"
        if manifest.get(filename, {}).get("status") == "done":
            total_q += manifest[filename].get("quarantine_rows", 0)
            continue

        local_path   = Path("data/raw") / filename
        bronze_arrow = parse_to_bronze_arrow(local_path)
        _, quarantine_arrow, _ = transform_to_silver_and_quarantine(
            bronze_arrow, source_file=filename
        )

        if quarantine_arrow is not None:
            q_tbl.append(quarantine_arrow)
            total_q += len(quarantine_arrow)
            context.log.info(f"  {filename}: {len(quarantine_arrow)} quarantined rows")

        # Mark done only after both silver and quarantine are written.
        # Silver asset runs first (dep order); we update manifest here.
        silver_arrow, _, _ = transform_to_silver_and_quarantine(
            bronze_arrow, source_file=filename
        )
        mark_done(
            manifest, filename,
            bronze_rows=len(bronze_arrow),
            silver_rows=len(silver_arrow),
            quarantine_rows=(len(quarantine_arrow) if quarantine_arrow else 0),
        )

    return Output(
        value={"day": day, "quarantine_rows": total_q},
        metadata={"quarantine_rows": total_q},
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[silver_github_events, silver_quarantine],
    group_name="gold",
    description="Daily repo and actor activity aggregates rebuilt from all Silver data.",
)
def gold_tables(context: AssetExecutionContext):
    """
    Gold is always a full recompute from all Silver rows (not just this partition).
    COUNT DISTINCT aggregates cannot be safely merged across partial results.
    """
    catalog = get_catalog()
    context.log.info("Recomputing Gold from all Silver rows ...")
    recompute_gold(catalog)

    silver_count = int(
        catalog.load_table("silver.github_events")
        .metadata.snapshots[-1]
        .summary.additional_properties.get("total-records", 0)
    )
    context.log.info(f"Gold recompute complete. Silver total: {silver_count:,} rows")

    return Output(
        value={"silver_rows_used": silver_count},
        metadata={"silver_rows_used": silver_count},
    )

"""
Soda Core data quality checks for the GitHub Archive lakehouse.

Loads each Iceberg layer from MinIO into DuckDB, runs Soda checks,
prints a pass/fail report, and saves results to quality/results/.

Usage:
    .venv/bin/python quality/soda_checks.py
    .venv/bin/python quality/soda_checks.py --layer silver   # single layer
    .venv/bin/python quality/soda_checks.py --fail-fast       # stop on first failure
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from soda.scan import Scan

load_dotenv()

CATALOG_URI   = "sqlite:///data/processed/iceberg_catalog.db"
RESULTS_DIR   = Path("quality/results")
CHECKS_DIR    = Path("quality/checks")
RESULTS_FILE  = RESULTS_DIR / "latest_check_results.json"

S3_CONFIG = {
    "uri": CATALOG_URI,
    "warehouse": f"s3://{os.getenv('S3_BUCKET', 'lakehouse')}",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    "s3.endpoint":          os.getenv("S3_ENDPOINT",   "http://localhost:9000"),
    "s3.access-key-id":     os.getenv("S3_ACCESS_KEY", "minio"),
    "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "minio123"),
    "s3.path-style-access": "true",
}

LAYERS = {
    "bronze": {
        "table":      "bronze.github_events",
        "alias":      "bronze_events",
        "checks":     CHECKS_DIR / "bronze_checks.yml",
    },
    "silver": {
        "table":      "silver.github_events",
        "alias":      "silver_events",
        "checks":     CHECKS_DIR / "silver_checks.yml",
    },
    "gold_repo": {
        "table":      "gold.repo_daily_activity",
        "alias":      "gold_repo",
        "checks":     CHECKS_DIR / "gold_checks.yml",
    },
    "gold_actor": {
        "table":      "gold.actor_daily_activity",
        "alias":      "gold_actor",
        "checks":     CHECKS_DIR / "gold_checks.yml",
    },
}


def get_catalog() -> SqlCatalog:
    return SqlCatalog("local", **S3_CONFIG)


def load_table_to_duckdb(con: duckdb.DuckDBPyConnection, catalog: SqlCatalog,
                          iceberg_name: str, alias: str) -> int:
    """Read Iceberg table into DuckDB and register as alias. Returns row count."""
    print(f"  Loading {iceberg_name} → DuckDB '{alias}' ...", end=" ", flush=True)
    df = catalog.load_table(iceberg_name).scan().to_pandas()
    con.register(alias, df)
    print(f"{len(df):,} rows")
    return len(df)


def run_layer(layer_name: str, config: dict, catalog: SqlCatalog,
              con: duckdb.DuckDBPyConnection) -> dict:
    """Run Soda checks for one layer. Returns result dict."""
    print(f"\n{'─' * 60}")
    print(f"  Layer: {layer_name.upper()}")
    print(f"{'─' * 60}")

    row_count = load_table_to_duckdb(con, catalog, config["table"], config["alias"])

    scan = Scan()
    scan.set_data_source_name("duckdb")
    scan.add_configuration_yaml_str(f"""
data_sources:
  duckdb:
    type: duckdb
    connection:
      database: ':memory:'
""")
    # Inject the DuckDB connection directly
    scan._data_source_manager._data_sources = {}
    scan.add_duckdb_connection(duckdb_connection=con, data_source_name="duckdb")
    scan.add_sodacl_yaml_file(str(config["checks"]))
    scan.set_verbose(False)
    scan.execute()

    passed = sum(1 for r in scan.get_checks_fail_message() and [] or [] if True)

    results = scan.get_scan_results()
    checks  = results.get("checks", []) if results else []

    passed  = sum(1 for c in checks if c.get("outcome") == "pass")
    failed  = sum(1 for c in checks if c.get("outcome") == "fail")
    warned  = sum(1 for c in checks if c.get("outcome") == "warn")
    total   = len(checks)

    print(f"\n  Results: {passed} passed / {warned} warned / {failed} failed / {total} total")
    for c in checks:
        outcome = c.get("outcome", "?")
        name    = c.get("name", "unnamed")
        icon    = "✓" if outcome == "pass" else ("⚠" if outcome == "warn" else "✗")
        print(f"    {icon}  {name}")

    return {
        "layer":     layer_name,
        "table":     config["table"],
        "row_count": row_count,
        "passed":    passed,
        "warned":    warned,
        "failed":    failed,
        "total":     total,
        "details":   checks,
    }


def main():
    parser = argparse.ArgumentParser(description="Run Soda Core data quality checks")
    parser.add_argument("--layer",     choices=list(LAYERS), help="Run checks for one layer only")
    parser.add_argument("--fail-fast", action="store_true",  help="Stop after first layer with failures")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  GITHUB ARCHIVE LAKEHOUSE — DATA QUALITY REPORT")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    print("\n==> Connecting to catalog and DuckDB ...")
    catalog = get_catalog()
    con     = duckdb.connect()

    layers_to_run = {args.layer: LAYERS[args.layer]} if args.layer else LAYERS
    all_results   = []
    total_failed  = 0

    for layer_name, config in layers_to_run.items():
        try:
            result = run_layer(layer_name, config, catalog, con)
            all_results.append(result)
            total_failed += result["failed"]
            if args.fail_fast and result["failed"] > 0:
                print(f"\n  --fail-fast: stopping after failures in {layer_name}")
                break
        except Exception as e:
            print(f"\n  ERROR running {layer_name}: {e}")
            all_results.append({
                "layer": layer_name, "error": str(e),
                "passed": 0, "warned": 0, "failed": 0, "total": 0,
            })

    con.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    total_passed = sum(r.get("passed", 0) for r in all_results)
    total_warned = sum(r.get("warned", 0) for r in all_results)
    total_checks = sum(r.get("total",  0) for r in all_results)
    total_failed = sum(r.get("failed", 0) for r in all_results)

    print(f"\n{'═' * 60}")
    print(f"  SUMMARY")
    print(f"{'═' * 60}")
    print(f"  Total checks : {total_checks}")
    print(f"  Passed       : {total_passed}  ✓")
    print(f"  Warned       : {total_warned}  ⚠")
    print(f"  Failed       : {total_failed}  ✗")
    print(f"{'─' * 60}")
    for r in all_results:
        status = "ERROR" if "error" in r else f"{r['passed']}/{r['total']} passed"
        print(f"  {r['layer']:<20} {status}")
    print(f"{'═' * 60}\n")

    # ── Save results ──────────────────────────────────────────────────────────
    output = {
        "run_at":       datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": total_checks, "passed": total_passed,
            "warned": total_warned, "failed": total_failed,
        },
        "layers": all_results,
    }
    RESULTS_FILE.write_text(json.dumps(output, indent=2, default=str))
    print(f"  Results saved to {RESULTS_FILE}")

    sys.exit(1 if total_failed > 0 else 0)


if __name__ == "__main__":
    main()

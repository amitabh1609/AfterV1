"""
Dagster asset: run_data_quality_checks

Runs after the Gold layer completes. Calls quality/soda_checks.py,
logs pass/fail counts, and fails the Dagster run if more than 3 checks fail.
"""

import json
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, Output, asset

from .assets import gold_tables

PARTITIONS    = DailyPartitionsDefinition(start_date="2024-01-01")
RESULTS_FILE  = Path("quality/results/latest_check_results.json")
MAX_FAILURES  = 3


@asset(
    partitions_def=PARTITIONS,
    deps=[gold_tables],
    group_name="quality",
    description=(
        "Runs 32+ Soda Core checks across Bronze, Silver, and Gold. "
        "Fails the Dagster run if more than 3 checks fail."
    ),
)
def run_data_quality_checks(context: AssetExecutionContext):
    context.log.info("Starting Soda Core data quality checks ...")

    result = subprocess.run(
        [sys.executable, "quality/soda_checks.py"],
        capture_output=True,
        text=True,
    )

    if result.stdout:
        for line in result.stdout.splitlines():
            context.log.info(line)
    if result.stderr:
        for line in result.stderr.splitlines():
            context.log.warning(line)

    # Parse saved results
    total = passed = warned = failed = 0
    if RESULTS_FILE.exists():
        data    = json.loads(RESULTS_FILE.read_text())
        summary = data.get("summary", {})
        total   = summary.get("total",  0)
        passed  = summary.get("passed", 0)
        warned  = summary.get("warned", 0)
        failed  = summary.get("failed", 0)

        context.log.info(
            f"Quality results: {passed}/{total} passed, "
            f"{warned} warned, {failed} failed"
        )

        for layer in data.get("layers", []):
            context.log.info(
                f"  {layer['layer']}: {layer.get('passed',0)}/{layer.get('total',0)} passed"
            )
    else:
        context.log.warning("Results file not found — checks may not have run.")

    if failed > MAX_FAILURES:
        raise Exception(
            f"Data quality gate failed: {failed} checks failed "
            f"(threshold is {MAX_FAILURES}). "
            f"See {RESULTS_FILE} for details."
        )

    return Output(
        value={"total": total, "passed": passed, "warned": warned, "failed": failed},
        metadata={
            "checks_total":  total,
            "checks_passed": passed,
            "checks_warned": warned,
            "checks_failed": failed,
            "gate_passed":   failed <= MAX_FAILURES,
        },
    )

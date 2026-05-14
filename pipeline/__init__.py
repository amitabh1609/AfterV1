from dagster import Definitions

from .assets import (
    bronze_github_events,
    silver_github_events,
    silver_quarantine,
    gold_tables,
)
from .quality_asset import run_data_quality_checks
from .schedules import daily_ingest_schedule

defs = Definitions(
    assets=[
        bronze_github_events,
        silver_github_events,
        silver_quarantine,
        gold_tables,
        run_data_quality_checks,
    ],
    schedules=[daily_ingest_schedule],
)

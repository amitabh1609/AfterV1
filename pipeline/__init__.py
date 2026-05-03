from dagster import Definitions

from .assets import (
    bronze_github_events,
    silver_github_events,
    silver_quarantine,
    gold_tables,
)
from .schedules import daily_ingest_schedule

defs = Definitions(
    assets=[
        bronze_github_events,
        silver_github_events,
        silver_quarantine,
        gold_tables,
    ],
    schedules=[daily_ingest_schedule],
)

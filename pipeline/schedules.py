"""
Daily schedule: at 03:00 UTC materialise the previous day's partition.

GitHub Archive files for day D are published by ~02:00 UTC on day D+1,
so a 03:00 UTC trigger gives a comfortable buffer.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

daily_ingest_schedule = ScheduleDefinition(
    name="daily_github_ingest",
    cron_schedule="0 3 * * *",                      # 03:00 UTC every day
    target="*",                                      # materialise all assets
    default_status=DefaultScheduleStatus.RUNNING,
    description="Ingest previous day's GitHub Archive files at 03:00 UTC",
)

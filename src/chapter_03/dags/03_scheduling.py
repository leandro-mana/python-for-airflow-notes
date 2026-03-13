"""
Scheduling — Cron, Presets, and Data Intervals.

Airflow scheduling concepts:
- Cron expressions: "0 6 * * *" (6 AM daily)
- Presets: @daily, @hourly, @weekly, @monthly, @yearly
- Data intervals: the time PERIOD a DAG run covers (not when it executes)
- logical_date: the START of the data interval (replaces legacy execution_date)
- None: manually triggered only

Key insight: A @daily DAG with start_date=Jan 1 runs at the END of Jan 1
(i.e., midnight Jan 2), processing data FOR Jan 1.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


# --- DAG 1: Using a cron expression ---
@dag(
    dag_id="ch03_03a_cron_schedule",
    schedule="0 6 * * 1-5",  # 6 AM UTC, Monday through Friday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_03", "scheduling"],
    doc_md="Runs at 6 AM UTC on weekdays using a cron expression.",
)
def cron_schedule():
    @task
    def process(**context):
        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        print(f"logical_date (start of interval): {logical_date}")
        print(f"data_interval_start: {data_interval_start}")
        print(f"data_interval_end:   {data_interval_end}")
        print(f"This run processes data FROM {data_interval_start} TO {data_interval_end}")

    process()


cron_schedule()


# --- DAG 2: Using a preset ---
@dag(
    dag_id="ch03_03b_preset_schedule",
    schedule="@weekly",  # Equivalent to "0 0 * * 0" (midnight Sunday)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_03", "scheduling"],
    doc_md=(
        "Uses @weekly preset. Other presets:\n"
        "- @once, @hourly, @daily, @weekly, @monthly, @yearly\n"
        "- None = manual trigger only"
    ),
)
def preset_schedule():
    @task
    def weekly_report(**context):
        print(f"Weekly report for interval: {context['data_interval_start']}")

    weekly_report()


preset_schedule()

"""
Catchup & Backfill — Handling Historical Runs.

Two related but distinct concepts:

1. catchup (DAG parameter):
   - catchup=True:  Airflow creates runs for ALL missed intervals since start_date
   - catchup=False: Only the most recent interval runs (default recommendation)

2. Backfill (CLI command):
   - `airflow dags backfill --start-date 2024-01-01 --end-date 2024-01-31 dag_id`
   - Manually trigger runs for a date range, even with catchup=False
   - Useful for reprocessing historical data after a bug fix

Best practice: Set catchup=False and use backfill when you explicitly need historical runs.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


# --- catchup=False (recommended default) ---
@dag(
    dag_id="ch03_04a_no_catchup",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Only run the latest interval
    default_args={"retries": 2},
    tags=["chapter_03", "catchup"],
    doc_md=(
        "catchup=False: Only the most recent interval runs.\n\n"
        "Even though start_date is Jan 1 2024, Airflow will NOT create runs\n"
        "for every day between then and now. Only today's run is created."
    ),
)
def no_catchup():
    @task
    def process(**context):
        print(f"Processing data for: {context['logical_date']}")
        print("catchup=False — only the latest interval runs")

    process()


no_catchup()


# --- catchup=True (use with caution) ---
@dag(
    dag_id="ch03_04b_with_catchup",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),  # Recent date to avoid thousands of backlog runs
    catchup=True,  # Create runs for ALL missed intervals
    max_active_runs=3,  # Limit concurrent catchup runs
    default_args={"retries": 2},
    tags=["chapter_03", "catchup"],
    doc_md=(
        "catchup=True: Airflow creates a run for EVERY missed interval.\n\n"
        "**Warning:** If start_date is far in the past, this creates a huge backlog.\n"
        "Always set max_active_runs to limit concurrency during catchup.\n\n"
        "Idempotency is critical here — each run must produce correct results\n"
        "regardless of when it actually executes."
    ),
)
def with_catchup():
    @task
    def process_idempotent(**context):
        logical_date = context["logical_date"]
        print(f"Processing data for: {logical_date}")
        print("This task MUST be idempotent — same result if re-run for this date")
        print(f"Example: SELECT * FROM events WHERE date = '{logical_date.date()}'")

    process_idempotent()


with_catchup()

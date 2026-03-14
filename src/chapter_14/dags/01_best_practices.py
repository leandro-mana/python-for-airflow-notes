"""
Best Practices — Production-Quality DAG Patterns.

This DAG demonstrates Airflow best practices:

1. Keep top-level code minimal (fast DAG parsing)
2. Use from __future__ import annotations (always)
3. Set meaningful default_args
4. Make tasks idempotent (safe to re-run)
5. Keep tasks atomic (single responsibility)
6. Use tags for UI organization
7. Add doc_md for documentation
8. Use catchup=False unless you need historical runs
9. Set start_date to a fixed date (never datetime.now())
10. Use template variables instead of Variable.get() at module level
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

# GOOD: Constants at module level are fine (no DB calls)
DAG_ID = "ch14_01_best_practices"
DEFAULT_ARGS = {
    "owner": "data_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# BAD — DO NOT DO THIS:
# from airflow.models import Variable
# config = Variable.get("config", deserialize_json=True)  # DB query on every parse!

# BAD — DO NOT DO THIS:
# import pandas as pd  # Heavy import at module level slows parsing!


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),  # GOOD: Fixed date, never datetime.now()
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chapter_14", "best_practices"],
    doc_md=__doc__,
    max_active_runs=1,  # Prevent overlapping runs
)
def best_practices():
    @task
    def idempotent_extract(**context) -> list[dict]:
        """
        BEST PRACTICE: Idempotent task.

        Uses logical_date to query a specific partition.
        Re-running this for the same date returns the same data.
        """
        ds = context["ds"]
        # In production: SELECT * FROM source WHERE date = '{ds}'
        return [{"date": ds, "value": 100}]

    @task
    def atomic_transform(data: list[dict]) -> list[dict]:
        """
        BEST PRACTICE: Atomic task.

        Does ONE thing (transform). Doesn't also load or send emails.
        If this fails, it can be retried without side effects.
        """
        # GOOD: Import heavy libraries inside the task, not at module level
        import json

        transformed = [{"date": r["date"], "value_doubled": r["value"] * 2} for r in data]
        print(f"Transformed: {json.dumps(transformed)}")
        return transformed

    @task
    def idempotent_load(data: list[dict], **context) -> None:
        """
        BEST PRACTICE: Idempotent load.

        Uses DELETE + INSERT (or ON CONFLICT) so re-running
        for the same date produces the same result.
        """
        ds = context["ds"]
        print(f"DELETE FROM target WHERE date = '{ds}'")
        print(f"INSERT INTO target: {data}")
        print("Load complete — idempotent")

    raw = idempotent_extract()
    clean = atomic_transform(raw)
    idempotent_load(clean)


best_practices()

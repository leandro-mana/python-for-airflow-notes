"""
Conditional Patterns — ShortCircuit, LatestOnly, and Cross-DAG Triggers.

Advanced control flow patterns:
- ShortCircuitOperator: Skip ALL downstream tasks if condition is False
- LatestOnlyOperator: Only run if this is the latest scheduled run (skip during catchup)
- TriggerDagRunOperator: Trigger another DAG from within a DAG
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# --- DAG 1: ShortCircuit + LatestOnly ---
@dag(
    dag_id="ch06_03a_conditional_patterns",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_06", "conditional"],
    doc_md=__doc__,
)
def conditional_patterns():
    # LatestOnlyOperator: Skips downstream tasks if this is a backfill/catchup run.
    # Only lets tasks through for the most recent scheduled interval.
    latest_only = LatestOnlyOperator(task_id="latest_only")

    # ShortCircuitOperator: Skips ALL downstream if callable returns False.
    gate = ShortCircuitOperator(
        task_id="check_data_exists",
        python_callable=lambda: True,  # Replace with real check in production
    )

    @task
    def process_latest():
        print("Processing latest data — skipped during catchup")

    @task
    def send_notification():
        print("Sending notification — only for latest run with data")

    latest_only >> gate >> process_latest() >> send_notification()


conditional_patterns()


# --- DAG 2: Cross-DAG trigger ---
@dag(
    dag_id="ch06_03b_trigger_downstream",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_06", "conditional", "cross_dag"],
    doc_md="Demonstrates TriggerDagRunOperator to trigger another DAG.",
)
def trigger_downstream():
    @task
    def prepare():
        print("Preparing data for downstream DAG")

    trigger = TriggerDagRunOperator(
        task_id="trigger_hello_airflow",
        trigger_dag_id="ch01_01_hello_airflow",  # Trigger the Ch01 DAG
        wait_for_completion=False,  # Don't block — fire and forget
        reset_dag_run=True,  # Allow re-triggering even if already ran
    )

    done = EmptyOperator(task_id="done")

    prepare() >> trigger >> done


trigger_downstream()

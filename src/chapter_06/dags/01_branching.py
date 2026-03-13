"""
Branching — Conditional Task Execution.

Branching lets a DAG choose which path to follow at runtime:
- @task.branch: Return the task_id(s) to execute, others are skipped
- BranchPythonOperator: Same behavior, traditional style
- Join pattern: Use trigger_rule='none_failed_min_one_success' on downstream tasks

Skipped tasks show as pink/light purple in the Airflow UI.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="ch06_01_branching",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_06", "branching"],
    doc_md=__doc__,
)
def branching():

    @task.branch
    def choose_branch(**context) -> str:
        """
        Return the task_id to execute. All other branches are skipped.
        In production, this could check a Variable, API, or data condition.
        """
        day = context["logical_date"].day_of_week  # 0=Monday, 6=Sunday
        if day < 5:  # Monday-Friday
            return "weekday_processing"
        return "weekend_processing"

    @task
    def weekday_processing():
        print("Running weekday ETL pipeline — full processing")

    @task
    def weekend_processing():
        print("Running weekend pipeline — light maintenance only")

    # Join point: runs regardless of which branch was taken.
    # trigger_rule='none_failed_min_one_success' means:
    # "Run if at least one upstream succeeded and none failed"
    # (skipped tasks don't count as failed)
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    @task(trigger_rule="none_failed_min_one_success")
    def send_report():
        print("Sending completion report — runs after either branch")

    # Wire it up
    branch = choose_branch()
    weekday = weekday_processing()
    weekend = weekend_processing()

    branch >> [weekday, weekend] >> join >> send_report()


branching()

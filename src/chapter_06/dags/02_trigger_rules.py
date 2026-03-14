"""
Trigger Rules — Controlling When Tasks Execute.

By default, a task runs only when ALL upstream tasks succeed (all_success).
Trigger rules let you change this behavior:

| Rule                          | Runs when...                                    |
|-------------------------------|------------------------------------------------|
| all_success (default)         | All upstreams succeeded                         |
| all_failed                    | All upstreams failed                            |
| all_done                      | All upstreams completed (any state)             |
| one_success                   | At least one upstream succeeded                 |
| one_failed                    | At least one upstream failed                    |
| none_failed                   | No upstream failed (success or skipped)         |
| none_failed_min_one_success   | No failure + at least one success               |
| none_skipped                  | No upstream was skipped                         |
| always                        | Runs regardless of upstream state               |

This DAG demonstrates the most commonly used trigger rules.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="ch06_02_trigger_rules",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},  # No retries to see failures clearly
    tags=["chapter_06", "trigger_rules"],
    doc_md=__doc__,
)
def trigger_rules():
    @task
    def task_succeeds():
        print("This task succeeds")

    @task
    def task_fails():
        raise ValueError("This task intentionally fails")

    # all_done: Runs after all upstreams finish, regardless of success/failure
    cleanup = EmptyOperator(
        task_id="cleanup_all_done",
        trigger_rule="all_done",
    )

    # one_success: Runs if at least one upstream succeeded
    report = EmptyOperator(
        task_id="report_one_success",
        trigger_rule="one_success",
    )

    # always: Runs no matter what
    @task(trigger_rule="always")
    def always_notify():
        print("This notification ALWAYS fires — even if everything upstream failed")

    success = task_succeeds()
    failure = task_fails()

    [success, failure] >> cleanup
    [success, failure] >> report
    [success, failure] >> always_notify()


trigger_rules()

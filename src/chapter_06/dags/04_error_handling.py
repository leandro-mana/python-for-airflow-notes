"""
Error Handling — Callbacks, Retries, and SLAs.

Production DAGs need robust error handling:
- Retries: Automatically retry failed tasks with configurable delay
- Callbacks: Run Python functions on success, failure, or retry events
- SLA misses: Alert when tasks take longer than expected

Callbacks receive a `context` dict with the full Airflow execution context.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

LOG = logging.getLogger(__name__)


def _on_failure(context: dict) -> None:
    """Called when a task fails (after all retries exhausted)."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"]
    exception = context.get("exception", "Unknown")

    LOG.error(
        "FAILURE CALLBACK: task=%s, dag=%s, date=%s, error=%s",
        task_id,
        dag_id,
        execution_date,
        exception,
    )
    # In production: send Slack/PagerDuty/email alert here


def _on_success(context: dict) -> None:
    """Called when a task succeeds."""
    task_id = context["task_instance"].task_id
    LOG.info("SUCCESS CALLBACK: task=%s completed successfully", task_id)


def _on_retry(context: dict) -> None:
    """Called when a task is about to be retried."""
    task_id = context["task_instance"].task_id
    try_number = context["task_instance"].try_number
    LOG.warning("RETRY CALLBACK: task=%s, attempt=%d", task_id, try_number)


@dag(
    dag_id="ch06_04_error_handling",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,  # 10s, 20s, 40s...
        "max_retry_delay": timedelta(minutes=5),
        "on_failure_callback": _on_failure,
        "on_success_callback": _on_success,
        "on_retry_callback": _on_retry,
    },
    tags=["chapter_06", "error_handling"],
    doc_md=__doc__,
)
def error_handling():
    @task
    def reliable_task():
        """This task always succeeds — success callback fires."""
        print("Doing reliable work...")

    @task
    def flaky_task():
        """Simulates a task that fails intermittently."""
        import random

        if random.random() < 0.5:
            raise ConnectionError("Simulated transient failure — will retry")
        print("Flaky task succeeded this time!")

    @task(
        retries=0,  # Override default — no retries for this task
        on_failure_callback=_on_failure,
    )
    def critical_task():
        """A task with custom retry config (no retries)."""
        print("Critical task — fails immediately if there's an error, no retries")

    reliable_task() >> flaky_task() >> critical_task()


error_handling()

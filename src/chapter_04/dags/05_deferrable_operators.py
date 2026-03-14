"""
Deferrable Operators — Async Waiting Without Blocking Workers.

Traditional sensors in poke mode occupy a worker slot the entire time they wait.
Deferrable operators solve this by:

1. Starting execution on a worker
2. Deferring to the Triggerer (async event loop) when they need to wait
3. Freeing the worker slot entirely
4. Resuming on a worker when the trigger fires

Benefits:
- One triggerer can handle thousands of deferred tasks
- Worker slots are free for actual compute work
- Much more efficient than sensors for long waits

Requires: The Triggerer service must be running (it is in our Docker Compose setup).

This DAG demonstrates the concept using TimeDeltaSensorAsync.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensorAsync


@dag(
    dag_id="ch04_05_deferrable_operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_04", "operators", "deferrable"],
    doc_md=__doc__,
)
def deferrable_example():
    # TimeDeltaSensorAsync: Defers to the triggerer instead of occupying a worker.
    # Compare this with TimeDeltaSensor (mode='poke') from DAG 03 — same behavior,
    # but this version doesn't hold a worker slot while waiting.
    wait = TimeDeltaSensorAsync(
        task_id="wait_async",
        delta=timedelta(seconds=15),
    )

    @task
    def after_wait():
        print("Deferrable wait completed!")
        print("The worker slot was FREE the entire time we were waiting.")
        print("The triggerer handled the async wait with minimal resources.")

    wait >> after_wait()


deferrable_example()

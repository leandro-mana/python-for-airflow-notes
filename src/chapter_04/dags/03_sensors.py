"""
Sensors — Waiting for Conditions.

Sensors are special operators that wait until a condition is met:
- FileSensor: Wait for a file to appear
- HttpSensor: Wait for an HTTP endpoint to return success
- ExternalTaskSensor: Wait for another DAG's task to complete

Two execution modes:
- mode='poke' (default): Occupies a worker slot while waiting (checks every poke_interval)
- mode='reschedule': Frees the worker slot between checks (better for long waits)

For very long waits, consider deferrable operators instead (Chapter 04, DAG 05).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_delta import TimeDeltaSensor


@dag(
    dag_id="ch04_03_sensors",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_04", "operators", "sensors"],
    doc_md=__doc__,
)
def sensors_example():

    # TimeDeltaSensor: Wait for a duration after the data interval start.
    # Useful for waiting for upstream data to land before processing.
    wait_for_data = TimeDeltaSensor(
        task_id="wait_30_seconds",
        delta=timedelta(seconds=30),
        mode="reschedule",  # Free the worker slot while waiting
        poke_interval=10,   # Check every 10 seconds
        timeout=120,        # Give up after 2 minutes
    )

    # FileSensor: Wait for a file to appear on the filesystem.
    # In Docker, this checks inside the container's filesystem.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/data/.gitkeep",  # Use a file we know exists for demo
        mode="poke",         # Keep the worker slot (fine for short waits)
        poke_interval=5,     # Check every 5 seconds
        timeout=60,          # Give up after 1 minute
    )

    @task
    def process_file():
        print("File detected! Processing...")

    # Wait for either condition, then process
    [wait_for_data, wait_for_file] >> process_file()


sensors_example()

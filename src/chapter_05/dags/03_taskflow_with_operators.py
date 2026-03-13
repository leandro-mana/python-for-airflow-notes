"""
Mixing TaskFlow with Traditional Operators.

In real DAGs you often combine @task functions with traditional operators:
- @task for Python logic
- BashOperator for shell commands
- Sensors for waiting conditions
- @task.bash for inline bash (Airflow 2.6+)
- @task.virtualenv for isolated Python environments

This DAG shows how to mix paradigms seamlessly.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="ch05_03_taskflow_with_operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_05", "taskflow", "mixed"],
    doc_md=__doc__,
)
def taskflow_with_operators():

    # Traditional BashOperator
    setup = BashOperator(
        task_id="setup_environment",
        bash_command="echo 'Environment ready' && date",
    )

    # @task for Python logic
    @task
    def extract_data() -> list[dict]:
        """Extract data using Python."""
        return [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]

    @task
    def transform_data(records: list[dict]) -> dict:
        """Transform using Python."""
        total = sum(r["value"] for r in records)
        return {"total": total, "count": len(records)}

    # @task.bash: Inline bash command as a decorated function (Airflow 2.6+)
    @task.bash
    def generate_report(summary: dict) -> str:
        """Returns a bash command string to execute."""
        return f"echo 'Report: {summary[\"count\"]} records, total={summary[\"total\"]}'"

    # @task.virtualenv: Run in an isolated virtual environment
    @task.virtualenv(
        task_id="isolated_task",
        requirements=["requests"],
        system_site_packages=False,
    )
    def isolated_task() -> str:
        """This runs in its own virtualenv with only 'requests' installed."""
        import requests

        resp = requests.get("https://httpbin.org/get", timeout=10)
        return f"Status: {resp.status_code}"

    # Mix and match: BashOperator >> @task >> @task >> @task.bash
    data = extract_data()
    summary = transform_data(data)
    report = generate_report(summary)
    isolated = isolated_task()

    setup >> data
    report >> isolated


taskflow_with_operators()

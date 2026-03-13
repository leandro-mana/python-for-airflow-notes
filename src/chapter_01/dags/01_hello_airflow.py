"""
Hello Airflow — Your First DAG.

This DAG demonstrates the absolute basics of Apache Airflow:
- Creating a DAG with the context manager pattern
- Setting default_args for all tasks
- Using BashOperator for simple shell commands
- Defining task dependencies with the >> operator
- Key parameters: dag_id, schedule, start_date, catchup, tags

Run this DAG and inspect it in the Airflow UI to see:
- The Graph view (task dependencies)
- The Grid view (run history)
- Task logs (stdout from each operator)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Default args apply to every task in this DAG unless overridden.
# This avoids repeating the same configuration on every task.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _greet():
    """A simple Python callable used by PythonOperator."""
    print("Hello from Airflow! Your first DAG is running.")


# DAG definition using the context manager pattern.
# Everything indented under `with DAG(...) as dag:` belongs to this DAG.
with DAG(
    dag_id="ch01_01_hello_airflow",
    default_args=default_args,
    description="Your very first Airflow DAG — the Hello World of workflows",
    doc_md=__doc__,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chapter_01", "basics"],
) as dag:

    # Task 1: Print the current date using BashOperator
    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date:' && date",
    )

    # Task 2: Greet the user using PythonOperator
    greet = PythonOperator(
        task_id="greet",
        python_callable=_greet,
    )

    # Task 3: Print a summary
    summary = BashOperator(
        task_id="summary",
        bash_command="echo 'DAG completed successfully!'",
    )

    # Dependencies: print_date runs first, then greet, then summary.
    # The >> operator means "runs before" (bitshift = set_downstream).
    print_date >> greet >> summary

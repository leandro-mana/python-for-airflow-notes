"""
PythonOperator — Executing Python Callables.

PythonOperator runs any Python function as a task. Key features:
- op_args / op_kwargs: Pass arguments to the callable
- Return value: Automatically pushed to XCom
- Context access: **kwargs gives you the full Airflow context (ti, ds, params, etc.)

For new DAGs, prefer the @task decorator (TaskFlow API) over PythonOperator —
it's cleaner and handles XCom automatically. This DAG shows both for comparison.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def greet(name: str, greeting: str = "Hello") -> str:
    """A simple callable — return value is auto-pushed to XCom."""
    message = f"{greeting}, {name}! Welcome to Airflow."
    print(message)
    return message


def process_with_context(**kwargs) -> None:
    """Access the full Airflow context via **kwargs."""
    ti = kwargs["ti"]
    ds = kwargs["ds"]
    run_id = kwargs["run_id"]

    # Pull XCom from a previous task
    greeting = ti.xcom_pull(task_ids="greet_user")

    print(f"Run ID: {run_id}")
    print(f"Logical date: {ds}")
    print(f"Previous task said: {greeting}")


def compute(x: int, y: int) -> dict:
    """Return a dict — each key becomes a separate XCom entry if multiple_outputs=True."""
    result = {"sum": x + y, "product": x * y}
    print(f"Computed: {result}")
    return result


with DAG(
    dag_id="ch04_02_python_operator",
    default_args=default_args,
    description="PythonOperator — callables, args, kwargs, context, XCom",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chapter_04", "operators", "python"],
) as dag:
    # Basic: pass positional args
    greet_task = PythonOperator(
        task_id="greet_user",
        python_callable=greet,
        op_args=["Leandro"],
        op_kwargs={"greeting": "G'day"},
    )

    # Context access: use **kwargs to get ti, ds, run_id, etc.
    context_task = PythonOperator(
        task_id="process_with_context",
        python_callable=process_with_context,
    )

    # Return dict
    compute_task = PythonOperator(
        task_id="compute",
        python_callable=compute,
        op_args=[10, 5],
    )

    greet_task >> context_task >> compute_task

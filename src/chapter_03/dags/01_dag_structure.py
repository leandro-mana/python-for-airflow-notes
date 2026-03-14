"""
DAG Structure — Context Manager vs Decorator.

Two ways to define a DAG in Airflow:
1. Context manager: `with DAG(...) as dag:`
2. Decorator: `@dag` (modern, recommended for TaskFlow)

This file shows both patterns side by side. In practice, choose one style
per project for consistency.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# ============================================================================
# Pattern 1: Context Manager (traditional)
# ============================================================================
# Good for: mixing operator types, explicit DAG object access, classic style.

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ch03_01a_dag_context_manager",
    default_args=default_args,
    description="DAG defined with the context manager pattern",
    doc_md="Traditional DAG definition using `with DAG(...) as dag:`",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chapter_03", "dag_structure"],
) as dag_cm:
    start = BashOperator(task_id="start", bash_command="echo 'Context manager DAG started'")
    end = BashOperator(task_id="end", bash_command="echo 'Context manager DAG finished'")

    start >> end


# ============================================================================
# Pattern 2: Decorator (modern TaskFlow)
# ============================================================================
# Good for: pure Python tasks, automatic XCom, cleaner syntax.


@dag(
    dag_id="ch03_01b_dag_decorator",
    default_args=default_args,
    description="DAG defined with the @dag decorator",
    doc_md="Modern DAG definition using `@dag` decorator and `@task` functions",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chapter_03", "dag_structure"],
)
def dag_decorator():
    @task
    def start():
        print("Decorator DAG started")

    @task
    def end():
        print("Decorator DAG finished")

    start() >> end()


dag_decorator()

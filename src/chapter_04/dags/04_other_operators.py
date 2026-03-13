"""
Other Useful Operators — Empty, Short Circuit, and More.

Airflow ships with many operators beyond Bash and Python:
- EmptyOperator: No-op placeholder (useful for DAG structure, join points)
- ShortCircuitOperator: Skips all downstream tasks if callable returns False
- BranchPythonOperator: Chooses which downstream branch to execute (see Ch06)

These operators help you build expressive DAG control flow.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator


def _check_condition() -> bool:
    """Return True to continue, False to skip all downstream tasks."""
    import random

    should_continue = random.choice([True, False])
    print(f"Condition check: {'CONTINUE' if should_continue else 'SKIP'}")
    return should_continue


@dag(
    dag_id="ch04_04_other_operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_04", "operators"],
    doc_md=__doc__,
)
def other_operators():

    # EmptyOperator: A no-op task used for DAG structure.
    # Great for start/end markers and join points.
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ShortCircuitOperator: If callable returns False, ALL downstream tasks are skipped.
    gate = ShortCircuitOperator(
        task_id="check_condition",
        python_callable=_check_condition,
    )

    @task
    def process():
        print("This only runs if check_condition returned True")

    @task
    def report():
        print("Final report — only reached if gate was open")

    start >> gate >> process() >> report() >> end


other_operators()

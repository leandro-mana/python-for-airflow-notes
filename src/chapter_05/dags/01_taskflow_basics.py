"""
TaskFlow Basics — The @task Decorator.

The TaskFlow API (Airflow 2.0+) lets you write DAGs as plain Python functions:
- @task decorator turns a function into an Airflow task
- Return values are automatically passed via XCom
- Type hints serve as documentation
- No more PythonOperator boilerplate

This is the recommended way to write DAGs for Python-heavy workflows.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch05_01_taskflow_basics",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_05", "taskflow"],
    doc_md=__doc__,
)
def taskflow_basics():
    @task
    def extract() -> dict:
        """Simulate extracting data from a source system."""
        orders = {
            "1001": {"product": "Widget A", "amount": 301.27},
            "1002": {"product": "Widget B", "amount": 433.21},
            "1003": {"product": "Widget C", "amount": 502.22},
        }
        print(f"Extracted {len(orders)} orders")
        return orders

    @task
    def transform(orders: dict) -> dict:
        """Compute total revenue from extracted orders."""
        total = sum(order["amount"] for order in orders.values())
        result = {"total_revenue": total, "order_count": len(orders)}
        print(f"Transformed: {result}")
        return result

    @task
    def load(summary: dict) -> None:
        """Load the summary (in production, write to DB/warehouse)."""
        print(f"Loading summary: {summary}")
        print(f"Total revenue: ${summary['total_revenue']:.2f}")
        print(f"Order count: {summary['order_count']}")

    # TaskFlow magic: return values flow automatically between tasks.
    # No xcom_push / xcom_pull needed!
    raw_data = extract()
    summary = transform(raw_data)
    load(summary)


taskflow_basics()

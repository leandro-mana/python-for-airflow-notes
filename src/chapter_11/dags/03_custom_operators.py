"""
Custom Operators — Extending Airflow.

When built-in operators don't fit your use case, create custom ones:
1. Subclass BaseOperator
2. Implement execute(self, context)
3. Define template_fields for Jinja-rendered parameters

Custom operators promote reuse across DAGs and can be packaged as providers.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.models.baseoperator import BaseOperator


class GreetOperator(BaseOperator):
    """
    A custom operator that greets someone.

    Demonstrates:
    - Subclassing BaseOperator
    - template_fields for Jinja rendering
    - execute() method with context
    - XCom push via return value
    """

    # Fields listed here support Jinja templates (e.g., {{ ds }})
    template_fields = ("name", "greeting")

    def __init__(self, name: str, greeting: str = "Hello", **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.greeting = greeting

    def execute(self, context):
        """Called when the task runs."""
        message = f"{self.greeting}, {self.name}!"
        self.log.info(message)

        # Return value is automatically pushed to XCom
        return message


class DataQualityOperator(BaseOperator):
    """
    A custom operator that runs data quality checks.

    In production, this would connect to a database and run SQL checks.
    """

    template_fields = ("table_name", "check_date")

    def __init__(
        self,
        table_name: str,
        check_date: str = "{{ ds }}",
        min_rows: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.check_date = check_date
        self.min_rows = min_rows

    def execute(self, context):
        self.log.info(
            "Running quality check on %s for date %s",
            self.table_name, self.check_date,
        )
        # Simulated check
        row_count = 1500
        self.log.info("Found %d rows (minimum: %d)", row_count, self.min_rows)

        if row_count < self.min_rows:
            raise ValueError(
                f"Quality check failed: {self.table_name} has {row_count} rows "
                f"(minimum: {self.min_rows})"
            )

        return {"table": self.table_name, "rows": row_count, "status": "passed"}


@dag(
    dag_id="ch11_03_custom_operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_11", "custom_operators"],
    doc_md=__doc__,
)
def custom_operators():

    # Use the custom GreetOperator — note template rendering in name
    greet = GreetOperator(
        task_id="greet",
        name="Airflow user on {{ ds }}",
        greeting="Welcome",
    )

    # Use the custom DataQualityOperator
    check_orders = DataQualityOperator(
        task_id="check_orders",
        table_name="orders",
        min_rows=100,
    )

    check_users = DataQualityOperator(
        task_id="check_users",
        table_name="users",
        min_rows=10,
    )

    greet >> [check_orders, check_users]


custom_operators()

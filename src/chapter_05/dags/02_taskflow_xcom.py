"""
TaskFlow XCom — How Data Passes Between Tasks.

Under the hood, TaskFlow uses XCom (Cross-Communication) to pass data:
- Return values are serialized to JSON and stored in the metadata DB
- Downstream tasks deserialize the data automatically
- multiple_outputs=True: Returns a dict where each key is a separate XCom

This DAG shows both modern (TaskFlow) and traditional (xcom_push/pull) patterns,
and demonstrates multiple_outputs for structured data passing.

Important XCom limitations:
- Default XCom backend stores data in the metadata DB (not for large datasets!)
- Max size depends on your DB (PostgreSQL: ~1GB, but keep it small)
- For large data: use object storage (S3/GCS) and pass the path via XCom
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import dag, task

LOG = logging.getLogger(__name__)


@dag(
    dag_id="ch05_02_taskflow_xcom",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_05", "taskflow", "xcom"],
    doc_md=__doc__,
)
def taskflow_xcom():

    # --- Pattern 1: Traditional XCom (pre-TaskFlow) ---
    @task
    def extract_traditional(**kwargs) -> None:
        """Push data to XCom manually using task instance."""
        ti = kwargs["ti"]
        data = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push(key="order_data", value=data)
        LOG.info("Pushed order_data to XCom (traditional)")

    @task
    def transform_traditional(**kwargs) -> None:
        """Pull data from XCom manually."""
        ti = kwargs["ti"]
        raw = ti.xcom_pull(task_ids="extract_traditional", key="order_data")
        orders = json.loads(raw)
        total = sum(orders.values())
        ti.xcom_push(key="total", value=total)
        LOG.info("Total (traditional): %.2f", total)

    # --- Pattern 2: TaskFlow with multiple_outputs ---
    @task(multiple_outputs=True)
    def extract_modern() -> dict:
        """Each key in the returned dict becomes a separate XCom entry."""
        return {
            "revenue": 1236.70,
            "order_count": 3,
            "currency": "USD",
        }

    @task
    def report(revenue: float, order_count: int, currency: str) -> None:
        """Each XCom key is passed as a separate argument."""
        print(f"Revenue: {revenue} {currency}")
        print(f"Orders: {order_count}")
        print(f"Average: {revenue / order_count:.2f} {currency}")

    # Traditional flow
    ext_t = extract_traditional()
    trans_t = transform_traditional()
    ext_t >> trans_t

    # Modern flow with multiple_outputs
    data = extract_modern()
    report(
        revenue=data["revenue"],
        order_count=data["order_count"],
        currency=data["currency"],
    )


taskflow_xcom()

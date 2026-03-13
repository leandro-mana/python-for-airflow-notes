"""
Datasets — Data-Aware Scheduling.

Datasets (Airflow 2.4+) enable event-driven DAG scheduling:
- A producer DAG declares that it updates a Dataset
- A consumer DAG is scheduled to run when that Dataset is updated
- No cron needed — the consumer runs automatically after the producer

This replaces patterns like ExternalTaskSensor for cross-DAG dependencies.
"""
from __future__ import annotations

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Define a dataset — the URI is a logical identifier, not a real path.
# Convention: use a meaningful URI that describes the data.
ORDERS_DATASET = Dataset("s3://warehouse/orders/daily")


# --- Producer DAG: Updates the dataset ---
@dag(
    dag_id="ch11_01a_dataset_producer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_11", "datasets", "producer"],
    doc_md="Produces data and declares it updated a Dataset.",
)
def dataset_producer():

    @task(outlets=[ORDERS_DATASET])
    def produce_orders():
        """
        outlets=[ORDERS_DATASET] tells Airflow this task updates the dataset.
        When this task succeeds, any DAG scheduled on this dataset is triggered.
        """
        print("Processing daily orders...")
        print("Writing to s3://warehouse/orders/daily")
        print("Dataset updated — consumers will be triggered")

    produce_orders()


dataset_producer()


# --- Consumer DAG: Triggered by the dataset ---
@dag(
    dag_id="ch11_01b_dataset_consumer",
    schedule=[ORDERS_DATASET],  # Triggered when ORDERS_DATASET is updated
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_11", "datasets", "consumer"],
    doc_md="Automatically triggered when the orders dataset is updated.",
)
def dataset_consumer():

    @task
    def consume_orders():
        """This runs automatically when the producer updates the dataset."""
        print("New orders data available!")
        print("Building aggregations from s3://warehouse/orders/daily")

    @task
    def build_dashboard():
        print("Refreshing dashboard with latest order data")

    consume_orders() >> build_dashboard()


dataset_consumer()

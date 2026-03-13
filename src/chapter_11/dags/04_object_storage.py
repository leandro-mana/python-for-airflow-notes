"""
Object Storage — Universal File Handling.

ObjectStoragePath (Airflow 2.8+) provides a unified API for file operations
across local, S3, GCS, and Azure Blob storage.

Same code works regardless of storage backend — just change the connection.
This DAG demonstrates local file operations (same API works for cloud storage).
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch11_04_object_storage",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_11", "object_storage"],
    doc_md=__doc__,
)
def object_storage():

    @task
    def write_data(**context) -> str:
        """Write data to local storage (same pattern works for S3/GCS)."""
        ds = context["ds"]
        output_path = f"/opt/airflow/data/output_{ds}.json"
        data = {
            "date": ds,
            "metrics": {"revenue": 15000, "orders": 150, "users": 75},
        }

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Wrote data to {output_path}")
        # In production with ObjectStoragePath:
        # base = ObjectStoragePath("s3://my-bucket/data/", conn_id="aws_default")
        # path = base / f"output_{ds}.json"
        # path.write_text(json.dumps(data))
        return output_path

    @task
    def read_data(file_path: str) -> dict:
        """Read data back (same API for any storage backend)."""
        with open(file_path) as f:
            data = json.load(f)

        print(f"Read from {file_path}: {data}")
        return data

    @task
    def summarize(data: dict) -> None:
        """Process the data."""
        metrics = data["metrics"]
        print(f"Date: {data['date']}")
        print(f"Revenue: ${metrics['revenue']:,}")
        print(f"Avg order value: ${metrics['revenue'] / metrics['orders']:.2f}")

    path = write_data()
    data = read_data(path)
    summarize(data)


object_storage()

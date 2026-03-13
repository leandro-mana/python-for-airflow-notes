"""
Dynamic Task Mapping — Runtime-Determined Parallelism.

Dynamic task mapping (Airflow 2.3+) creates task instances at runtime:
- .expand(): Map a task across a list of inputs
- .partial(): Set fixed arguments for mapped tasks
- The number of instances isn't known until the upstream task completes

Use cases: processing files, API pagination, per-customer pipelines.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch10_01_dynamic_task_mapping",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_10", "dynamic", "mapping"],
    doc_md=__doc__,
)
def dynamic_task_mapping():

    @task
    def discover_files() -> list[str]:
        """Discover files to process (number unknown at DAG parse time)."""
        # In production: list S3 bucket, query API, scan directory
        return ["users.csv", "orders.csv", "products.csv", "transactions.csv"]

    @task
    def process_file(filename: str, output_dir: str) -> dict:
        """
        Process a single file.

        This task runs ONCE PER FILE — Airflow creates N parallel instances
        at runtime based on the output of discover_files().
        """
        row_count = hash(filename) % 10000
        print(f"Processing {filename} → {output_dir}/{filename}")
        print(f"  Rows processed: {row_count}")
        return {"file": filename, "rows": row_count}

    @task
    def aggregate(results: list[dict]) -> None:
        """Collect all mapped results into a single summary."""
        total = sum(r["rows"] for r in results)
        print(f"Aggregated {len(results)} files, {total} total rows")
        for r in results:
            print(f"  {r['file']}: {r['rows']} rows")

    files = discover_files()

    # .expand() creates one task per file
    # .partial() sets fixed args shared across all mapped instances
    processed = process_file.partial(output_dir="/opt/airflow/data").expand(
        filename=files
    )

    aggregate(processed)


dynamic_task_mapping()

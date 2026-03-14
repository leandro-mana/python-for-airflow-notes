"""
Incremental Loading — Avoiding Full Reloads.

Instead of reloading all data every run, incremental loading processes only
new or changed records since the last run.

Patterns:
1. **Watermark**: Track max(updated_at) from the last run
2. **Partition-based**: Process data for a specific date partition
3. **CDC (Change Data Capture)**: Stream changes from a source log

This DAG demonstrates the partition-based pattern using Airflow's data intervals.
The logical_date naturally defines which partition to process.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch08_04_incremental_loading",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_08", "incremental", "pipeline"],
    doc_md=__doc__,
)
def incremental_loading():
    @task
    def extract_partition(**context) -> list[dict]:
        """
        Extract data for a specific date partition.

        Uses logical_date (data_interval_start) to determine which partition.
        This makes the task naturally idempotent — re-running for the same
        date always extracts the same data.
        """
        ds = context["ds"]
        print(f"Extracting data for partition: date={ds}")

        # Simulated: In production, this would be:
        # SELECT * FROM source_table WHERE date = '{ds}'
        simulated_data = [
            {"id": 1, "value": 100, "date": ds},
            {"id": 2, "value": 200, "date": ds},
            {"id": 3, "value": 300, "date": ds},
        ]
        print(f"Extracted {len(simulated_data)} records for {ds}")
        return simulated_data

    @task
    def load_partition(records: list[dict], **context) -> None:
        """
        Load data into the target using partition replacement.

        Pattern: DELETE + INSERT (or REPLACE) for the partition.
        This is idempotent — re-running replaces the same partition.

        SQL equivalent:
            DELETE FROM target_table WHERE date = '{ds}';
            INSERT INTO target_table SELECT * FROM staging WHERE date = '{ds}';
        """
        ds = context["ds"]
        print(f"Loading {len(records)} records into partition date={ds}")
        print(f"Step 1: DELETE FROM target WHERE date = '{ds}'")
        print(f"Step 2: INSERT {len(records)} records")
        print("Load complete — partition replaced idempotently")

    @task
    def update_watermark(**context) -> None:
        """
        Update the high watermark for the next run.

        In production, store this as an Airflow Variable or in a control table.
        With partition-based loading, the watermark is implicit (logical_date),
        but explicit watermarks are useful for non-partitioned sources.
        """
        ds = context["ds"]
        print(f"Watermark updated to: {ds}")
        # Variable.set("pipeline_watermark", ds)

    data = extract_partition()
    load_partition(data) >> update_watermark()


incremental_loading()

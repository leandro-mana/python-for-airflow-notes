"""
Dynamic Task Groups — Organized Parallel Processing.

Task groups + dynamic mapping = scalable, organized DAGs:
- @task_group creates collapsible groups in the UI
- Combine with .expand() for per-item grouped processing
- DAG params allow runtime configuration via the trigger UI

This DAG processes multiple data sources, each with its own extract → transform
→ validate group.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="ch10_03_dynamic_task_groups",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    params={"sources": ["users", "orders", "products"]},
    tags=["chapter_10", "dynamic", "task_groups"],
    doc_md=__doc__,
)
def dynamic_task_groups():

    @task
    def get_sources(**context) -> list[str]:
        """Get data sources from DAG params (overridable at trigger time)."""
        sources = context["params"]["sources"]
        print(f"Processing {len(sources)} sources: {sources}")
        return sources

    @task_group(group_id="process")
    def process_source(source: str):
        """A task group that runs for each data source."""

        @task
        def extract(source_name: str) -> dict:
            print(f"Extracting from {source_name}")
            return {"source": source_name, "records": 500}

        @task
        def transform(data: dict) -> dict:
            print(f"Transforming {data['records']} records from {data['source']}")
            return {**data, "transformed": True}

        @task
        def validate(data: dict) -> bool:
            print(f"Validating {data['source']}: {data['records']} records")
            return True

        extracted = extract(source)
        transformed = transform(extracted)
        validate(transformed)

    @task
    def final_report():
        print("All sources processed and validated")

    sources = get_sources()
    # Create one task group per source using dynamic mapping
    process_source.expand(source=sources) >> final_report()


dynamic_task_groups()

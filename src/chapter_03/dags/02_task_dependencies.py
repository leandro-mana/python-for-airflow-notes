"""
Task Dependencies — Defining Execution Order.

Airflow provides several ways to define how tasks relate to each other:
- Bitshift operators: >> (downstream) and << (upstream)
- chain(): Linear or cross dependencies
- Task groups: Logical grouping of related tasks

This DAG demonstrates all dependency patterns.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


@dag(
    dag_id="ch03_02_task_dependencies",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_03", "dependencies"],
    doc_md=__doc__,
)
def task_dependencies():
    # --- Linear chain with >> ---
    @task
    def extract():
        print("Extracting data")
        return {"raw_data": [1, 2, 3, 4, 5]}

    @task
    def transform(data: dict):
        print(f"Transforming {data}")
        return {"transformed": [x * 2 for x in data["raw_data"]]}

    @task
    def load(data: dict):
        print(f"Loading {data}")

    # --- Fan-out: one task feeds multiple downstream tasks ---
    @task
    def validate_schema():
        print("Schema validation passed")

    @task
    def validate_quality():
        print("Quality checks passed")

    @task
    def validate_completeness():
        print("Completeness check passed")

    # --- Fan-in: multiple tasks feed one downstream task ---
    @task
    def report():
        print("All validations passed — generating report")

    # Wire it all up:
    # 1. Linear: extract >> transform >> load
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    # 2. Fan-out: load feeds three validation tasks
    schema = validate_schema()
    quality = validate_quality()
    completeness = validate_completeness()
    loaded >> [schema, quality, completeness]

    # 3. Fan-in: all validations must pass before report
    # chain() is equivalent to: schema >> report; quality >> report; completeness >> report
    chain([schema, quality, completeness], report())


task_dependencies()

"""
Data Quality Checks.

Data quality should be a first-class citizen in every pipeline:
- Schema validation: Are the expected columns present with correct types?
- Completeness: Are there unexpected nulls or missing records?
- Range checks: Are values within expected bounds?
- Freshness: Is the data recent enough?

This DAG demonstrates custom data quality checks as tasks.
In production, consider integrating Great Expectations or Soda.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="ch08_03_data_quality",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_08", "data_quality", "pipeline"],
    doc_md=__doc__,
)
def data_quality():

    @task
    def generate_sample_data() -> list[dict]:
        """Simulate data that needs quality checking."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
            {"id": 3, "name": None, "age": 35, "email": "charlie@example.com"},
            {"id": 4, "name": "Diana", "age": -5, "email": "diana@example.com"},
            {"id": 5, "name": "Eve", "age": 28, "email": None},
        ]

    @task
    def check_schema(data: list[dict]) -> bool:
        """Validate that expected columns are present."""
        required_columns = {"id", "name", "age", "email"}

        for i, record in enumerate(data):
            missing = required_columns - set(record.keys())
            if missing:
                raise AirflowFailException(
                    f"Record {i}: missing columns {missing}"
                )

        print(f"Schema check PASSED: all {len(data)} records have required columns")
        return True

    @task
    def check_completeness(data: list[dict]) -> dict:
        """Check for null values and report completeness."""
        null_report = {}
        for key in data[0].keys():
            nulls = sum(1 for r in data if r.get(key) is None)
            null_report[key] = {"nulls": nulls, "total": len(data)}

        for col, stats in null_report.items():
            pct = (1 - stats["nulls"] / stats["total"]) * 100
            status = "PASS" if pct >= 95 else "WARN" if pct >= 80 else "FAIL"
            print(f"  {col}: {pct:.0f}% complete [{status}]")

        print("Completeness check finished")
        return null_report

    @task
    def check_ranges(data: list[dict]) -> bool:
        """Validate that numeric values are within expected ranges."""
        violations = []
        for record in data:
            if record["age"] is not None and (record["age"] < 0 or record["age"] > 150):
                violations.append(f"id={record['id']}: age={record['age']} out of range")

        if violations:
            for v in violations:
                print(f"  VIOLATION: {v}")
            print(f"Range check: {len(violations)} violations found (warning, not failing)")
        else:
            print("Range check PASSED: all values within bounds")

        return len(violations) == 0

    @task
    def quality_report(
        schema_ok: bool, completeness: dict, ranges_ok: bool
    ) -> None:
        """Generate a summary quality report."""
        print("=" * 50)
        print("DATA QUALITY REPORT")
        print("=" * 50)
        print(f"  Schema:       {'PASS' if schema_ok else 'FAIL'}")
        print(f"  Ranges:       {'PASS' if ranges_ok else 'WARN'}")
        print(f"  Completeness: {len(completeness)} columns checked")
        print("=" * 50)

    data = generate_sample_data()
    schema = check_schema(data)
    completeness = check_completeness(data)
    ranges = check_ranges(data)
    quality_report(schema, completeness, ranges)


data_quality()

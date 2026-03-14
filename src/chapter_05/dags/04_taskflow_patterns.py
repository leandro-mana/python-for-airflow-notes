"""
TaskFlow Patterns — Dynamic Mapping and Task Groups.

Advanced TaskFlow patterns:
- Dynamic task mapping: .expand() creates tasks at runtime based on data
- @task_group: Logically group related tasks in the UI
- Conditional task execution with return values

These patterns enable scalable, data-driven DAGs.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="ch05_04_taskflow_patterns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_05", "taskflow", "patterns"],
    doc_md=__doc__,
)
def taskflow_patterns():
    # --- Dynamic Task Mapping ---
    # .expand() creates one task instance per item at runtime.
    # The number of tasks isn't known until the upstream task completes.

    @task
    def get_files() -> list[str]:
        """Simulate discovering files to process."""
        return ["file_a.csv", "file_b.csv", "file_c.csv", "file_d.csv"]

    @task
    def process_file(filename: str) -> dict:
        """This task runs ONCE PER FILE — Airflow creates 4 parallel instances."""
        print(f"Processing: {filename}")
        row_count = len(filename) * 100  # Simulated
        return {"file": filename, "rows": row_count}

    @task
    def summarize(results: list[dict]) -> None:
        """Collect all mapped results."""
        total_rows = sum(r["rows"] for r in results)
        print(f"Processed {len(results)} files, {total_rows} total rows")

    # --- Task Groups ---
    # @task_group creates a collapsible group in the Airflow UI.
    # Great for organizing related tasks visually.

    @task_group(group_id="validation")
    def validate():
        @task
        def check_schema() -> bool:
            print("Schema validation passed")
            return True

        @task
        def check_freshness() -> bool:
            print("Data freshness check passed")
            return True

        @task
        def validation_report(schema_ok: bool, fresh_ok: bool) -> None:
            print(f"Validation: schema={schema_ok}, freshness={fresh_ok}")

        s = check_schema()
        f = check_freshness()
        validation_report(s, f)

    # Wire it up:
    files = get_files()

    # .expand() creates one process_file task per file
    processed = process_file.expand(filename=files)

    summarize(processed) >> validate()


taskflow_patterns()

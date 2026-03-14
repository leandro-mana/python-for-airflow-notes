"""
Debugging — Finding and Fixing DAG Issues.

Common debugging techniques:
1. `airflow tasks test` — run a task locally without recording
2. Task logs in the Airflow UI (Graph → click task → Log)
3. `self.log` in custom operators for structured logging
4. Python breakpoints (not in Docker — use print/log instead)
5. DAG parse errors: check scheduler logs or `airflow dags list`

Common mistakes and their symptoms:
| Symptom                    | Likely cause                          |
|----------------------------|---------------------------------------|
| DAG not visible in UI      | Import error, wrong dags_folder       |
| Task stuck in "scheduled"  | No available worker slots             |
| Task stuck in "queued"     | Celery/K8s executor issue             |
| Task immediately fails     | Python exception in execute()         |
| Task succeeds but wrong    | XCom serialization issue, wrong args  |
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="ch09_04_debugging",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_09", "testing", "debugging"],
    doc_md=__doc__,
)
def debugging():
    @task
    def good_logging():
        """Demonstrate effective logging in tasks."""
        logger.info("Starting data processing")
        logger.info("Configuration: batch_size=%d, timeout=%d", 1000, 300)

        records = [{"id": i, "value": i * 10} for i in range(5)]
        logger.info("Extracted %d records", len(records))

        for record in records:
            logger.debug("Processing record: %s", record)

        logger.info("Processing complete — %d records handled", len(records))
        return len(records)

    @task
    def common_errors():
        """Display common DAG errors and how to fix them."""
        errors = [
            (
                "DAG not appearing in UI",
                "1. Check for import errors: airflow dags list\n"
                "   2. Verify dags_folder path in airflow.cfg\n"
                "   3. Check file has 'dag' or 'DAG' in content\n"
                "   4. Wait for scheduler parse interval (default 30s)",
            ),
            (
                "Task fails with ModuleNotFoundError",
                "1. Install package in the Airflow image (Dockerfile)\n"
                "   2. Or add to _PIP_ADDITIONAL_REQUIREMENTS\n"
                "   3. Rebuild: docker compose build",
            ),
            (
                "XCom value is None unexpectedly",
                "1. Ensure upstream task returns a value\n"
                "   2. Check task_id matches in xcom_pull()\n"
                "   3. Verify serialization (large objects may fail)",
            ),
            (
                "Template not rendered (shows {{ ds }} literally)",
                "1. Field must be in operator's template_fields\n"
                "   2. Custom operators: add field to template_fields tuple\n"
                "   3. PythonOperator: use op_kwargs with templates_dict",
            ),
        ]

        print("COMMON AIRFLOW ERRORS AND FIXES")
        print("=" * 50)
        for error, fix in errors:
            print(f"\nProblem: {error}")
            print(f"  Fix: {fix}")

    @task
    def debugging_tools():
        """Display available debugging tools."""
        print(
            "DEBUGGING TOOLS\n"
            "===============\n"
            "\n"
            "1. CLI — Test a single task without recording:\n"
            "   airflow tasks test <dag_id> <task_id> 2024-01-01\n"
            "\n"
            "2. CLI — List DAGs and check for parse errors:\n"
            "   airflow dags list\n"
            "   airflow dags list-import-errors\n"
            "\n"
            "3. UI — Task Instance details:\n"
            "   Graph view -> click task -> Log / XCom / Details\n"
            "\n"
            "4. Scheduler logs:\n"
            "   docker compose logs -f airflow-scheduler\n"
            "\n"
            "5. Python logging (not print):\n"
            "   import logging\n"
            "   logger = logging.getLogger(__name__)\n"
            "   logger.info('Processing %d records', count)\n"
            "\n"
            "6. Increase log verbosity:\n"
            "   AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG"
        )

    good_logging() >> common_errors() >> debugging_tools()


debugging()

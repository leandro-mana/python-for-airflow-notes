"""
Integration Testing — End-to-End DAG Runs.

Integration tests verify DAGs work in a real Airflow environment:
- Test full DAG execution using `airflow dags test`
- Verify task outputs and XCom values
- Test with real (or containerized) external systems
- Use Docker Compose for reproducible test environments

CLI command:
  airflow dags test <dag_id> <execution_date>
  airflow tasks test <dag_id> <task_id> <execution_date>

`airflow tasks test` runs a single task WITHOUT recording to the DB,
perfect for development and debugging.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch09_03_integration_testing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_09", "testing", "integration"],
    doc_md=__doc__,
)
def integration_testing():
    @task
    def show_test_commands():
        """Display CLI commands for integration testing."""
        print(
            "AIRFLOW CLI TEST COMMANDS\n"
            "========================\n"
            "\n"
            "# Test an entire DAG (runs all tasks sequentially):\n"
            "airflow dags test ch09_03_integration_testing 2024-01-01\n"
            "\n"
            "# Test a single task (no DB recording):\n"
            "airflow tasks test ch09_03_integration_testing step_one 2024-01-01\n"
            "\n"
            "# List tasks in a DAG:\n"
            "airflow tasks list ch09_03_integration_testing --tree\n"
            "\n"
            "# From Docker Compose:\n"
            "docker compose exec airflow-worker airflow dags test ch09_03_integration_testing 2024-01-01"
        )

    @task
    def step_one() -> str:
        """First step — produces output for downstream tasks."""
        result = "data_from_step_one"
        print(f"Step 1 produced: {result}")
        return result

    @task
    def step_two(data: str) -> str:
        """Second step — transforms data from step one."""
        result = f"processed_{data}"
        print(f"Step 2 received: {data}")
        print(f"Step 2 produced: {result}")
        return result

    @task
    def verify(data: str):
        """
        Verification step — in a real integration test, assert expected values.

        In pytest:
            def test_pipeline_end_to_end():
                result = dag_bag.dags["my_dag"].test()
                # Check XCom values, DB state, output files, etc.
        """
        print(f"Verifying result: {data}")
        assert data == "processed_data_from_step_one", f"Unexpected result: {data}"
        print("Integration test PASSED")

    @task
    def show_integration_patterns():
        """Display integration testing strategies."""
        print(
            "INTEGRATION TESTING STRATEGIES\n"
            "==============================\n"
            "1. Use `airflow dags test` for quick end-to-end runs\n"
            "2. Use `airflow tasks test` to debug individual tasks\n"
            "3. Run tests against a Docker Compose Airflow instance\n"
            "4. Use test databases (not production!) for DB-backed tests\n"
            "5. Astronomer: `astro dev pytest` runs tests in the Airflow env\n"
            "6. CI/CD: spin up Airflow in Docker, run tests, tear down"
        )

    cmds = show_test_commands()
    s1 = step_one()
    s2 = step_two(s1)
    v = verify(s2)
    cmds >> s1
    v >> show_integration_patterns()


integration_testing()

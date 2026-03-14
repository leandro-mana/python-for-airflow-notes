"""
Unit Testing — Testing Task Logic in Isolation.

Unit tests verify your Python callables WITHOUT running Airflow:
- Test @task functions as regular Python functions
- Mock external dependencies (APIs, databases, Variables, Connections)
- Use pytest fixtures for test data
- Assert return values and side effects

Key principle: your business logic should be testable WITHOUT Airflow.
Extract logic into plain Python functions, then call them from tasks.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


def calculate_metrics(records: list[dict]) -> dict:
    """
    Pure business logic — easily testable without Airflow.

    In your test file:
        def test_calculate_metrics():
            records = [{"amount": 100}, {"amount": 200}]
            result = calculate_metrics(records)
            assert result["total"] == 300
            assert result["count"] == 2
            assert result["average"] == 150.0
    """
    if not records:
        return {"total": 0, "count": 0, "average": 0.0}

    total = sum(r["amount"] for r in records)
    count = len(records)
    return {
        "total": total,
        "count": count,
        "average": round(total / count, 2),
    }


def validate_record(record: dict) -> bool:
    """
    Validation logic — test edge cases without Airflow.

    In your test file:
        def test_validate_record_valid():
            assert validate_record({"id": 1, "amount": 100}) is True

        def test_validate_record_missing_id():
            assert validate_record({"amount": 100}) is False

        def test_validate_record_negative_amount():
            assert validate_record({"id": 1, "amount": -5}) is False
    """
    required_keys = {"id", "amount"}
    if not required_keys.issubset(record.keys()):
        return False
    if record["amount"] <= 0:
        return False
    return True


@dag(
    dag_id="ch09_02_unit_testing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_09", "testing", "unit_tests"],
    doc_md=__doc__,
)
def unit_testing():
    @task
    def extract() -> list[dict]:
        """Simulate data extraction."""
        return [
            {"id": 1, "amount": 100.50},
            {"id": 2, "amount": 200.75},
            {"id": 3, "amount": 50.00},
        ]

    @task
    def validate_and_transform(records: list[dict]) -> list[dict]:
        """Use the testable validation function."""
        valid = [r for r in records if validate_record(r)]
        invalid_count = len(records) - len(valid)
        print(f"Valid: {len(valid)}, Invalid: {invalid_count}")
        return valid

    @task
    def compute(records: list[dict]) -> dict:
        """Use the testable metrics function."""
        metrics = calculate_metrics(records)
        print(f"Metrics: {metrics}")
        return metrics

    @task
    def show_testing_patterns():
        """Display unit testing best practices."""
        print(
            "UNIT TESTING BEST PRACTICES\n"
            "===========================\n"
            "1. Extract business logic into plain Python functions\n"
            "2. Test those functions directly with pytest (no Airflow needed)\n"
            "3. Mock Variable.get() and BaseHook.get_connection() in tests\n"
            "4. Use pytest fixtures for reusable test data\n"
            "5. Test edge cases: empty input, invalid data, missing keys\n"
            "\n"
            "Example mock:\n"
            '  @mock.patch("airflow.models.Variable.get")\n'
            "  def test_my_task(mock_var):\n"
            '      mock_var.return_value = "test_value"\n'
            "      result = my_task_function()\n"
            '      assert result == "expected"'
        )

    raw = extract()
    clean = validate_and_transform(raw)
    compute(clean) >> show_testing_patterns()


unit_testing()

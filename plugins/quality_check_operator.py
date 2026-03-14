"""
Reusable Data Quality Check Operator.

A production-ready custom operator for running data quality checks.
Place in the plugins/ directory — Airflow auto-discovers it.

Usage in DAGs:
    from quality_check_operator import DataQualityCheckOperator

    check = DataQualityCheckOperator(
        task_id="check_orders",
        checks=[
            {"name": "row_count", "query": "SELECT COUNT(*) FROM orders", "min_value": 1},
            {"name": "null_check", "query": "SELECT COUNT(*) FROM orders WHERE name IS NULL", "max_value": 0},
        ],
    )

See chapter 11 (custom operators) for more details on building custom operators.
"""

from __future__ import annotations

from airflow.models.baseoperator import BaseOperator


class DataQualityCheckOperator(BaseOperator):
    """
    Run a list of data quality checks and fail if any check is violated.

    Each check is a dict with:
    - name: descriptive name for the check
    - query: SQL query that returns a single numeric value
    - min_value (optional): result must be >= this value
    - max_value (optional): result must be <= this value

    In production, this would use a Hook (e.g., PostgresHook) to execute queries.
    This example simulates the results for demonstration.
    """

    template_fields = ("checks",)

    def __init__(self, checks: list[dict], **kwargs):
        super().__init__(**kwargs)
        self.checks = checks

    def execute(self, context):
        results = []

        for check in self.checks:
            name = check["name"]
            query = check["query"]
            min_value = check.get("min_value")
            max_value = check.get("max_value")

            # In production: result = hook.get_first(query)[0]
            self.log.info("Running check '%s': %s", name, query)
            result = self._simulate_query(query)

            passed = True
            if min_value is not None and result < min_value:
                passed = False
                self.log.error("Check '%s' FAILED: %s < min_value %s", name, result, min_value)
            if max_value is not None and result > max_value:
                passed = False
                self.log.error("Check '%s' FAILED: %s > max_value %s", name, result, max_value)

            if passed:
                self.log.info("Check '%s' PASSED: value=%s", name, result)

            results.append({"name": name, "value": result, "passed": passed})

        failed = [r for r in results if not r["passed"]]
        if failed:
            raise ValueError(
                f"{len(failed)} quality check(s) failed: " + ", ".join(r["name"] for r in failed)
            )

        self.log.info("All %d quality checks passed", len(results))
        return results

    @staticmethod
    def _simulate_query(query: str) -> int:
        """Simulate query results for demo purposes."""
        if "COUNT" in query.upper() and "NULL" in query.upper():
            return 0  # No nulls found
        if "COUNT" in query.upper():
            return 1500  # Row count
        return 0

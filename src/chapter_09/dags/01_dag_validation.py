"""
DAG Validation — Structural Tests Without Running.

The most important Airflow tests validate DAG structure at parse time:
- No import errors (syntax, missing modules)
- No cycles in task dependencies
- Every DAG has required metadata (tags, retries, description)
- Task count matches expectations

These tests use DagBag, which is how the scheduler loads DAGs.
Run with: pytest tests/test_dag_integrity.py

This DAG demonstrates the concepts you'd test — it's itself a valid DAG
that passes all structural checks.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch09_01_dag_validation",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_09", "testing", "validation"],
    doc_md=__doc__,
)
def dag_validation():
    @task
    def show_validation_checks():
        """Display the key structural checks every DAG should pass."""
        checks = [
            ("Import check", "DagBag.import_errors should be empty"),
            ("Cycle check", "No circular dependencies between tasks"),
            ("Tags check", "Every DAG has at least one tag for UI filtering"),
            ("Retries check", "default_args includes retries >= 1"),
            ("Description check", "Every DAG has description or doc_md"),
            ("Task count", "DAG has expected number of tasks (smoke test)"),
        ]

        print("DAG VALIDATION CHECKS")
        print("=" * 50)
        for name, desc in checks:
            print(f"  {name:20s} — {desc}")

    @task
    def show_dagbag_usage():
        """Show how DagBag is used in tests."""
        code = """
# tests/test_dag_integrity.py
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors

def test_dags_have_tags():
    dag_bag = DagBag(include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' has no tags"
"""
        print("DAGBAG TEST PATTERN")
        print("=" * 50)
        print(code)
        print("Run with: pytest tests/test_dag_integrity.py -v")

    show_validation_checks() >> show_dagbag_usage()


dag_validation()

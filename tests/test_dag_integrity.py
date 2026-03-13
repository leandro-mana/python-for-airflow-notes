"""
DAG Integrity Tests.

These tests validate ALL DAGs in the project without running them:
- No import errors
- Every DAG has tags
- Every DAG has retries configured
- No cycles in task dependencies

Run with: make test
"""
from __future__ import annotations

import os
from contextlib import contextmanager
import logging

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace: str):
    """Temporarily suppress Airflow logging during DAG parsing."""
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


@pytest.fixture(scope="session")
def dag_bag():
    """Load all DAGs once per test session."""
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def test_no_import_errors(dag_bag):
    """Ensure all DAG files can be imported without errors."""
    assert not dag_bag.import_errors, (
        f"DAG import errors:\n"
        + "\n".join(f"  {k}: {v}" for k, v in dag_bag.import_errors.items())
    )


def test_dags_have_tags(dag_bag):
    """Every DAG should have at least one tag for UI filtering."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' has no tags"


def test_dags_have_retries(dag_bag):
    """Every DAG should have retries configured in default_args."""
    for dag_id, dag in dag_bag.dags.items():
        retries = dag.default_args.get("retries", 0)
        assert retries >= 1, (
            f"DAG '{dag_id}' has retries={retries}, expected >= 1"
        )


def test_dags_have_description_or_doc(dag_bag):
    """Every DAG should have a description or doc_md."""
    for dag_id, dag in dag_bag.dags.items():
        has_docs = dag.description or dag.doc_md
        assert has_docs, f"DAG '{dag_id}' has no description or doc_md"

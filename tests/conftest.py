"""
Pytest configuration for Airflow DAG testing.

Sets up the minimal Airflow environment needed for tests:
- AIRFLOW_HOME to a temp directory
- SQLite for the metadata DB (fast, no Docker needed)
- Initializes the DB once per session
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def airflow_env(tmp_path_factory):
    """Configure a minimal Airflow environment for testing."""
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "true"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{airflow_home}/airflow.db"

    # Initialize the DB
    from airflow.utils.db import initdb

    initdb()

    yield

    # Cleanup is handled by tmp_path_factory

"""
Tests for chapter 11 — custom operators.

Tests the GreetOperator and DataQualityOperator defined in ch11_03.
"""

from __future__ import annotations

import importlib

# DAG filenames start with numbers, so we use importlib
_mod = importlib.import_module("src.chapter_11.dags.03_custom_operators")
GreetOperator = _mod.GreetOperator
DataQualityOperator = _mod.DataQualityOperator


class TestGreetOperator:
    """Test the GreetOperator from ch11_03."""

    def test_default_greeting(self):
        op = GreetOperator(task_id="test_greet", name="World")
        assert op.name == "World"
        assert op.greeting == "Hello"

    def test_custom_greeting(self):
        op = GreetOperator(task_id="test_greet", name="Airflow", greeting="Welcome")
        assert op.name == "Airflow"
        assert op.greeting == "Welcome"

    def test_template_fields(self):
        assert "name" in GreetOperator.template_fields
        assert "greeting" in GreetOperator.template_fields


class TestDataQualityOperator:
    """Test the DataQualityOperator from ch11_03."""

    def test_init(self):
        op = DataQualityOperator(
            task_id="test_quality",
            table_name="orders",
            min_rows=100,
        )
        assert op.table_name == "orders"
        assert op.min_rows == 100

    def test_template_fields(self):
        assert "table_name" in DataQualityOperator.template_fields
        assert "check_date" in DataQualityOperator.template_fields

    def test_default_check_date_template(self):
        op = DataQualityOperator(task_id="test", table_name="orders")
        assert op.check_date == "{{ ds }}"

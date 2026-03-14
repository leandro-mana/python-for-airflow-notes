"""
Tests for chapter 09 — unit testing patterns.

Demonstrates the key point of ch09_02: extract business logic into
plain Python functions and test them without Airflow.
"""

from __future__ import annotations

import importlib

# DAG filenames start with numbers, so we use importlib
_mod = importlib.import_module("src.chapter_09.dags.02_unit_testing")
calculate_metrics = _mod.calculate_metrics
validate_record = _mod.validate_record


class TestCalculateMetrics:
    """Test the calculate_metrics function from ch09_02."""

    def test_basic_calculation(self):
        records = [{"amount": 100}, {"amount": 200}, {"amount": 300}]
        result = calculate_metrics(records)
        assert result["total"] == 600
        assert result["count"] == 3
        assert result["average"] == 200.0

    def test_single_record(self):
        records = [{"amount": 42.5}]
        result = calculate_metrics(records)
        assert result["total"] == 42.5
        assert result["count"] == 1
        assert result["average"] == 42.5

    def test_empty_records(self):
        result = calculate_metrics([])
        assert result["total"] == 0
        assert result["count"] == 0
        assert result["average"] == 0.0

    def test_rounding(self):
        records = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
        result = calculate_metrics(records)
        assert result["average"] == 20.0


class TestValidateRecord:
    """Test the validate_record function from ch09_02."""

    def test_valid_record(self):
        assert validate_record({"id": 1, "amount": 100}) is True

    def test_missing_id(self):
        assert validate_record({"amount": 100}) is False

    def test_missing_amount(self):
        assert validate_record({"id": 1}) is False

    def test_negative_amount(self):
        assert validate_record({"id": 1, "amount": -5}) is False

    def test_zero_amount(self):
        assert validate_record({"id": 1, "amount": 0}) is False

    def test_extra_fields_ok(self):
        assert validate_record({"id": 1, "amount": 50, "name": "Widget"}) is True

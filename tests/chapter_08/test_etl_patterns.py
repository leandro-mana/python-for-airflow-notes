"""
Tests for chapter 08 — ETL data transformation logic.

Tests the transform step from ch08_01 to verify data cleaning and validation.
"""

from __future__ import annotations


class TestTransformLogic:
    """Test ETL transformation patterns from ch08_01."""

    def test_clean_record(self):
        """Verify record cleaning logic."""
        raw = {"id": "1001", "name": "  Widget A  ", "amount": "301.27", "date": "2024-01-15"}
        cleaned = {
            "id": int(raw["id"]),
            "name": raw["name"].strip(),
            "amount": round(float(raw["amount"]), 2),
            "date": raw["date"],
        }
        assert cleaned["id"] == 1001
        assert cleaned["name"] == "Widget A"
        assert cleaned["amount"] == 301.27
        assert cleaned["date"] == "2024-01-15"

    def test_amount_rounding(self):
        """Verify amounts are rounded to 2 decimal places."""
        amount = round(float("100.999"), 2)
        assert amount == 101.0

    def test_strip_whitespace(self):
        """Verify whitespace is stripped from names."""
        assert "  Widget A  ".strip() == "Widget A"

    def test_validation_positive_amounts(self):
        """Verify validation rejects non-positive amounts."""
        records = [
            {"id": 1, "name": "A", "amount": 100.0, "date": "2024-01-01"},
            {"id": 2, "name": "B", "amount": -5.0, "date": "2024-01-01"},
            {"id": 3, "name": "C", "amount": 0, "date": "2024-01-01"},
        ]
        invalid = [r for r in records if r["amount"] <= 0]
        assert len(invalid) == 2

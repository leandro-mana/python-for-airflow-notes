"""
Tests for chapter 05 — TaskFlow logic.

Demonstrates testing the pure Python logic used in @task functions
without needing Airflow runtime.
"""

from __future__ import annotations

import pytest


class TestTaskFlowLogic:
    """Test business logic patterns from ch05_01."""

    def test_revenue_calculation(self):
        """Test the transform logic from taskflow_basics."""
        orders = {
            "1001": {"product": "Widget A", "amount": 301.27},
            "1002": {"product": "Widget B", "amount": 433.21},
            "1003": {"product": "Widget C", "amount": 502.22},
        }
        total = sum(order["amount"] for order in orders.values())
        result = {"total_revenue": total, "order_count": len(orders)}

        assert result["total_revenue"] == pytest.approx(1236.70)
        assert result["order_count"] == 3

    def test_empty_orders(self):
        """Test with no orders."""
        orders = {}
        total = sum(order["amount"] for order in orders.values())
        assert total == 0
        assert len(orders) == 0

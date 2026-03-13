"""
ETL: CSV to Database Pipeline.

A practical ETL pattern:
1. Extract: Read a CSV file
2. Transform: Clean and validate data
3. Load: Insert into PostgreSQL (idempotent upsert)

Key pattern: Idempotent loads using INSERT ON CONFLICT (upsert).
Re-running for the same date produces the same result.

This DAG uses the local PostgreSQL that ships with our Docker Compose setup.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch08_01_etl_csv_to_db",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_08", "etl", "pipeline"],
    doc_md=__doc__,
)
def etl_csv_to_db():

    @task
    def extract() -> list[dict]:
        """Extract data from a CSV source (simulated inline for demo)."""
        csv_data = """id,name,amount,date
1001,Widget A,301.27,2024-01-15
1002,Widget B,433.21,2024-01-15
1003,Widget C,502.22,2024-01-16
1004,Widget D,150.00,2024-01-16
1005,Widget E,275.50,2024-01-17"""

        reader = csv.DictReader(io.StringIO(csv_data))
        records = list(reader)
        print(f"Extracted {len(records)} records")
        return records

    @task
    def transform(records: list[dict]) -> list[dict]:
        """Clean and validate extracted data."""
        cleaned = []
        for record in records:
            cleaned.append({
                "id": int(record["id"]),
                "name": record["name"].strip(),
                "amount": round(float(record["amount"]), 2),
                "date": record["date"],
            })

        # Validation
        invalid = [r for r in cleaned if r["amount"] <= 0]
        if invalid:
            raise ValueError(f"Found {len(invalid)} records with non-positive amounts")

        print(f"Transformed {len(cleaned)} records (all valid)")
        return cleaned

    @task
    def load(records: list[dict]) -> None:
        """
        Load records to destination (simulated).

        In production, use PostgresHook or SQLAlchemy with:
            INSERT INTO orders (id, name, amount, date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                amount = EXCLUDED.amount,
                date = EXCLUDED.date;

        The ON CONFLICT clause makes this idempotent — re-running
        for the same data produces the same result.
        """
        print(f"Loading {len(records)} records (simulated)")
        total = sum(r["amount"] for r in records)
        print(f"Total amount: ${total:.2f}")
        for record in records:
            print(f"  UPSERT: {record}")

    raw = extract()
    clean = transform(raw)
    load(clean)


etl_csv_to_db()

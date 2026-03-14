"""
DAG Factory — Generating DAGs from Configuration.

When you have many similar pipelines (e.g., one per customer, per table, per region),
a DAG factory generates DAGs programmatically from a config file.

Pattern:
1. Define pipeline config in YAML/JSON/dict
2. Loop over configs and create a DAG for each
3. Register each DAG in globals() so Airflow discovers it

Warning: Keep the factory code fast — it runs on every scheduler parse.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Pipeline configuration — in production, load from YAML/JSON file
PIPELINE_CONFIGS = [
    {
        "name": "customer_alpha",
        "source_table": "alpha_events",
        "schedule": "@daily",
        "owner": "data_team",
    },
    {
        "name": "customer_beta",
        "source_table": "beta_events",
        "schedule": "@hourly",
        "owner": "data_team",
    },
    {
        "name": "customer_gamma",
        "source_table": "gamma_events",
        "schedule": "0 6 * * 1-5",  # Weekdays at 6 AM
        "owner": "analytics_team",
    },
]


def create_pipeline_dag(config: dict):
    """Factory function that creates a DAG from a config dict."""

    @dag(
        dag_id=f"ch10_02_factory_{config['name']}",
        schedule=config["schedule"],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args={"owner": config["owner"], "retries": 2},
        tags=["chapter_10", "factory", config["name"]],
        doc_md=f"Auto-generated pipeline for **{config['name']}**.",
    )
    def pipeline():
        extract = BashOperator(
            task_id="extract",
            bash_command=f"echo 'Extracting from {config['source_table']}'",
        )

        @task
        def transform():
            print(f"Transforming data from {config['source_table']}")
            return {"source": config["source_table"], "rows": 1000}

        @task
        def load(data: dict):
            print(f"Loading {data['rows']} rows from {data['source']}")

        extract >> load(transform())

    return pipeline()


# Generate and register DAGs — each call creates a new DAG in Airflow
for config in PIPELINE_CONFIGS:
    globals()[f"factory_{config['name']}"] = create_pipeline_dag(config)

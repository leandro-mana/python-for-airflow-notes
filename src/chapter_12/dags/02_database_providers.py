"""
Database Providers — SQL Execution Patterns.

Airflow supports many databases via provider packages:
- PostgreSQL: apache-airflow-providers-postgres
- MySQL: apache-airflow-providers-mysql
- Snowflake: apache-airflow-providers-snowflake
- BigQuery: apache-airflow-providers-google

Common patterns:
- SQLExecuteQueryOperator: Run SQL against any supported database
- SQL check operators: Data quality via SQL
- Hooks: Programmatic database access in @task functions

This DAG demonstrates SQL patterns using the local PostgreSQL in our Docker Compose.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch12_02_database_providers",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_12", "providers", "database"],
    doc_md=__doc__,
)
def database_providers():

    @task
    def explain_sql_operators():
        """SQL operator patterns for databases."""
        print(
            "SQL OPERATORS\n"
            "=============\n\n"
            "# Run SQL on any supported database:\n"
            "SQLExecuteQueryOperator(\n"
            "    task_id='create_table',\n"
            "    conn_id='postgres_default',\n"
            "    sql='''\n"
            "        CREATE TABLE IF NOT EXISTS daily_metrics (\n"
            "            date DATE PRIMARY KEY,\n"
            "            revenue DECIMAL(10,2),\n"
            "            orders INTEGER\n"
            "        )\n"
            "    ''',\n"
            ")\n\n"
            "# Templated SQL file:\n"
            "SQLExecuteQueryOperator(\n"
            "    task_id='load_data',\n"
            "    conn_id='postgres_default',\n"
            "    sql='sql/load_daily_metrics.sql',  # Jinja-rendered .sql file\n"
            "    params={'date': '{{ ds }}'},\n"
            ")"
        )

    @task
    def explain_sql_checks():
        """SQL check operator patterns."""
        print(
            "SQL CHECK OPERATORS\n"
            "===================\n\n"
            "# Check a single value:\n"
            "SQLCheckOperator(\n"
            "    task_id='check_row_count',\n"
            "    conn_id='postgres_default',\n"
            "    sql=\"SELECT COUNT(*) > 0 FROM orders WHERE date = '{{ ds }}'\",\n"
            ")\n\n"
            "# Check value is within threshold:\n"
            "SQLThresholdCheckOperator(\n"
            "    task_id='check_revenue',\n"
            "    conn_id='postgres_default',\n"
            "    sql=\"SELECT SUM(amount) FROM orders WHERE date = '{{ ds }}'\",\n"
            "    min_threshold=1000,\n"
            "    max_threshold=1000000,\n"
            ")"
        )

    @task
    def explain_hooks():
        """Using hooks for programmatic database access."""
        print(
            "DATABASE HOOKS\n"
            "==============\n\n"
            "# Use hooks in @task functions:\n"
            "@task\n"
            "def query_postgres():\n"
            "    from airflow.providers.postgres.hooks.postgres import PostgresHook\n"
            "    hook = PostgresHook(postgres_conn_id='postgres_default')\n"
            "    records = hook.get_records('SELECT * FROM orders LIMIT 10')\n"
            "    df = hook.get_pandas_df('SELECT * FROM orders')  # Returns DataFrame\n"
            "    return records\n\n"
            "Hooks give you full programmatic control over the connection."
        )

    explain_sql_operators() >> explain_sql_checks() >> explain_hooks()


database_providers()

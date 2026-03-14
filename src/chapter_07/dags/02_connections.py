"""
Connections — Managing External System Credentials.

Connections store credentials for databases, APIs, and cloud services:
- Set via UI (Admin → Connections), CLI, env vars, or secrets backends
- Each connection has: conn_id, conn_type, host, schema, login, password, port, extra
- Hooks use connections to interact with external systems

Environment variable format:
  AIRFLOW_CONN_MY_DB='postgresql://user:pass@host:5432/dbname'

This DAG demonstrates how to use connections in tasks.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


@dag(
    dag_id="ch07_02_connections",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_07", "configuration", "connections"],
    doc_md=__doc__,
)
def connections_example():
    @task
    def inspect_connection():
        """Retrieve and inspect a connection (using the built-in postgres connection)."""
        try:
            conn = BaseHook.get_connection("airflow_db")
            print(f"Connection ID: {conn.conn_id}")
            print(f"Connection Type: {conn.conn_type}")
            print(f"Host: {conn.host}")
            print(f"Schema: {conn.schema}")
            print(f"Port: {conn.port}")
            print(f"Login: {conn.login}")
            # Never print passwords in production!
            print(f"Extra: {conn.extra}")
        except Exception as e:
            print(f"Connection 'airflow_db' not found: {e}")
            print("This is expected if you haven't set up this connection yet.")
            print("Set it via: Admin → Connections in the Airflow UI")

    @task
    def connection_best_practices():
        """Guidelines for managing connections."""
        print(
            "CONNECTION BEST PRACTICES\n"
            "========================\n"
            "1. Use the Airflow UI for dev, secrets backends for prod\n"
            "2. Never hardcode credentials in DAG files\n"
            "3. Use conn_id consistently — same ID across environments\n"
            "4. Set connections via env vars for Docker/K8s:\n"
            "   AIRFLOW_CONN_MY_DB='postgresql://user:pass@host:5432/db'\n"
            "5. Use Hooks (not raw connections) to interact with systems\n"
            "6. Test connections in the UI before using in DAGs"
        )

    inspect_connection() >> connection_best_practices()


connections_example()

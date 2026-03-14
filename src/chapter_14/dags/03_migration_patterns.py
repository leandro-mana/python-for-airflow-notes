"""
Migration Patterns — Airflow 1.x to 2.x.

Key changes when migrating from Airflow 1.x to 2.x:
- execution_date → logical_date (data_interval_start)
- TaskFlow API (@task decorator) replaces most PythonOperator usage
- Provider packages extracted from core (pip install apache-airflow-providers-*)
- DAG scheduling: schedule_interval → schedule
- Smart sensors removed in favor of deferrable operators

This DAG documents the migration patterns with before/after examples.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch14_03_migration_patterns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_14", "migration"],
    doc_md=__doc__,
)
def migration_patterns():
    @task
    def dag_definition_changes():
        """Changes in DAG definition syntax."""
        print(
            "DAG DEFINITION\n"
            "==============\n\n"
            "BEFORE (1.x):\n"
            "  dag = DAG('my_dag', schedule_interval='@daily', ...)\n\n"
            "AFTER (2.x):\n"
            "  dag = DAG('my_dag', schedule='@daily', ...)\n"
            "  # or with decorator:\n"
            "  @dag(schedule='@daily', ...)\n"
            "  def my_dag(): ...\n"
        )

    @task
    def date_variable_changes():
        """Changes in date-related template variables."""
        print(
            "DATE VARIABLES\n"
            "==============\n\n"
            "BEFORE (1.x): execution_date (confusing — it's actually the interval START)\n"
            "AFTER (2.x):  logical_date / data_interval_start / data_interval_end\n\n"
            "In Jinja templates:\n"
            "  BEFORE: {{ execution_date }}\n"
            "  AFTER:  {{ logical_date }} or {{ data_interval_start }}\n\n"
            "In Python:\n"
            "  BEFORE: context['execution_date']\n"
            "  AFTER:  context['logical_date']\n"
        )

    @task
    def operator_changes():
        """Changes in operators and imports."""
        print(
            "OPERATOR CHANGES\n"
            "================\n\n"
            "Providers extracted — install separately:\n"
            "  pip install apache-airflow-providers-postgres\n"
            "  pip install apache-airflow-providers-amazon\n"
            "  pip install apache-airflow-providers-google\n\n"
            "Import changes:\n"
            "  BEFORE: from airflow.operators.postgres_operator import PostgresOperator\n"
            "  AFTER:  from airflow.providers.postgres.operators.postgres import PostgresOperator\n\n"
            "  BEFORE: from airflow.operators.python_operator import PythonOperator\n"
            "  AFTER:  from airflow.operators.python import PythonOperator\n"
            "          # or better: @task decorator\n"
        )

    @task
    def taskflow_migration():
        """Migrating PythonOperator to TaskFlow."""
        print(
            "TASKFLOW MIGRATION\n"
            "==================\n\n"
            "BEFORE (PythonOperator):\n"
            "  def my_func(**kwargs):\n"
            "      ti = kwargs['ti']\n"
            "      data = ti.xcom_pull(task_ids='upstream')\n"
            "      result = process(data)\n"
            "      ti.xcom_push(key='result', value=result)\n\n"
            "  task = PythonOperator(\n"
            "      task_id='my_task',\n"
            "      python_callable=my_func,\n"
            "  )\n\n"
            "AFTER (TaskFlow):\n"
            "  @task\n"
            "  def my_func(data: dict) -> dict:\n"
            "      return process(data)\n\n"
            "  result = my_func(upstream_output)  # XCom is automatic!\n"
        )

    (
        dag_definition_changes()
        >> date_variable_changes()
        >> operator_changes()
        >> taskflow_migration()
    )


migration_patterns()

"""
Jinja Templating — Dynamic Values at Runtime.

Airflow uses Jinja2 to template operator parameters at RUNTIME.
Only fields listed in an operator's `template_fields` are rendered.

Common template variables:
| Variable             | Value                                    |
|----------------------|------------------------------------------|
| {{ ds }}             | Logical date as YYYY-MM-DD               |
| {{ ds_nodash }}      | Logical date as YYYYMMDD                 |
| {{ logical_date }}   | Full datetime object                     |
| {{ data_interval_start }} | Start of the data interval          |
| {{ data_interval_end }}   | End of the data interval            |
| {{ params.key }}     | DAG-level or task-level params           |
| {{ var.value.key }}  | Airflow Variable value                   |
| {{ macros.ds_add(ds, 7) }} | Date math with macros              |
| {{ ti }}             | TaskInstance object                       |

Templates are rendered AFTER the scheduler sends the task to a worker,
so they don't slow down DAG parsing.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
    dag_id="ch07_03_jinja_templating",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    params={
        "environment": "development",
        "table_name": "daily_metrics",
    },
    tags=["chapter_07", "templating", "jinja"],
    doc_md=__doc__,
)
def jinja_templating():

    # Basic date templates
    date_info = BashOperator(
        task_id="date_info",
        bash_command=(
            "echo 'Logical date: {{ ds }}' && "
            "echo 'No dashes: {{ ds_nodash }}' && "
            "echo 'Interval: {{ data_interval_start }} to {{ data_interval_end }}'"
        ),
    )

    # Params — set at DAG level or overridden per trigger
    use_params = BashOperator(
        task_id="use_params",
        bash_command=(
            "echo 'Environment: {{ params.environment }}' && "
            "echo 'Table: {{ params.table_name }}'"
        ),
    )

    # Macros — date math and utilities
    date_math = BashOperator(
        task_id="date_math",
        bash_command=(
            "echo 'Today: {{ ds }}' && "
            "echo '7 days ago: {{ macros.ds_add(ds, -7) }}' && "
            "echo '30 days ago: {{ macros.ds_add(ds, -30) }}' && "
            "echo 'Formatted: {{ macros.ds_format(ds, \"%Y-%m-%d\", \"%d/%m/%Y\") }}'"
        ),
    )

    # Practical example: partitioned SQL-like command
    partitioned_query = BashOperator(
        task_id="partitioned_query",
        bash_command=(
            "echo \"SELECT * FROM {{ params.table_name }} "
            "WHERE date = '{{ ds }}' "
            "AND environment = '{{ params.environment }}'\""
        ),
    )

    date_info >> use_params >> date_math >> partitioned_query


jinja_templating()

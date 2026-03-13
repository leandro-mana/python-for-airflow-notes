"""
BashOperator — Running Shell Commands.

BashOperator executes bash commands in a temporary directory. Key features:
- Templated commands: Use Jinja {{ ds }}, {{ params }} in bash_command
- Environment variables: Pass env vars to the bash subprocess
- Return value: Stdout of the last line is pushed to XCom (if do_xcom_push=True)

Common use cases: Running scripts, data exports, calling CLI tools, cron migration.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ch04_01_bash_operator",
    default_args=default_args,
    description="BashOperator examples — shell commands, templates, env vars",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chapter_04", "operators", "bash"],
) as dag:

    # Basic command
    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date:' && date",
    )

    # Templated command — {{ ds }} is the logical date (YYYY-MM-DD)
    templated = BashOperator(
        task_id="templated_command",
        bash_command=(
            "echo 'Logical date: {{ ds }}' && "
            "echo 'Year: {{ macros.ds_format(ds, \"%Y-%m-%d\", \"%Y\") }}'"
        ),
    )

    # Environment variables
    with_env = BashOperator(
        task_id="with_env_vars",
        bash_command="echo \"Processing for environment: $ENVIRONMENT, region: $REGION\"",
        env={"ENVIRONMENT": "development", "REGION": "us-east-1"},
        append_env=True,  # Inherit existing env vars + add these
    )

    # Multi-line script
    multi_line = BashOperator(
        task_id="multi_line_script",
        bash_command=(
            "echo 'Step 1: Check disk space' && df -h / | tail -1 && "
            "echo 'Step 2: Check memory' && vm_stat | head -5 && "
            "echo 'Step 3: Done'"
        ),
    )

    # Capturing output via XCom (last line of stdout)
    capture_output = BashOperator(
        task_id="capture_output",
        bash_command="echo 'result_42'",
        do_xcom_push=True,  # Default is True — last stdout line goes to XCom
    )

    print_date >> templated >> with_env >> multi_line >> capture_output

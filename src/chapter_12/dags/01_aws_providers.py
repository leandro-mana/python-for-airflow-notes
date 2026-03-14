"""
AWS Providers — Cloud-Native Operators.

Airflow's AWS provider package (apache-airflow-providers-amazon) includes:
- S3: Upload, download, list, copy objects
- Lambda: Invoke serverless functions
- ECS/Fargate: Run containerized tasks
- Glue: ETL jobs
- Step Functions: Orchestrate AWS state machines

This DAG simulates AWS patterns using BashOperator (no real AWS credentials needed).
In production, install the provider and use the real operators with connections.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="ch12_01_aws_providers",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_12", "providers", "aws"],
    doc_md=__doc__,
)
def aws_providers():
    @task
    def explain_s3():
        """S3 operator patterns."""
        print(
            "S3 OPERATORS\n"
            "============\n"
            "# Upload a file:\n"
            "S3CreateObjectOperator(\n"
            "    task_id='upload',\n"
            "    s3_bucket='my-bucket',\n"
            "    s3_key='data/{{ ds }}/output.csv',\n"
            "    data=csv_string,\n"
            "    aws_conn_id='aws_default',\n"
            ")\n\n"
            "# Check if file exists:\n"
            "S3KeySensor(\n"
            "    task_id='wait_for_file',\n"
            "    bucket_key='data/{{ ds }}/*.csv',\n"
            "    wildcard_match=True,\n"
            "    aws_conn_id='aws_default',\n"
            ")"
        )

    @task
    def explain_lambda():
        """Lambda operator patterns."""
        print(
            "LAMBDA OPERATORS\n"
            "================\n"
            "LambdaInvokeFunctionOperator(\n"
            "    task_id='invoke',\n"
            "    function_name='my-transform-function',\n"
            "    payload=json.dumps({'date': '{{ ds }}'}),\n"
            "    aws_conn_id='aws_default',\n"
            ")"
        )

    @task
    def explain_ecs():
        """ECS/Fargate operator patterns."""
        print(
            "ECS OPERATORS\n"
            "=============\n"
            "EcsRunTaskOperator(\n"
            "    task_id='run_container',\n"
            "    cluster='my-cluster',\n"
            "    task_definition='my-task-def',\n"
            "    launch_type='FARGATE',\n"
            "    overrides={'containerOverrides': [...]},\n"
            "    aws_conn_id='aws_default',\n"
            ")\n\n"
            "Great for heavy compute tasks that need custom environments."
        )

    # Simulate a local version of the pattern
    simulate_upload = BashOperator(
        task_id="simulate_s3_upload",
        bash_command=(
            "echo 'Simulating S3 upload: data/{{ ds }}/output.json' && "
            "echo '{\"rows\": 1000}' > /opt/airflow/data/simulated_upload.json && "
            "echo 'Upload complete'"
        ),
    )

    [explain_s3(), explain_lambda(), explain_ecs()] >> simulate_upload


aws_providers()

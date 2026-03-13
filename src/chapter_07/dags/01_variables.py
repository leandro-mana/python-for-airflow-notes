"""
Airflow Variables — Runtime Configuration.

Variables store key-value pairs in the metadata DB, accessible from DAGs:
- Set via UI (Admin → Variables), CLI, or API
- Variable.get("key") retrieves the value at RUNTIME (not parse time)
- Support JSON values with deserialize_json=True
- Environment variable fallback: AIRFLOW_VAR_<KEY> (uppercase)

IMPORTANT: Never use Variable.get() at the TOP LEVEL of a DAG file!
Top-level code runs every time the scheduler parses the file (every 30s).
This creates a DB query on every parse, slowing down the scheduler.
Always use Variable.get() inside a task or as a Jinja template.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator


# BAD — DO NOT DO THIS:
# environment = Variable.get("environment")  # DB query on every DAG parse!

# GOOD — Use Jinja template (rendered at runtime, not parse time):
# bash_command="echo {{ var.value.environment }}"


@dag(
    dag_id="ch07_01_variables",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_07", "configuration", "variables"],
    doc_md=__doc__,
)
def variables_example():

    # Option 1: Jinja template (BEST — no DB query at parse time)
    print_var = BashOperator(
        task_id="print_var_template",
        bash_command=(
            "echo 'Environment: {{ var.value.get(\"environment\", \"development\") }}'"
        ),
    )

    # Option 2: Variable.get() inside a task (OK — runs at execution time)
    @task
    def use_variable():
        """Variable.get() inside a task is fine — it only runs at execution time."""
        env = Variable.get("environment", default_var="development")
        config = Variable.get(
            "pipeline_config",
            default_var='{"batch_size": 1000, "timeout": 300}',
            deserialize_json=True,
        )
        print(f"Environment: {env}")
        print(f"Config: {config}")
        print(f"Batch size: {config.get('batch_size', 1000)}")

    @task
    def set_variable():
        """You can also set Variables programmatically."""
        Variable.set("last_run", datetime.now().isoformat())
        Variable.set(
            "pipeline_config",
            {"batch_size": 2000, "timeout": 600},
            serialize_json=True,
        )
        print("Variables updated")

    print_var >> use_variable() >> set_variable()


variables_example()

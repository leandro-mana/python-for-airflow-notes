"""
Airflow Architecture — Understanding the Components.

This DAG doesn't do real work — it serves as a living diagram of Airflow's
architecture. Each task represents a component and logs what that component does.

Airflow components:
- **Scheduler**: Parses DAG files, creates DagRuns, schedules tasks
- **Webserver**: Flask app serving the Airflow UI
- **Executor**: Determines HOW tasks run (Local, Celery, Kubernetes)
- **Workers**: Processes that actually execute task logic
- **Metadata DB**: PostgreSQL/MySQL storing DAG state, XComs, variables
- **Triggerer**: Handles deferrable (async) operators efficiently

Run this DAG and read the logs of each task to learn what each component does.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch01_02_airflow_architecture",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_01", "architecture"],
    doc_md=__doc__,
)
def airflow_architecture():
    @task
    def explain_scheduler():
        """The Scheduler is the heart of Airflow."""
        print(
            "SCHEDULER\n"
            "=========\n"
            "- Continuously parses DAG files in the dags/ folder\n"
            "- Creates DagRun objects based on schedule intervals\n"
            "- Determines which tasks are ready to run (dependencies met)\n"
            "- Sends tasks to the Executor\n"
            "- Config: AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL (default 300s)\n"
            "- Tip: Keep DAG files lightweight — heavy top-level code slows parsing"
        )

    @task
    def explain_executor():
        """The Executor determines HOW tasks are run."""
        print(
            "EXECUTOR\n"
            "========\n"
            "- LocalExecutor: Tasks run as subprocesses on the scheduler machine\n"
            "- CeleryExecutor: Tasks distributed to worker nodes via message broker\n"
            "- KubernetesExecutor: Each task runs in its own Kubernetes pod\n"
            "- CeleryKubernetesExecutor: Hybrid — Celery for fast tasks, K8s for heavy\n"
            "\n"
            "This local setup uses CeleryExecutor with Redis as the broker."
        )

    @task
    def explain_metadata_db():
        """The metadata database stores all Airflow state."""
        print(
            "METADATA DATABASE\n"
            "=================\n"
            "- Stores DAG definitions, DagRuns, TaskInstances, XComs\n"
            "- Stores Variables, Connections, and user permissions\n"
            "- PostgreSQL (production) or SQLite (dev only)\n"
            "- All components connect to this DB\n"
            "- This local setup uses PostgreSQL 16"
        )

    @task
    def explain_webserver():
        """The Webserver provides the Airflow UI."""
        print(
            "WEBSERVER\n"
            "=========\n"
            "- Flask application serving the Airflow UI\n"
            "- Views: Grid (run history), Graph (dependencies), Code, Gantt\n"
            "- Trigger DAGs, inspect logs, manage Variables and Connections\n"
            "- Default: http://localhost:8080\n"
            "- Reads state from the metadata database (not from DAG files directly)"
        )

    @task
    def explain_triggerer():
        """The Triggerer handles deferrable (async) operators."""
        print(
            "TRIGGERER\n"
            "=========\n"
            "- New in Airflow 2.2+\n"
            "- Handles deferrable operators that wait for external events\n"
            "- Uses asyncio — one triggerer can handle thousands of waiting tasks\n"
            "- Frees up worker slots while tasks wait (sensors, API polling)\n"
            "- Deferrable operators are the modern replacement for sensors in poke mode"
        )

    # All components are independent — they can be explained in any order.
    # Using a fan-out pattern from scheduler to show they're all peers.
    scheduler = explain_scheduler()
    executor = explain_executor()
    metadata = explain_metadata_db()
    webserver = explain_webserver()
    triggerer = explain_triggerer()

    # The scheduler orchestrates everything, so it "feeds" the other components.
    scheduler >> [executor, metadata, webserver, triggerer]


airflow_architecture()

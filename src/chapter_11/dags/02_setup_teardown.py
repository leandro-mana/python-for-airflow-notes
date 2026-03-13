"""
Setup & Teardown Tasks — Resource Lifecycle Management.

Setup/teardown tasks (Airflow 2.7+) guarantee cleanup runs even if work tasks fail:
- @setup: Runs before work tasks (create resources)
- @teardown: Always runs after work tasks (cleanup resources)
- If a work task fails, teardown STILL executes

Use cases: Spinning up/down clusters, creating/dropping temp tables,
acquiring/releasing locks.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, setup, task, teardown


@dag(
    dag_id="ch11_02_setup_teardown",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_11", "setup_teardown"],
    doc_md=__doc__,
)
def setup_teardown_example():

    @setup
    def create_cluster() -> dict:
        """Setup: Create a compute cluster (simulated)."""
        cluster_id = "cluster-abc-123"
        print(f"Creating cluster: {cluster_id}")
        print("Cluster is ready")
        return {"cluster_id": cluster_id}

    @task
    def run_job(cluster: dict) -> None:
        """Work: Run the actual computation on the cluster."""
        print(f"Running job on {cluster['cluster_id']}")
        print("Job completed successfully")

    @teardown
    def destroy_cluster(cluster: dict) -> None:
        """
        Teardown: Destroy the cluster.

        This runs EVEN IF run_job fails — guaranteed cleanup.
        Without setup/teardown, a failed task could leave resources orphaned.
        """
        print(f"Destroying cluster: {cluster['cluster_id']}")
        print("Cluster destroyed — no orphaned resources")

    cluster = create_cluster()
    run_job(cluster) >> destroy_cluster(cluster)


setup_teardown_example()

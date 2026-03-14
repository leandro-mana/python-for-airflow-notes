"""
Secrets Backends — Pluggable Credential Storage.

Airflow supports external secrets backends for Variables and Connections:
- Environment variables (default fallback)
- AWS Secrets Manager
- GCP Secret Manager
- HashiCorp Vault
- Azure Key Vault

Backends are configured in airflow.cfg or via environment variables:
  AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
  AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}

Lookup order:
1. Secrets backend (e.g., AWS Secrets Manager)
2. Environment variables (AIRFLOW_CONN_*, AIRFLOW_VAR_*)
3. Metadata database (UI / CLI)

This DAG demonstrates the concepts and shows how to verify which backend is active.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch07_04_secrets_backend",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_07", "configuration", "secrets"],
    doc_md=__doc__,
)
def secrets_backend():
    @task
    def show_backend_config():
        """Display the currently configured secrets backend."""
        from airflow.configuration import conf

        backend = conf.get("secrets", "backend", fallback="")
        backend_kwargs = conf.get("secrets", "backend_kwargs", fallback="{}")

        print("SECRETS BACKEND CONFIGURATION")
        print("=" * 40)
        print(f"Backend class: {backend or '(default — metastore)'}")
        print(f"Backend kwargs: {backend_kwargs}")
        print()
        print("If no backend is set, Airflow uses the metadata DB")
        print("(Admin -> Variables / Connections in the UI).")

    @task
    def env_var_secrets():
        """
        Environment variables as a secrets backend.

        This is the simplest approach and works well with Docker/K8s:
        - Connections: AIRFLOW_CONN_<CONN_ID>='<connection_uri>'
        - Variables: AIRFLOW_VAR_<KEY>='<value>'

        Env vars take precedence over the metadata DB.
        """
        import os

        print("ENVIRONMENT VARIABLE SECRETS")
        print("=" * 40)

        # Check for connection env vars
        conn_vars = {k: v for k, v in os.environ.items() if k.startswith("AIRFLOW_CONN_")}
        print(f"Connection env vars found: {len(conn_vars)}")
        for key in sorted(conn_vars):
            print(f"  {key} = ****")

        # Check for variable env vars
        var_vars = {k: v for k, v in os.environ.items() if k.startswith("AIRFLOW_VAR_")}
        print(f"Variable env vars found: {len(var_vars)}")
        for key in sorted(var_vars):
            print(f"  {key} = {os.environ[key]}")

    @task
    def backend_comparison():
        """Compare secrets backend options."""
        backends = [
            {
                "name": "Environment Variables",
                "class": "(built-in)",
                "pros": "Simple, works everywhere, no extra dependencies",
                "cons": "Not suitable for many secrets, no rotation, visible in process list",
                "use_case": "Local dev, small deployments, Docker Compose",
            },
            {
                "name": "AWS Secrets Manager",
                "class": "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
                "pros": "Automatic rotation, fine-grained IAM, audit trail",
                "cons": "AWS-only, cost per secret per month",
                "use_case": "Production on AWS / MWAA",
            },
            {
                "name": "GCP Secret Manager",
                "class": "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend",
                "pros": "Integrated with GCP IAM, versioning, automatic rotation",
                "cons": "GCP-only",
                "use_case": "Production on GCP / Cloud Composer",
            },
            {
                "name": "HashiCorp Vault",
                "class": "airflow.providers.hashicorp.secrets.vault.VaultBackend",
                "pros": "Cloud-agnostic, dynamic secrets, leasing, encryption as a service",
                "cons": "Operational overhead, requires Vault cluster",
                "use_case": "Multi-cloud, on-prem, strict compliance requirements",
            },
        ]

        print("SECRETS BACKEND COMPARISON")
        print("=" * 60)
        for b in backends:
            print(f"\n{b['name']}")
            print(f"  Class: {b['class']}")
            print(f"  Pros:  {b['pros']}")
            print(f"  Cons:  {b['cons']}")
            print(f"  Use:   {b['use_case']}")

    show_backend_config() >> env_var_secrets() >> backend_comparison()


secrets_backend()

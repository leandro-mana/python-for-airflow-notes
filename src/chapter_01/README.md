# Chapter 01: Introduction to Apache Airflow

What Airflow is, when to use it, and how it works under the hood.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_hello_airflow.py` | First DAG, `default_args`, `dag_id`, `schedule`, `start_date`, `catchup=False`, `>>` operator |
| 2 | `02_airflow_architecture.py` | Scheduler, webserver, executor, metadata DB, triggerer — what each component does |

## Key Concepts

### What is Apache Airflow?

Apache Airflow is an open-source platform for **programmatically authoring, scheduling, and
monitoring workflows**. Workflows are defined as Python code (DAGs), making them versionable,
testable, and collaborative.

### DAG — Directed Acyclic Graph

A DAG is a collection of tasks with defined dependencies:
- **Directed**: Dependencies flow in one direction (task A → task B)
- **Acyclic**: No circular dependencies (A → B → A is invalid)
- **Graph**: Tasks are nodes, dependencies are edges

### Core Philosophy

- **Workflows as code** — DAGs are Python files, not YAML or drag-and-drop
- **Idempotency** — running a DAG twice for the same interval should produce the same result
- **Extensibility** — operators, hooks, and providers for any system
- **Scalability** — from a single machine to thousands of parallel tasks

### Architecture Components

| Component | Role |
|-----------|------|
| **Scheduler** | Parses DAGs, creates runs, sends tasks to executor |
| **Webserver** | Flask UI for monitoring and management |
| **Executor** | Determines HOW tasks run (Local, Celery, Kubernetes) |
| **Workers** | Processes that execute task logic |
| **Metadata DB** | PostgreSQL storing all state (runs, XComs, variables) |
| **Triggerer** | Handles async/deferrable operators efficiently |

### Airflow vs Alternatives

| Feature | Airflow | Prefect | Dagster | Step Functions |
|---------|---------|---------|---------|----------------|
| Language | Python | Python | Python | JSON/YAML |
| Scheduling | Built-in | Built-in | Built-in | EventBridge |
| UI | Rich web UI | Cloud UI | Rich web UI | Console |
| Ecosystem | 80+ providers | Integrations | IO Managers | AWS services |
| Deployment | Self-hosted / managed | Cloud / self-hosted | Cloud / self-hosted | Serverless |
| Best for | Data pipelines at scale | Modern Python workflows | Data assets & lineage | AWS-native workflows |

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Astronomer — Introduction to Airflow](https://docs.astronomer.io/learn/intro-to-airflow)
- [Airflow GitHub — Example DAGs](https://github.com/apache/airflow/tree/main/airflow/example_dags)

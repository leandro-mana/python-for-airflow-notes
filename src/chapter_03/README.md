# Chapter 03: Core Concepts

The building blocks: DAGs, tasks, operators, dependencies, and scheduling.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_dag_structure.py` | DAG context manager vs `@dag` decorator, default args, tags, `doc_md` |
| 2 | `02_task_dependencies.py` | `>>` / `<<` operators, `chain()`, `cross_downstream()`, task groups |
| 3 | `03_scheduling.py` | Cron expressions, timetables, presets (`@daily`, `@hourly`), data intervals, `logical_date` |
| 4 | `04_catchup_and_backfill.py` | `catchup=True/False`, `airflow dags backfill`, idempotent design |

## Key Concepts

- **DAG structure** — context manager vs decorator, default args inheritance
- **Task dependencies** — bitshift operators (`>>`), `chain()`, fan-in/fan-out
- **Scheduling** — cron, timetables, data intervals, `logical_date` vs legacy `execution_date`
- **Catchup & backfill** — how Airflow handles historical runs

## References

- [Airflow — DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Airflow — Scheduling & Triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/index.html)
- [Astronomer — Scheduling in Airflow](https://docs.astronomer.io/learn/scheduling-in-airflow)

# Chapter 11: Advanced Features

Modern Airflow features that set apart production deployments.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_datasets.py` | Data-aware scheduling, `Dataset`, producer/consumer DAGs, dataset URI |
| 2 | `02_setup_teardown.py` | `@setup` / `@teardown` tasks, resource lifecycle, cleanup guarantees |
| 3 | `03_custom_operators.py` | `BaseOperator` subclass, custom hooks, templated fields, provider packages |
| 4 | `04_object_storage.py` | `ObjectStoragePath`, universal file handling (local/S3/GCS/Azure), `@task.virtualenv` |

## Key Concepts

- **Datasets** — data-aware scheduling that triggers DAGs when upstream data changes
- **Setup/teardown** — guaranteed resource lifecycle management
- **Custom operators** — extending Airflow with domain-specific operators and hooks
- **Object storage** — unified file API across local, S3, GCS, and Azure

## References

- [Airflow — Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [Airflow — Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)
- [Astronomer — Datasets](https://docs.astronomer.io/learn/airflow-datasets)

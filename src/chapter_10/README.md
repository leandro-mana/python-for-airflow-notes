# Chapter 10: Dynamic DAGs

Generating DAGs programmatically for scale.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_dynamic_task_mapping.py` | `.expand()`, `.partial()`, mapped tasks, `zip_longest`, reducing mapped outputs |
| 2 | `02_dag_factory.py` | Generating DAGs from config (YAML/JSON), factory pattern, when to use it |
| 3 | `03_dynamic_task_groups.py` | Nested task groups, programmatic group generation, DAG-level params |

## Key Concepts

- **Dynamic task mapping** — `.expand()` for runtime-determined parallelism
- **DAG factory** — generating DAGs from configuration files (YAML/JSON)
- **Dynamic task groups** — programmatic group creation for complex workflows
- **When to go dynamic** — trade-offs between explicit and generated DAGs

## References

- [Airflow — Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Astronomer — Dynamic Tasks](https://docs.astronomer.io/learn/dynamic-tasks)

# Chapter 05: TaskFlow API

The modern, Pythonic way to write Airflow DAGs.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_taskflow_basics.py` | `@task` decorator, automatic XCom, type hints, return values as inputs |
| 2 | `02_taskflow_xcom.py` | XCom under the hood, `xcom_push`/`xcom_pull`, multiple outputs, XCom backends |
| 3 | `03_taskflow_with_operators.py` | Mixing `@task` with traditional operators, `@task.bash`, `@task.virtualenv`, `@task.docker` |
| 4 | `04_taskflow_patterns.py` | Dynamic task mapping (`expand()`), task groups with `@task_group`, conditional logic |

## Key Concepts

- **TaskFlow API** — `@task` decorator for Python-native DAG authoring
- **Automatic XCom** — return values become XCom entries, passed implicitly between tasks
- **Multiple outputs** — returning dicts with `multiple_outputs=True`
- **Mixing paradigms** — combining `@task` with traditional operators seamlessly

## References

- [Airflow — TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Astronomer — Using the TaskFlow API](https://docs.astronomer.io/learn/airflow-decorators)

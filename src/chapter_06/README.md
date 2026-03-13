# Chapter 06: Branching & Control Flow

Conditional execution, trigger rules, and complex DAG patterns.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_branching.py` | `@task.branch`, `BranchPythonOperator`, join patterns, `none_failed_min_one_success` |
| 2 | `02_trigger_rules.py` | All trigger rules (`all_success`, `one_failed`, `none_failed`, etc.), use cases for each |
| 3 | `03_conditional_patterns.py` | `ShortCircuitOperator`, `LatestOnlyOperator`, `TriggerDagRunOperator`, cross-DAG dependencies |
| 4 | `04_error_handling.py` | `on_failure_callback`, `on_success_callback`, `retries`, `retry_delay`, SLA misses |

## Key Concepts

- **Branching** — conditional task execution based on runtime logic
- **Trigger rules** — controlling when downstream tasks execute based on upstream states
- **Cross-DAG dependencies** — `TriggerDagRunOperator`, `ExternalTaskSensor`
- **Error handling** — callbacks, retries, SLA monitoring

## References

- [Airflow — Branching](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#branching)
- [Astronomer — Branching in Airflow](https://docs.astronomer.io/learn/airflow-branch-operator)

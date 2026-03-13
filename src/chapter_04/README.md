# Chapter 04: Operators Deep Dive

The core operator types and when to use each one.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_bash_operator.py` | BashOperator, templated commands, env vars, working directory |
| 2 | `02_python_operator.py` | PythonOperator, `op_args`, `op_kwargs`, return values |
| 3 | `03_sensors.py` | FileSensor, HttpSensor, ExternalTaskSensor, `mode='poke'` vs `'reschedule'`, `timeout`, `poke_interval` |
| 4 | `04_other_operators.py` | EmailOperator, SimpleHttpOperator, BranchPythonOperator, ShortCircuitOperator |
| 5 | `05_deferrable_operators.py` | Async operators, triggerer, `BaseTrigger`, why deferrable > sensors for long waits |

## Key Concepts

- **Operators** — pre-built task templates (Bash, Python, HTTP, SQL, etc.)
- **Sensors** — special operators that wait for a condition, with poke vs reschedule modes
- **Deferrable operators** — async execution that frees worker slots during long waits
- **Choosing operators** — when to use each type for clarity and efficiency

## References

- [Airflow — Operators and Hooks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Astronomer — Airflow Sensors](https://docs.astronomer.io/learn/what-is-a-sensor)
- [Astronomer — Deferrable Operators](https://docs.astronomer.io/learn/deferrable-operators)

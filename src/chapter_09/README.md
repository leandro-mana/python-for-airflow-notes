# Chapter 09: Testing & Debugging

Writing reliable tests and debugging DAGs effectively.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_dag_validation.py` | `DagBag` tests, import errors, cycle detection, task count assertions |
| 2 | `02_unit_testing.py` | Testing Python callables, mocking connections/variables, `pytest` fixtures |
| 3 | `03_integration_testing.py` | Testing with a running Airflow (Astro CLI), end-to-end DAG runs |
| 4 | `04_debugging.py` | `airflow tasks test`, log levels, breakpoints, common errors and fixes |

## Key Concepts

- **DAG validation** — `DagBag` import checks, structural assertions
- **Unit testing** — testing task callables in isolation with mocked Airflow context
- **Integration testing** — end-to-end DAG runs against a live Airflow environment
- **Debugging** — `airflow tasks test`, log inspection, common pitfalls

## References

- [Airflow — Testing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag)
- [Astronomer — Testing Airflow DAGs](https://docs.astronomer.io/learn/testing-airflow)

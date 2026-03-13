# Chapter 14: Best Practices & Anti-Patterns

Lessons learned from production Airflow deployments.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_best_practices.py` | Top-level code, DAG file parse time, idempotency, atomic tasks, `start_date` gotchas |
| 2 | `02_anti_patterns.py` | DAGs that won't scale: heavy top-level imports, XCom abuse, monolithic tasks |
| 3 | `03_migration_patterns.py` | Airflow 1.x to 2.x patterns, deprecated APIs, upgrading strategies |

## Key Concepts

- **Top-level code** — minimize code at module level to reduce DAG parse time
- **Idempotency** — every task should produce the same result on re-execution
- **Atomic tasks** — single-responsibility tasks that succeed or fail cleanly
- **Anti-patterns** — XCom abuse, monolithic tasks, heavy imports, hardcoded values

## References

- [Airflow — Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer — DAG Writing Best Practices](https://docs.astronomer.io/learn/dag-best-practices)

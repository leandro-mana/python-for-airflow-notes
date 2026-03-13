# Chapter 07: Configuration & Templating

Variables, connections, secrets, and Jinja templating.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_variables.py` | `Variable.get()`, JSON variables, env var fallback, UI vs code, when NOT to use Variables |
| 2 | `02_connections.py` | Connection types, URI format, `Connection` object, provider-specific extras |
| 3 | `03_jinja_templating.py` | Template fields, `{{ ds }}`, `{{ params }}`, `{{ macros }}`, custom filters, rendering |
| 4 | `04_secrets_backend.py` | Secrets backends overview, env vars, AWS Secrets Manager, HashiCorp Vault |

## Key Concepts

- **Variables** — runtime configuration stored in the metadata DB, with env var fallback
- **Connections** — typed credentials for external systems (databases, APIs, cloud)
- **Jinja templating** — Airflow's template engine for dynamic values at runtime
- **Secrets backends** — pluggable credential storage (env vars, AWS SM, Vault)

## References

- [Airflow — Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
- [Airflow — Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Airflow — Templates Reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)

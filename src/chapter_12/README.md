# Chapter 12: Providers & Integrations

Connecting Airflow to external systems.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_aws_providers.py` | S3, Lambda, ECS/Fargate, Glue, Step Functions operators/sensors |
| 2 | `02_database_providers.py` | PostgreSQL, MySQL, Snowflake, BigQuery — SQL execution patterns |
| 3 | `03_api_providers.py` | HTTP, Slack, email, generic REST patterns |

## Key Concepts

- **Provider packages** — `apache-airflow-providers-*` modular ecosystem
- **AWS providers** — S3, Lambda, ECS operators for cloud-native pipelines
- **Database providers** — SQL operators, hooks, and connection patterns
- **Version compatibility** — matching provider versions with Airflow core

## References

- [Airflow — Provider Packages](https://airflow.apache.org/docs/apache-airflow-providers/index.html)
- [Astronomer — Provider Guides](https://docs.astronomer.io/learn)

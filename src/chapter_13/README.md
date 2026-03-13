# Chapter 13: Production Deployment

Taking Airflow from local Docker to production.

## Topics

| # | Content | Topics |
|---|---------|--------|
| 1 | README theory | Executors compared: Local, Celery, Kubernetes, CeleryKubernetes |
| 2 | README theory | CI/CD for DAGs: linting, testing, Docker image builds, GitSync |
| 3 | README theory | Monitoring: health checks, StatsD/Prometheus, Grafana dashboards, alerting |
| 4 | README theory | Managed services: MWAA, Cloud Composer, Astronomer — trade-offs |

## Key Concepts

- **Executors** — Local vs Celery vs Kubernetes, and when to use each
- **CI/CD** — automated DAG linting, testing, and deployment pipelines
- **Monitoring** — StatsD, Prometheus, Grafana for Airflow observability
- **Managed services** — AWS MWAA, GCP Cloud Composer, Astronomer comparison

## References

- [Airflow — Executor Types](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
- [Astronomer — CI/CD for Airflow](https://docs.astronomer.io/astro/ci-cd-templates/template-overview)
- [Airflow — Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/index.html)

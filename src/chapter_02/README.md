# Chapter 02: Local Development Environment

Setting up a production-like local Airflow with Docker Compose.

## DAGs

| # | File | Topics |
|---|------|--------|
| 1 | `docker-compose.yaml` | CeleryExecutor setup, PostgreSQL, Redis, webserver, scheduler, worker, triggerer |
| 2 | `Dockerfile` | Custom image, installing providers, Python dependencies |
| 3 | `Makefile` | `make up`, `make down`, `make shell`, `make logs`, `make test` |

## Key Concepts

- **CeleryExecutor** — production-grade executor using Redis as broker and PostgreSQL for results
- **Environment variables** — `AIRFLOW__SECTION__KEY` pattern overrides `airflow.cfg`
- **Docker Compose services** — webserver, scheduler, worker, triggerer, init, postgres, redis
- **Astro CLI** — Astronomer's alternative for local development

## References

- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Astronomer — Get Started](https://docs.astronomer.io/astro/cli/get-started-cli)

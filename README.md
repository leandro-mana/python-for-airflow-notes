# Python for Airflow Notes

![Python](https://img.shields.io/badge/Python-3.12+-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.10+-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![DAGs](https://img.shields.io/badge/DAGs-44-green)
![Chapters](https://img.shields.io/badge/Chapters-14-orange)
![License](https://img.shields.io/badge/License-MIT-yellow)

Study notes and runnable DAGs for Apache Airflow — from core concepts through TaskFlow, dynamic
DAGs, providers, testing, and production deployment patterns. All content is delivered through
executable DAG files designed for self-paced learning with a local Docker Compose environment.

> Part of the **Python Learning Series**:
> [Data Science](https://github.com/leandro-mana/python-for-data-science-notes) ·
> [Time Series](https://github.com/leandro-mana/python-for-time-series-notes) ·
> **Airflow** ·
> Deep Learning (planned) ·
> NLP (planned)

## Requirements

- [Python 3.12+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/#installation)
- [Docker](https://docs.docker.com/get-docker/)

## Quick Start

```bash
# 1. Install Python dependencies
make install

# 2. Copy environment file
cp .env.example .env

# 3. Initialize Airflow (first time only)
make init

# 4. Start Airflow
make up

# 5. Open http://localhost:8080 (airflow/airflow)
```

## Project Structure

```text
python-for-airflow-notes/
├── src/
│   ├── chapter_01/          # Introduction to Apache Airflow
│   ├── chapter_02/          # Local Development Environment
│   ├── chapter_03/          # Core Concepts
│   ├── chapter_04/          # Operators Deep Dive
│   ├── chapter_05/          # TaskFlow API
│   ├── chapter_06/          # Branching & Control Flow
│   ├── chapter_07/          # Configuration & Templating
│   ├── chapter_08/          # Data Pipeline Patterns
│   ├── chapter_09/          # Testing & Debugging
│   ├── chapter_10/          # Dynamic DAGs
│   ├── chapter_11/          # Advanced Features
│   ├── chapter_12/          # Providers & Integrations
│   ├── chapter_13/          # Production Deployment
│   └── chapter_14/          # Best Practices & Anti-Patterns
├── tests/                   # Pytest test suite
├── data/                    # Sample datasets for ETL examples
├── include/                 # Shared SQL, scripts, templates
├── plugins/                 # Custom operators/hooks/sensors
├── docker-compose.yaml      # Local Airflow environment
├── Dockerfile               # Custom Airflow image
├── Makefile                 # Automation commands
└── pyproject.toml           # Poetry configuration
```

## Chapters

### Part I: Foundations

| Chapter | DAGs | Topics |
| --- | --- | --- |
| **01 - Introduction to Apache Airflow** | 2 | What is Airflow, DAG concept, architecture overview, Airflow vs alternatives |
| **02 - Local Development Environment** | — | Docker Compose setup, CeleryExecutor, env vars, Astro CLI |
| **03 - Core Concepts** | 4 | DAG structure, task dependencies, scheduling, catchup & backfill |

### Part II: Building DAGs

| Chapter | DAGs | Topics |
| --- | --- | --- |
| **04 - Operators Deep Dive** | 5 | BashOperator, PythonOperator, sensors, deferrable operators |
| **05 - TaskFlow API** | 4 | `@task` decorator, XCom, mixing with operators, dynamic mapping |
| **06 - Branching & Control Flow** | 4 | Branching, trigger rules, conditional patterns, error handling |
| **07 - Configuration & Templating** | 4 | Variables, connections, Jinja templating, secrets backends |

### Part III: Real-World Patterns

| Chapter | DAGs | Topics |
| --- | --- | --- |
| **08 - Data Pipeline Patterns** | 4 | ETL/ELT, API extraction, data quality, incremental loading |
| **09 - Testing & Debugging** | 4 | DAG validation, unit testing, integration testing, debugging |
| **10 - Dynamic DAGs** | 3 | Dynamic task mapping, DAG factory, dynamic task groups |

### Part IV: Advanced & Production

| Chapter | DAGs | Topics |
| --- | --- | --- |
| **11 - Advanced Features** | 4 | Datasets, setup/teardown, custom operators, object storage |
| **12 - Providers & Integrations** | 3 | AWS, database, API providers |
| **13 - Production Deployment** | — | Executors, CI/CD, monitoring, managed services |
| **14 - Best Practices & Anti-Patterns** | 3 | Best practices, anti-patterns, migration patterns |

## Running Airflow

```bash
# Start the full Airflow stack
make up

# Stop Airflow
make down

# Open a shell in the worker container
make shell

# Tail scheduler logs
make logs
```

## Chapters & DAGs

```bash
# List all chapters
make list-chapters

# List DAGs in a chapter
make list-dags CH=04
```

## Code Quality

```bash
make lint           # Ruff linter
make format         # Auto-format
make type-check     # mypy
make check          # All checks
make test           # pytest
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for workflow, commit conventions, and code style guidelines.

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Astronomer Guides](https://docs.astronomer.io/learn)
- [Airflow GitHub — Example DAGs](https://github.com/apache/airflow/tree/main/airflow/example_dags)

## Author

[Leandro Mana](https://www.linkedin.com/in/leandro-mana/)

## License

MIT

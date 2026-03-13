# Chapter 08: Data Pipeline Patterns

Building production-quality ETL/ELT pipelines.

## DAGs

| # | DAG | Topics |
|---|-----|--------|
| 1 | `01_etl_csv_to_db.py` | CSV to PostgreSQL pipeline, idempotent loads, `INSERT ON CONFLICT` |
| 2 | `02_api_to_storage.py` | REST API extraction, pagination, writing to local/S3 storage |
| 3 | `03_data_quality.py` | Data validation checks, Great Expectations integration, custom check operators |
| 4 | `04_incremental_loading.py` | Watermark patterns, `max(updated_at)`, partition-based loading |

## Key Concepts

- **ETL vs ELT** — extract-transform-load patterns and when to use each
- **Idempotent loads** — upserts, `INSERT ON CONFLICT`, partition replacement
- **Data quality** — validation checks as first-class pipeline citizens
- **Incremental loading** — watermarks, partitions, avoiding full reloads

## References

- [Astronomer — ETL/ELT Patterns](https://docs.astronomer.io/learn/airflow-sql-data-quality)
- [Airflow — Data-aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)

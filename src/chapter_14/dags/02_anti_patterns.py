"""
Anti-Patterns — Common Mistakes to Avoid.

This DAG demonstrates CORRECT patterns alongside explanations of what NOT to do.
Each task explains a common anti-pattern and shows the fix.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch14_02_anti_patterns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_14", "anti_patterns"],
    doc_md=__doc__,
)
def anti_patterns():
    @task
    def anti_pattern_top_level_code():
        """
        ANTI-PATTERN: Heavy code at module level.

        BAD:
            import pandas as pd                    # Slow import on every parse
            data = pd.read_csv("big_file.csv")     # I/O on every parse
            config = Variable.get("config")         # DB query on every parse

        WHY IT'S BAD:
            The scheduler parses DAG files every 30 seconds.
            Heavy top-level code slows down the ENTIRE scheduler.
            With 100 DAGs doing this, you get a 100x slowdown.

        FIX:
            Move heavy imports and I/O INSIDE tasks.
            Use Jinja templates for Variables: {{ var.value.config }}
        """
        print("Keep top-level code to DAG/task definitions only")
        print("Heavy imports, I/O, and Variable.get() go INSIDE tasks")

    @task
    def anti_pattern_xcom_abuse():
        """
        ANTI-PATTERN: Passing large data through XCom.

        BAD:
            @task
            def extract():
                df = pd.read_csv("10gb_file.csv")
                return df.to_dict()  # 10GB in the metadata DB!

        WHY IT'S BAD:
            XCom stores data in the metadata database (PostgreSQL).
            Large XComs slow queries, bloat the DB, and can crash it.

        FIX:
            Write large data to object storage (S3/GCS/local).
            Pass the FILE PATH through XCom, not the data itself.

            @task
            def extract():
                df.to_parquet("/data/output.parquet")
                return "/data/output.parquet"  # Just the path!
        """
        print("XCom is for metadata (paths, counts, status)")
        print("NOT for large datasets — use object storage")

    @task
    def anti_pattern_monolithic_tasks():
        """
        ANTI-PATTERN: One giant task that does everything.

        BAD:
            @task
            def do_everything():
                data = extract_from_api()
                cleaned = clean(data)
                enriched = enrich(cleaned)
                validated = validate(enriched)
                load_to_db(validated)
                send_email()
                update_dashboard()

        WHY IT'S BAD:
            - If enrichment fails, you re-run extraction too (wasted work)
            - No visibility into which step failed
            - Can't parallelize independent steps
            - Can't retry individual steps

        FIX:
            One task per logical step. Each task is atomic and idempotent.

            extract() >> clean() >> enrich() >> validate() >> load()
        """
        print("One task = one responsibility")
        print("Enables retries, parallelism, and debugging")

    @task
    def anti_pattern_hardcoded_values():
        """
        ANTI-PATTERN: Hardcoded credentials, dates, and config.

        BAD:
            password = "super_secret_123"
            db_host = "prod-db.company.com"
            date = "2024-01-15"

        FIX:
            - Credentials: Use Connections (Admin → Connections)
            - Config: Use Variables or params
            - Dates: Use {{ ds }}, {{ logical_date }}, template variables
            - Secrets: Use a secrets backend (AWS SM, Vault)
        """
        print("Never hardcode credentials, hosts, or dates")
        print("Use Connections, Variables, and Jinja templates")

    (
        anti_pattern_top_level_code()
        >> anti_pattern_xcom_abuse()
        >> anti_pattern_monolithic_tasks()
        >> anti_pattern_hardcoded_values()
    )


anti_patterns()

"""
API to Storage Pipeline.

A common pattern: extract data from a REST API, transform it, and store locally.

This DAG demonstrates:
- HTTP API extraction with pagination awareness
- Data transformation and enrichment
- Writing results to local storage (in production: S3/GCS)
- Passing file paths via XCom (not the data itself — keep XCom small!)
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch08_02_api_to_storage",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_08", "api", "pipeline"],
    doc_md=__doc__,
)
def api_to_storage():

    @task
    def extract_from_api(**context) -> list[dict]:
        """
        Extract data from a REST API.

        In production with large datasets:
        1. Write to a temp file or object storage
        2. Pass the file PATH via XCom (not the data)
        3. Handle pagination for large result sets
        """
        import requests

        url = "https://jsonplaceholder.typicode.com/posts"
        params = {"_limit": 10}

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        print(f"Extracted {len(data)} posts from API")
        return data

    @task
    def transform(posts: list[dict]) -> list[dict]:
        """Enrich and filter the extracted data."""
        enriched = []
        for post in posts:
            enriched.append({
                "id": post["id"],
                "title": post["title"][:50],
                "title_length": len(post["title"]),
                "body_preview": post["body"][:100],
                "user_id": post["userId"],
            })

        print(f"Transformed {len(enriched)} records")
        return enriched

    @task
    def save_to_storage(records: list[dict], **context) -> str:
        """
        Save to local storage. In production, use S3Hook or GCSHook.

        Returns the file path via XCom so downstream tasks know where to find it.
        """
        ds = context["ds"]
        output_path = f"/opt/airflow/data/posts_{ds}.json"

        with open(output_path, "w") as f:
            json.dump(records, f, indent=2)

        print(f"Saved {len(records)} records to {output_path}")
        return output_path

    @task
    def verify(file_path: str) -> None:
        """Verify the saved file."""
        with open(file_path) as f:
            data = json.load(f)

        print(f"Verified: {file_path} contains {len(data)} records")

    raw = extract_from_api()
    transformed = transform(raw)
    path = save_to_storage(transformed)
    verify(path)


api_to_storage()

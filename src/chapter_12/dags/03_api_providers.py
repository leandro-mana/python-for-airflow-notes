"""
API Providers — HTTP, Slack, and Generic REST Patterns.

Airflow can interact with any REST API:
- HttpOperator / HttpSensor: Generic HTTP requests
- SlackWebhookOperator: Send Slack notifications
- SimpleHttpOperator: Quick HTTP calls with response handling

For custom APIs, use the requests library in @task functions with
connection credentials from BaseHook.get_connection().
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="ch12_03_api_providers",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["chapter_12", "providers", "api"],
    doc_md=__doc__,
)
def api_providers():
    @task
    def call_api() -> dict:
        """Call an external API using requests in a @task function."""
        import requests

        response = requests.get(
            "https://jsonplaceholder.typicode.com/todos/1",
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        print(f"API response: {data}")
        return data

    @task
    def explain_http_operators():
        """HTTP operator patterns."""
        print(
            "HTTP OPERATORS\n"
            "==============\n\n"
            "# Simple HTTP call:\n"
            "SimpleHttpOperator(\n"
            "    task_id='get_data',\n"
            "    http_conn_id='my_api',  # Connection with base URL\n"
            "    endpoint='api/v1/data',\n"
            "    method='GET',\n"
            "    headers={'Authorization': 'Bearer {{ var.value.api_token }}'},\n"
            "    response_filter=lambda r: r.json(),\n"
            ")\n\n"
            "# Wait for API health:\n"
            "HttpSensor(\n"
            "    task_id='wait_for_api',\n"
            "    http_conn_id='my_api',\n"
            "    endpoint='health',\n"
            "    response_check=lambda r: r.status_code == 200,\n"
            "    mode='reschedule',\n"
            "    poke_interval=30,\n"
            "    timeout=300,\n"
            ")"
        )

    @task
    def explain_notifications():
        """Notification patterns."""
        print(
            "NOTIFICATION PATTERNS\n"
            "=====================\n\n"
            "# Slack notification:\n"
            "SlackWebhookOperator(\n"
            "    task_id='notify_slack',\n"
            "    slack_webhook_conn_id='slack_webhook',\n"
            "    message='Pipeline {{ dag.dag_id }} completed for {{ ds }}',\n"
            ")\n\n"
            "# Email (requires SMTP connection):\n"
            "EmailOperator(\n"
            "    task_id='send_email',\n"
            "    to='team@company.com',\n"
            "    subject='DAG {{ dag.dag_id }} - {{ ds }}',\n"
            "    html_content='<h3>Pipeline completed</h3>',\n"
            ")\n\n"
            "# Better: Use callbacks in default_args for automatic notifications"
        )

    call_api() >> explain_http_operators() >> explain_notifications()


api_providers()

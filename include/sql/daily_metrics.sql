-- Daily metrics aggregation query.
-- Referenced by ch07_03 Jinja templating example.
-- Template variables ({{ ds }}, {{ params.table_name }}) are rendered by Airflow.
SELECT
    date,
    COUNT(*)        AS order_count,
    SUM(amount)     AS total_amount,
    AVG(amount)     AS avg_amount,
    MIN(amount)     AS min_amount,
    MAX(amount)     AS max_amount
FROM {{ params.table_name }}
WHERE date = '{{ ds }}'
GROUP BY date;

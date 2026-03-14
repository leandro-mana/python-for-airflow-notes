-- Idempotent upsert for the orders table.
-- Re-running with the same data produces the same result.
-- Referenced by ch08_01_etl_csv_to_db DAG.
INSERT INTO orders (id, name, amount, date)
VALUES (%(id)s, %(name)s, %(amount)s, %(date)s)
ON CONFLICT (id) DO UPDATE SET
    name       = EXCLUDED.name,
    amount     = EXCLUDED.amount,
    date       = EXCLUDED.date,
    updated_at = CURRENT_TIMESTAMP;

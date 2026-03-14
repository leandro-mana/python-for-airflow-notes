-- Create the orders table for chapter 08 ETL examples.
-- Used by ch08_01_etl_csv_to_db to demonstrate idempotent upserts.
CREATE TABLE IF NOT EXISTS orders (
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    date        DATE NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for date-partitioned queries (ch07_03 templating example)
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders (date);

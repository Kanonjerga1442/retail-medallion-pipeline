CREATE TABLE IF NOT EXISTS temp.retail_batch (
    invoice_no_raw TEXT,
    stock_code_raw TEXT,
    description_raw TEXT,
    quantity_raw TEXT,
    invoice_date_raw TEXT,
    unit_price_raw TEXT,
    customer_id_raw TEXT,
    country_raw TEXT,

    invoice_timestamp TIMESTAMP,

    batch_date DATE,

    row_hash TEXT,
    source_file TEXT,

    converted_at TIMESTAMP,
    bronze_loaded_at TIMESTAMP,

    process_date DATE,
    pipeline_name VARCHAR(100),

    run_start_date DATE,
    run_end_date DATE,

    temp_loaded_at TIMESTAMP
);

ALTER TABLE temp.retail_batch
ADD COLUMN IF NOT EXISTS run_start_date DATE;

ALTER TABLE temp.retail_batch
ADD COLUMN IF NOT EXISTS run_end_date DATE;

CREATE INDEX IF NOT EXISTS idx_temp_retail_batch_row_hash
ON temp.retail_batch(row_hash);

CREATE INDEX IF NOT EXISTS idx_temp_retail_batch_batch_date
ON temp.retail_batch(batch_date);

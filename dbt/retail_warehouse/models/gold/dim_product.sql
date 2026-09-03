{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='product_key',
    indexes=[
        {'columns': ['product_key'], 'unique': true},
        {'columns': ['stock_code'], 'unique': true}
    ]
) }}

WITH ranked_products AS (
    SELECT
        stock_code,
        description,
        invoice_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY stock_code
            ORDER BY invoice_timestamp DESC, silver_loaded_at DESC, row_hash DESC
        ) AS row_number
    FROM {{ ref('retail_transactions') }}
    WHERE batch_date BETWEEN '{{ var("start_date") }}'::date
                         AND '{{ var("end_date") }}'::date
)

SELECT
    MD5(stock_code) AS product_key,
    stock_code,
    description AS product_name,
    CURRENT_TIMESTAMP AS gold_loaded_at
FROM ranked_products
WHERE row_number = 1

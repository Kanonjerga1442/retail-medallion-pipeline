{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='sales_key',
    pre_hook="{{ delete_selected_range(this, 'sales_date') }}",
    indexes=[
        {'columns': ['sales_key'], 'unique': true},
        {'columns': ['sales_date']},
        {'columns': ['date_key']},
        {'columns': ['product_key']},
        {'columns': ['country_key']}
    ]
) }}

SELECT
    transaction.row_hash AS sales_key,
    TO_CHAR(transaction.batch_date, 'YYYYMMDD')::integer AS date_key,
    MD5(transaction.stock_code) AS product_key,
    MD5(transaction.country) AS country_key,
    transaction.batch_date AS sales_date,
    transaction.invoice_no,
    transaction.customer_id,
    transaction.customer_type,
    transaction.invoice_timestamp,
    transaction.transaction_type,
    transaction.quantity,
    transaction.unit_price,
    transaction.line_amount AS net_amount,
    CASE WHEN transaction.transaction_type = 'SALE'
         THEN transaction.line_amount ELSE 0 END::numeric(18,4) AS sales_amount,
    CASE WHEN transaction.transaction_type IN ('RETURN', 'CANCELLATION')
         THEN transaction.line_amount ELSE 0 END::numeric(18,4) AS adjustment_amount,
    transaction.is_cancelled,
    transaction.is_return,
    transaction.source_file,
    CURRENT_TIMESTAMP AS gold_loaded_at
FROM {{ ref('retail_transactions') }} AS transaction
INNER JOIN {{ ref('dim_date') }} AS date_dimension
    ON date_dimension.date_key = TO_CHAR(transaction.batch_date, 'YYYYMMDD')::integer
INNER JOIN {{ ref('dim_product') }} AS product_dimension
    ON product_dimension.product_key = MD5(transaction.stock_code)
INNER JOIN {{ ref('dim_country') }} AS country_dimension
    ON country_dimension.country_key = MD5(transaction.country)
WHERE transaction.batch_date BETWEEN '{{ var("start_date") }}'::date
                                 AND '{{ var("end_date") }}'::date

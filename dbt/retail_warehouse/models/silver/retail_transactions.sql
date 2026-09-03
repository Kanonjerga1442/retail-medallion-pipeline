{% set delete_sql %}

    {% if is_incremental() %}

        DELETE FROM {{ this }}
        WHERE batch_date BETWEEN
              CAST('{{ var("start_date") }}' AS DATE)
          AND CAST('{{ var("end_date") }}' AS DATE)

    {% else %}

        SELECT 1

    {% endif %}

{% endset %}


{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="{{ delete_selected_range(this, 'batch_date') }}"
) }}


SELECT
    row_hash,
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_timestamp,
    batch_date,
    unit_price,
    customer_id,
    customer_type,
    country,
    transaction_type,
    is_cancelled,
    is_return,
    line_amount,
    source_file,
    converted_at,
    bronze_loaded_at,
    run_start_date,
    run_end_date,
    CURRENT_TIMESTAMP AS silver_loaded_at

FROM {{ ref('int_retail_classified') }}

WHERE reject_reason IS NULL
  AND duplicate_rank = 1
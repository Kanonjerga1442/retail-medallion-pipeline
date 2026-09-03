{% set delete_sql %}
    {% if is_incremental() %}
        DELETE FROM {{ this }}
        WHERE batch_date BETWEEN
              '{{ var("start_date") }}'::date
          AND '{{ var("end_date") }}'::date
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

    invoice_no_raw,
    stock_code_raw,
    description_raw,
    quantity_raw,
    invoice_date_raw,
    unit_price_raw,
    customer_id_raw,
    country_raw,

    batch_date,

    reject_reason,

    source_file,

    run_start_date,
    run_end_date,

    CURRENT_TIMESTAMP AS rejected_at

FROM {{ ref('int_retail_classified') }}

WHERE reject_reason IS NOT NULL

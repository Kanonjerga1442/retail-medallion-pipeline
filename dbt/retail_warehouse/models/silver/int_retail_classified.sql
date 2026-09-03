{{ config(materialized='ephemeral') }}

WITH source_data AS (

    SELECT *
    FROM {{ source('temp', 'retail_batch') }}

    -- Safety guard:
    -- dù TEMP đáng lẽ chỉ chứa batch hiện tại,
    -- vẫn chỉ lấy đúng range user yêu cầu.
    WHERE batch_date BETWEEN
          '{{ var("start_date") }}'::date
      AND '{{ var("end_date") }}'::date

),


typed AS (

    SELECT

        *,

        NULLIF(BTRIM(invoice_no_raw), '') AS invoice_no,

        NULLIF(BTRIM(stock_code_raw), '') AS stock_code,

        COALESCE(
            NULLIF(BTRIM(description_raw), ''),
            'UNKNOWN_PRODUCT'
        ) AS description,

        CASE
            WHEN BTRIM(quantity_raw)
                 ~ '^[+-]?[0-9]+$'
            THEN BTRIM(quantity_raw)::bigint
            ELSE NULL
        END AS quantity,

        CASE
            WHEN BTRIM(unit_price_raw)
                 ~ '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
            THEN BTRIM(unit_price_raw)::numeric(18,4)
            ELSE NULL
        END AS unit_price,

        NULLIF(
            BTRIM(customer_id_raw),
            ''
        ) AS customer_id,

        COALESCE(
            NULLIF(BTRIM(country_raw), ''),
            'UNKNOWN'
        ) AS country

    FROM source_data

),


quality_rules AS (

    SELECT

        *,

        NULLIF(

            CONCAT_WS(

                ';',

                CASE
                    WHEN row_hash IS NULL
                    THEN 'MISSING_ROW_HASH'
                END,

                CASE
                    WHEN invoice_no IS NULL
                    THEN 'MISSING_INVOICE_NO'
                END,

                CASE
                    WHEN stock_code IS NULL
                    THEN 'MISSING_STOCK_CODE'
                END,

                CASE
                    WHEN invoice_timestamp IS NULL
                    THEN 'INVALID_INVOICE_DATE'
                END,

                CASE
                    WHEN quantity IS NULL
                    THEN 'INVALID_QUANTITY'
                END,

                CASE
                    WHEN quantity = 0
                    THEN 'ZERO_QUANTITY'
                END,

                CASE
                    WHEN unit_price IS NULL
                    THEN 'INVALID_UNIT_PRICE'
                END,

                CASE
                    WHEN unit_price < 0
                    THEN 'NEGATIVE_UNIT_PRICE'
                END

            ),

            ''

        ) AS reject_reason

    FROM typed

),


ranked AS (

    SELECT

        *,

        ROW_NUMBER() OVER (

            PARTITION BY row_hash

            ORDER BY
                temp_loaded_at,
                source_file,
                invoice_no_raw,
                stock_code_raw

        ) AS duplicate_rank

    FROM quality_rules

)


SELECT

    *,

    CASE
        WHEN customer_id IS NULL
        THEN 'GUEST'
        ELSE 'REGISTERED'
    END AS customer_type,

    CASE
        WHEN UPPER(invoice_no) LIKE 'C%'
        THEN 'CANCELLATION'

        WHEN quantity < 0
        THEN 'RETURN'

        ELSE 'SALE'
    END AS transaction_type,

    CASE
        WHEN UPPER(invoice_no) LIKE 'C%'
        THEN TRUE
        ELSE FALSE
    END AS is_cancelled,

    CASE
        WHEN quantity < 0
        THEN TRUE
        ELSE FALSE
    END AS is_return,

    (
        quantity * unit_price
    )::numeric(18,4) AS line_amount

FROM ranked

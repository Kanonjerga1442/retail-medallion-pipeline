{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date_key',
    indexes=[
        {'columns': ['date_key'], 'unique': true},
        {'columns': ['full_date'], 'unique': true}
    ]
) }}

WITH dates AS (
    SELECT DISTINCT batch_date AS full_date
    FROM {{ ref('retail_transactions') }}
    WHERE batch_date BETWEEN '{{ var("start_date") }}'::date
                         AND '{{ var("end_date") }}'::date
)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::integer AS date_key,
    full_date,
    EXTRACT(DAY FROM full_date)::smallint AS day_of_month,
    EXTRACT(ISODOW FROM full_date)::smallint AS day_of_week,
    TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
    EXTRACT(WEEK FROM full_date)::smallint AS week_of_year,
    EXTRACT(MONTH FROM full_date)::smallint AS month_number,
    TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM full_date)::smallint AS quarter_number,
    EXTRACT(YEAR FROM full_date)::smallint AS year_number,
    (EXTRACT(ISODOW FROM full_date) IN (6, 7)) AS is_weekend,
    CURRENT_TIMESTAMP AS gold_loaded_at
FROM dates

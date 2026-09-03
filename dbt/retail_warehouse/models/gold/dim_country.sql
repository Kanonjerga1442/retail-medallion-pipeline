{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='country_key',
    indexes=[
        {'columns': ['country_key'], 'unique': true},
        {'columns': ['country_name'], 'unique': true}
    ]
) }}

SELECT
    MD5(country) AS country_key,
    country AS country_name,
    CURRENT_TIMESTAMP AS gold_loaded_at
FROM {{ ref('retail_transactions') }}
WHERE batch_date BETWEEN '{{ var("start_date") }}'::date
                     AND '{{ var("end_date") }}'::date
GROUP BY country

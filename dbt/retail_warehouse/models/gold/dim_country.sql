{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='country_key',
    pre_hook="{{ delete_orphan_dimension(this, 'country_name', ref('retail_transactions'), 'country') }}",
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
GROUP BY country

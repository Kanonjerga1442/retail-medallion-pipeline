WITH source_hashes AS (

    SELECT
        batch_date,
        row_hash,
        COUNT(*) AS source_count

    FROM {{ source('temp', 'retail_batch') }}

    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

    GROUP BY
        batch_date,
        row_hash

),

silver_all AS (

    SELECT
        batch_date,
        row_hash
    FROM {{ ref('retail_transactions') }}

    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

    UNION ALL

    SELECT
        batch_date,
        row_hash
    FROM {{ ref('retail_rejects') }}

    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

    UNION ALL

    SELECT
        batch_date,
        row_hash
    FROM {{ ref('retail_duplicates') }}

    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

),

target_hashes AS (

    SELECT
        batch_date,
        row_hash,
        COUNT(*) AS target_count

    FROM silver_all

    GROUP BY
        batch_date,
        row_hash

)

SELECT
    COALESCE(s.batch_date, t.batch_date) AS batch_date,
    COALESCE(s.row_hash, t.row_hash) AS row_hash,
    COALESCE(s.source_count, 0) AS source_count,
    COALESCE(t.target_count, 0) AS target_count,
    COALESCE(s.source_count, 0) - COALESCE(t.target_count, 0) AS difference

FROM source_hashes s

FULL OUTER JOIN target_hashes t
    ON s.batch_date = t.batch_date
   AND s.row_hash = t.row_hash

WHERE COALESCE(s.source_count, 0)
   <> COALESCE(t.target_count, 0)
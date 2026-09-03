WITH source_counts AS (

    SELECT
        batch_date,
        COUNT(*) AS source_count
    FROM {{ source('temp', 'retail_batch') }}
    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)
    GROUP BY batch_date

),

silver_all AS (

    SELECT batch_date
    FROM {{ ref('retail_transactions') }}
    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

    UNION ALL

    SELECT batch_date
    FROM {{ ref('retail_rejects') }}
    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

    UNION ALL

    SELECT batch_date
    FROM {{ ref('retail_duplicates') }}
    WHERE batch_date BETWEEN
          CAST('{{ var("start_date") }}' AS DATE)
      AND CAST('{{ var("end_date") }}' AS DATE)

),

target_counts AS (

    SELECT
        batch_date,
        COUNT(*) AS target_count
    FROM silver_all
    GROUP BY batch_date

)

SELECT
    COALESCE(s.batch_date, t.batch_date) AS batch_date,
    COALESCE(s.source_count, 0) AS source_count,
    COALESCE(t.target_count, 0) AS target_count,
    COALESCE(s.source_count, 0) - COALESCE(t.target_count, 0) AS difference

FROM source_counts s

FULL OUTER JOIN target_counts t
    ON s.batch_date = t.batch_date

WHERE COALESCE(s.source_count, 0)
   <> COALESCE(t.target_count, 0)
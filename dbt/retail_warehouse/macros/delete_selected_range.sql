{% macro delete_selected_range(relation, date_column) %}

    {% if is_incremental() %}

        DELETE FROM {{ relation }}
        WHERE {{ date_column }}
              BETWEEN CAST('{{ var("start_date") }}' AS DATE)
                  AND CAST('{{ var("end_date") }}' AS DATE)

    {% else %}

        SELECT 1

    {% endif %}

{% endmacro %}
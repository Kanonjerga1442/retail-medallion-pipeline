{% macro delete_orphan_dimension(relation, dimension_column, source_relation, source_column) %}

    {% if is_incremental() %}

        DELETE FROM {{ relation }} AS dimension
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ source_relation }} AS source
            WHERE source.{{ source_column }} = dimension.{{ dimension_column }}
        )

    {% else %}

        SELECT 1

    {% endif %}

{% endmacro %}

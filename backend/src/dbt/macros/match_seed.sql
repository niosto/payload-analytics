{% macro match_seed(column, seed_name, raw_col, clean_col) %}
    COALESCE(
        (   
            SELECT LOWER(TRIM(s.{{ clean_col }}))
            FROM {{ ref(seed_name) }} AS s 
            WHERE LOWER(TRIM(s.{{ raw_col }})) = LOWER(TRIM({{ column }}))
            LIMIT 1   
        ),
        {{ column }}
    )
{% endmacro %}
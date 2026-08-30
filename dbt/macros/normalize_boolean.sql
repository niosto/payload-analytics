{% macro normalize_boolean(column) %}
    CASE 
        WHEN LOWER(TRIM({{ column }})) in ('true', '1', 'yes', 'y', 'si') THEN TRUE
        WHEN LOWER(TRIM({{ column }})) in ('false', '0', 'no', 'n') THEN FALSE
        ELSE NULL
    END
{% endmacro %}
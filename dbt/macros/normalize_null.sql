{% macro normalize_null(column) %}
    CASE
        WHEN LOWER(TRIM({{ column }})) IN ('na','none','null', 'n/a', '', ' ', '-')
        THEN NULL
        ELSE TRIM({{ column }})
    END
{% endmacro %}
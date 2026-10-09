{% macro clean_amount(column) %}
   CASE
        WHEN {{ normalize_null(column) }} IS NULL THEN NULL
        ELSE CAST(
            REGEXP_REPLACE(
                REPLACE(TRIM({{ column }}), ',', '.'),
                '[$]|[a-zA-Z\s]+',
                '',
                'g'
            )
        AS float)
    END
{% endmacro %}

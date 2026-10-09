{% macro email_status(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN 'null'
        WHEN {{ clean_email(column) }} !~ '^[a-z0-9._\-]+@[a-z0-9.\-]+\.[a-z]{2,}$' THEN 'invalid'
        ELSE 'valid'
    END
{% endmacro %}
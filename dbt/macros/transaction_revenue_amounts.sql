{% macro transaction_revenue_amount(amount, type, status) %}
    CASE
        WHEN LOWER(TRIM({{ type }})) = 'fee' AND LOWER(TRIM({{ status }})) = 'completed'
        THEN {{ amount }}
        ELSE 0
    END
{% endmacro %}

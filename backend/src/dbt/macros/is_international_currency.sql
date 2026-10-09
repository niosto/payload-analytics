{% macro is_international_currency(txn_currency, country) %}
    UPPER(TRIM({{ txn_currency }})) != (
        SELECT UPPER(TRIM(sc.expected_currency))
        FROM {{ ref('seed_currency_map') }} sc
        WHERE UPPER(TRIM(sc.country)) = UPPER(TRIM({{ country }}))
        LIMIT 1
    )
{% endmacro %}

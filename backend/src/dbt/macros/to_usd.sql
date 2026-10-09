{% macro to_usd(amount_expr, currency_expr) %}
    CASE
        WHEN {{ amount_expr }} IS NULL THEN NULL
        WHEN {{ currency_expr }} IS NULL THEN NULL
        ELSE {{ amount_expr }} * (
            SELECT r.usd_per_unit
            FROM {{ ref('seed_fx_rates') }} AS r
            WHERE UPPER(TRIM(r.currency)) = UPPER(TRIM({{ currency_expr }}))
            LIMIT 1
        )
    END
{% endmacro %}
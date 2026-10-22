{% macro to_usd(amount_expr, currency_expr) %}
    {#- Rate from fx_rates_current: latest live tick, or seed_fx_rates when the
        stream has no recent tick. Unknown currency stays NULL, not 0. -#}
    CASE
        WHEN {{ amount_expr }} IS NULL THEN NULL
        WHEN {{ currency_expr }} IS NULL THEN NULL
        ELSE {{ amount_expr }} * (
            SELECT r.usd_per_unit
            FROM {{ ref('fx_rates_current') }} AS r
            WHERE r.currency = UPPER(TRIM({{ currency_expr }}))
            LIMIT 1
        )
    END
{% endmacro %}

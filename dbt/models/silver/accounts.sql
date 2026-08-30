{{config(materialized='table')}}

SELECT
    a.customer_id,
    a.account_id,
    LOWER(TRIM(a.account_type)) AS account_type,
    UPPER(TRIM(a.currency)) AS currency,
    CASE
        WHEN UPPER(TRIM(a.currency)) != (
            SELECT sc.expected_currency
            FROM {{ ref('seed_currency_map') }} sc
            JOIN {{ ref('customers') }} c ON UPPER(TRIM(c.country)) = UPPER(TRIM(sc.country))
            WHERE c.customer_id = a.customer_id
            LIMIT 1
        ) THEN true
        ELSE false
    END AS is_currency_mismatch,
    {{ clean_amount('a.balance') }} AS balance,
    {{ to_usd(clean_amount('a.balance'), 'a.currency') }} AS balance_usd,
    CAST(a.credit_limit AS FLOAT) AS credit_limit,
    {{ to_usd('CAST(a.credit_limit AS FLOAT)', 'a.currency') }} AS credit_limit_usd,
    CAST(a.interest_rate AS FLOAT) AS interest_rate,
    {{ format_date('a.opened_date') }} AS opened_date,
    (DATE_PART('year', AGE({{ format_date('a.opened_date') }}::date)) * 12
        + DATE_PART('month', AGE({{ format_date('a.opened_date') }}::date)))::integer
    AS account_age_months,
    {{ match_seed('a.status', 'seed_status', 'status_fuzzy', 'status_clean') }} AS status,
    UPPER(TRIM(a.branch_code)) AS branch_code,
    CAST(a.load_timestamp AS timestamp) AS load_timestamp

FROM {{ source('silver', 'stg_accounts') }} AS a

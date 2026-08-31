{{ config(materialized='table') }}

SELECT
    TRIM(l.customer_id) AS customer_id,
    TRIM(l.loan_id) AS loan_id,
    LOWER(TRIM(l.type)) AS type,
    UPPER(TRIM(l.currency)) AS currency,
    {{ clean_amount('l.principal') }} AS principal,
    {{ to_usd(clean_amount('l.principal'), 'l.currency') }} AS principal_usd,
    {{ clean_amount('l.outstanding_balance') }} AS outstanding_balance,
    {{ to_usd(clean_amount('l.outstanding_balance'), 'l.currency') }} AS outstanding_balance_usd,
    CAST(l.interest_rate AS float) AS interest_rate,
    CAST(l.term_months AS integer) AS term_months,
    {{ clean_amount('l.monthly_payment') }} AS monthly_payment,
    {{ to_usd(clean_amount('l.monthly_payment'), 'l.currency') }} AS monthly_payment_usd,
    {{ format_date('l.start_date') }} AS start_date,
    {{ format_date('l.end_date') }} AS end_date,
    LOWER(TRIM(l.status)) AS status,
    LOWER(TRIM(l.status)) IN ('delinquent', 'default') AS is_delinquent,
    CAST(l.days_past_due AS integer) AS days_past_due,
    CASE
        WHEN CAST(l.days_past_due AS integer) = 0 THEN 'current'
        WHEN CAST(l.days_past_due AS integer) BETWEEN 1 AND 30 THEN '1-30'
        WHEN CAST(l.days_past_due AS integer) BETWEEN 31 AND 60 THEN '31-60'
        WHEN CAST(l.days_past_due AS integer) BETWEEN 61 AND 90 THEN '61-90'
        WHEN CAST(l.days_past_due AS integer) > 90 THEN '90+'
        ELSE NULL
    END AS dpd_bucket,
    {{ normalize_null('l.collateral_type') }} AS collateral_type,
    CAST(l.load_timestamp AS timestamp) AS load_timestamp

FROM {{ source('silver', 'stg_loans') }} AS l
{{ config(materialized='table') }}

SELECT
    TRIM(c.customer_id) AS customer_id,
    CASE
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) BETWEEN 350 AND 800
        THEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer)
        ELSE NULL
    END AS credit_score,
    UPPER(TRIM((c.credit_info::jsonb)->>'currency')) AS currency,
    {{ clean_amount("(c.credit_info::jsonb)->>'utilization_pct'") }} AS utilization_pct,
    {{ clean_amount("(c.credit_info::jsonb)->>'total_limit'") }} AS total_limit,
    {{ to_usd(clean_amount("(c.credit_info::jsonb)->>'total_limit'"), "(c.credit_info::jsonb)->>'currency'") }} AS total_limit_usd,
    {{ clean_amount("(c.credit_info::jsonb)->>'total_used'") }} AS total_used,
    {{ to_usd(clean_amount("(c.credit_info::jsonb)->>'total_used'"), "(c.credit_info::jsonb)->>'currency'") }} AS total_used_usd,
    CAST((c.credit_info::jsonb)->>'num_credit_accounts' AS integer) AS num_credit_accounts,
    CAST((c.credit_info::jsonb)->>'oldest_account_age_months' AS integer) AS oldest_account_age_months,
    CAST((c.credit_info::jsonb)->>'late_payments_12m' AS integer) AS late_payments_12m,
    CAST((c.credit_info::jsonb)->>'inquiries_6m' AS integer) AS inquiries_6m,
    {{ normalize_boolean("(c.credit_info::jsonb)->>'bankruptcy_flag'") }} AS bankruptcy_flag,
    CAST(c.load_timestamp AS timestamp) AS load_timestamp,

    CASE
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) < 580 THEN 'poor'
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) BETWEEN 580 AND 669 THEN 'fair'
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) BETWEEN 670 AND 739 THEN 'good'
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) BETWEEN 740 AND 799 THEN 'very_good'
        WHEN CAST((c.credit_info::jsonb)->>'credit_score' AS integer) >= 800 THEN 'exceptional'
        ELSE NULL
    END AS credit_score_band,

    CASE
        WHEN {{ clean_amount("(c.credit_info::jsonb)->>'utilization_pct'") }} < 30 THEN 'low'
        WHEN {{ clean_amount("(c.credit_info::jsonb)->>'utilization_pct'") }} <= 60 THEN 'medium'
        WHEN {{ clean_amount("(c.credit_info::jsonb)->>'utilization_pct'") }} > 60 THEN 'high'
        ELSE NULL
    END AS utilization_tier

FROM {{ source('silver', 'stg_customers') }} AS c
WHERE c.credit_info IS NOT NULL

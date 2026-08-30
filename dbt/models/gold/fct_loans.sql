{{ config(materialized='table') }}

select
    loan_id,
    customer_id,
    TO_CHAR(start_date, 'YYYYMMDD')::integer as date_id,
    type as loan_type,
    currency,
    principal_usd as principal,
    outstanding_balance_usd as outstanding_balance,
    interest_rate,
    term_months,
    monthly_payment_usd as monthly_payment,
    (DATE_PART('year', AGE(CURRENT_DATE, start_date)) * 12 + DATE_PART('month', AGE(CURRENT_DATE, start_date)))::integer as loan_age_months,
    status,
    LOWER(TRIM(status)) in ('delinquent', 'default') as is_delinquent,
    days_past_due,
    dpd_bucket,
    case
        when principal_usd > 0
        then least(outstanding_balance_usd / principal_usd, 1.0)
        else 0
    end as pct_outstanding,
    case
        when outstanding_balance_usd > 0
        then outstanding_balance_usd * (interest_rate / 100.0) / 12
        else 0
    end as estimated_monthly_interest,
    collateral_type
from {{ ref('loans') }}

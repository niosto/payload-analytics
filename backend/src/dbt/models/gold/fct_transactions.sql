{{ config(
    materialized='table',
    indexes=[
      {'columns': ['transaction_id'], 'unique': True},
      {'columns': ['customer_id', 'transaction_date']},
      {'columns': ['date_id']},
      {'columns': ['account_id']}
    ]
) }}

select
    t.transaction_id,
    t.customer_id,
    t.account_id,
    TO_CHAR(t.transaction_date, 'YYYYMMDD')::integer as date_id,
    t.transaction_date,
    t.day_of_week,
    t.is_weekend,
    c.country as customer_country,
    t.amount_usd as amount,
    t.currency,
    t.type,
    t.category,
    t.merchant,
    t.channel,
    t.status,
    LOWER(t.status) = 'failed' as is_failed,
    {{ is_international_currency('t.currency', 'c.country') }} as is_international,
    coalesce({{ transaction_revenue_amount('t.amount_usd', 't.type', 't.status') }}, 0) as revenue_amount,
    coalesce({{ transaction_revenue_amount('t.amount_usd', 't.type', 't.status') }}, 0) > 0 as is_revenue,
    t.description
from {{ ref('transactions') }} t
left join {{ ref('accounts') }} a on t.account_id = a.account_id
left join {{ ref('customers') }} c on t.customer_id = c.customer_id

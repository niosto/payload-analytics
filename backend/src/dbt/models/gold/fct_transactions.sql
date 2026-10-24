{{ config(
    materialized='table',
    indexes=[
      {'columns': ['transaction_id'], 'unique': True},
      {'columns': ['customer_id', 'transaction_date']},
      {'columns': ['date_id']},
      {'columns': ['account_id']}
    ],
    post_hook=[
      {"sql": "create index if not exists fct_transactions_revenue_idx on {{ this }} (date_id) include (customer_country, revenue_amount) where is_revenue",
       "transaction": False},
      {"sql": "analyze {{ this }}", "transaction": False}
    ]
) }}

-- The revenue index is partial with INCLUDE, which the indexes config cannot
-- express, so it is a post_hook. It must run outside the transaction: inside
-- it, dbt's backup of the previous table still exists and still owns an index
-- with this name, "if not exists" skips, and dropping the backup then drops
-- the only copy. The index would exist on every other run.

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

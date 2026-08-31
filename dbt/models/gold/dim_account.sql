{{ config(
    materialized='table',
    indexes=[
      {'columns': ['account_id'], 'unique': True},
      {'columns': ['customer_id']}
    ]
) }}

select
    a.account_id,
    a.customer_id,
    a.account_type,
    (a.account_type = 'credit_card') as is_credit_product,
    a.currency,
    a.balance_usd as balance,
    a.credit_limit_usd as credit_limit,
    a.interest_rate,
    a.opened_date,
    a.account_age_months,
    a.status,
    a.branch_code,
    c.customer_segment,
    c.country
from {{ ref('accounts') }} a
left join {{ ref('customers') }} c on a.customer_id = c.customer_id

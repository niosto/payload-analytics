{{ config(materialized='view') }}

with revenue as (
    select
        customer_id,
        sum(revenue_amount) as total_revenue
    from {{ ref('fct_transactions') }}
    group by customer_id
),
accounts as (
    select
        customer_id,
        count(*) as account_count,
        sum(balance) as total_balance
    from {{ ref('dim_account') }}
    group by customer_id
),
loans as (
    select
        customer_id,
        count(*) as loan_count
    from {{ ref('fct_loans') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_segment,
    c.country,
    coalesce(r.total_revenue, 0) as total_revenue,
    coalesce(a.account_count, 0) as account_count,
    coalesce(a.total_balance, 0) as total_balance,
    coalesce(l.loan_count, 0) as loan_count,
    coalesce(a.account_count, 0) + coalesce(l.loan_count, 0) as product_count
from {{ ref('dim_customer') }} c
left join revenue r on c.customer_id = r.customer_id
left join accounts a on c.customer_id = a.customer_id
left join loans l on c.customer_id = l.customer_id

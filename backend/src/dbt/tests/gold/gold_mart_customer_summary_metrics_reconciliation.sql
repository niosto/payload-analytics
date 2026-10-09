-- Test: mart metrics must match recomputed sums/counts from gold facts and dims
-- Returns customers where any metric differs
with expected as (
    select
        c.customer_id,
        coalesce(r.total_revenue, 0) as total_revenue,
        coalesce(a.total_balance, 0) as total_balance,
        coalesce(a.account_count, 0) as account_count,
        coalesce(l.loan_count, 0) as loan_count
    from {{ ref('dim_customer') }} c
    left join (
        select customer_id, sum(revenue_amount) as total_revenue
        from {{ ref('fct_transactions') }}
        group by customer_id
    ) r on c.customer_id = r.customer_id
    left join (
        select customer_id, count(*) as account_count, sum(balance) as total_balance
        from {{ ref('dim_account') }}
        group by customer_id
    ) a on c.customer_id = a.customer_id
    left join (
        select customer_id, count(*) as loan_count
        from {{ ref('fct_loans') }}
        group by customer_id
    ) l on c.customer_id = l.customer_id
)
select
    m.customer_id,
    m.total_revenue,
    e.total_revenue,
    m.total_balance,
    e.total_balance,
    m.product_count,
    e.account_count + e.loan_count as expected_product_count
from {{ ref('mart_customer_summary') }} m
join expected e on m.customer_id = e.customer_id
where m.total_revenue != e.total_revenue
   or m.total_balance != e.total_balance
   or m.product_count != e.account_count + e.loan_count

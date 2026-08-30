-- Test: revenue_amount and is_revenue must match gold revenue definition
-- Revenue = amount_usd when type is fee and status is completed; otherwise 0
-- Returns rows that FAIL
select
    transaction_id,
    type,
    status,
    amount,
    revenue_amount,
    is_revenue
from {{ ref('fct_transactions') }}
where revenue_amount is distinct from (
    case
        when lower(trim(type)) = 'fee' and lower(trim(status)) = 'completed'
        then amount
        else 0
    end
)
or is_revenue is distinct from (
    case
        when lower(trim(type)) = 'fee' and lower(trim(status)) = 'completed'
        then amount > 0
        else false
    end
)

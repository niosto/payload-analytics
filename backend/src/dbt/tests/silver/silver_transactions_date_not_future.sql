-- Test: transaction_date must not be in the future
-- Returns rows that FAIL
select
    transaction_id,
    customer_id,
    transaction_date
from {{ ref('transactions') }}
where transaction_date::date > CURRENT_DATE

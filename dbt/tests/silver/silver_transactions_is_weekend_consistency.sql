-- Test: is_weekend must be consistent with transaction_date
-- Saturday (DOW=6) and Sunday (DOW=0) → is_weekend must be TRUE
-- Monday–Friday → is_weekend must be FALSE
-- Returns rows that FAIL
select
    transaction_id,
    transaction_date,
    is_weekend
from {{ ref('transactions') }}
where
    -- Flagged as weekend but falls on a weekday
    (is_weekend = true  and EXTRACT(DOW FROM transaction_date::date) NOT IN (0, 6))
    or
    -- Flagged as weekday but falls on a weekend
    (is_weekend = false and EXTRACT(DOW FROM transaction_date::date) IN (0, 6))

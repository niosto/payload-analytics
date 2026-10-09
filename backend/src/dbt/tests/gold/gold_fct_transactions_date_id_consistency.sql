-- Test: date_id must match transaction_date as YYYYMMDD integer
-- Returns rows that FAIL
select
    transaction_id,
    transaction_date,
    date_id
from {{ ref('fct_transactions') }}
where date_id is distinct from to_char(transaction_date, 'YYYYMMDD')::integer

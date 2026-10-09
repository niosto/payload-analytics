-- Test: is_delinquent must be true when status is delinquent or default
-- Returns rows that FAIL
select
    loan_id,
    status,
    is_delinquent
from {{ ref('fct_loans') }}
where is_delinquent is distinct from (
    lower(trim(status)) in ('delinquent', 'default')
)

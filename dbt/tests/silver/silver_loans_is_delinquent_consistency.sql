-- Test: is_delinquent must be true when status is delinquent or default
-- Mirrors gold_fct_loans_is_delinquent_consistency so the two layers cannot drift
-- Returns rows that FAIL
select
    loan_id,
    status,
    is_delinquent
from {{ ref('loans') }}
where is_delinquent is distinct from (
    lower(trim(status)) in ('delinquent', 'default')
)

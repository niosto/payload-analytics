-- Test: amount (USD) must equal amount_native * fx_usd_per_unit.
-- Returns rows that FAIL
select
    transaction_id,
    amount,
    amount_native,
    fx_usd_per_unit
from {{ ref('fct_live_transactions') }}
where abs(amount - amount_native * fx_usd_per_unit) > 0.0001 * greatest(abs(amount), 1)

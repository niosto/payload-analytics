-- Test: every currency in seed_fx_rates must have a rate in fx_rates_current,
-- so switching to live rates never turns a converted amount into NULL.
-- Returns rows that FAIL
select s.currency
from {{ ref('seed_fx_rates') }} s
left join {{ ref('fx_rates_current') }} r
    on r.currency = upper(trim(s.currency))
where r.currency is null

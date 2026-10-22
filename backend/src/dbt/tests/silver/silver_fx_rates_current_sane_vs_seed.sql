-- Test: a live rate must stay within fx_max_ratio_vs_seed of the seed rate.
-- Catches a units_per_usd / usd_per_unit inversion or a bad provider value
-- before it rescales every USD amount in Silver and Gold.
-- Returns rows that FAIL
select
    currency,
    usd_per_unit,
    seed_usd_per_unit
from {{ ref('fx_rates_current') }}
where rate_source = 'stream'
  and seed_usd_per_unit is not null
  and (
      usd_per_unit > seed_usd_per_unit * {{ var('fx_max_ratio_vs_seed', 10) }}
      or usd_per_unit < seed_usd_per_unit / {{ var('fx_max_ratio_vs_seed', 10) }}
  )

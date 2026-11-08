{{ config(
    materialized='table',
    indexes=[
      {'columns': ['currency'], 'unique': True}
    ]
) }}

-- Reporting dimension for the rate in effect per currency. It exposes the
-- same snapshot to_usd uses, so the dashboard can show the current rate, its
-- source (stream or seed), and the age of the tick without reading Silver.

select
    currency,
    usd_per_unit,
    rate_source,
    rate_event_time,
    provider_updated_at,
    seed_usd_per_unit
from {{ ref('fx_rates_current') }}

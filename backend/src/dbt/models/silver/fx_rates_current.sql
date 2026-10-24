{{ config(materialized='table') }}

-- One row per currency: the rate to_usd applies.
-- The latest live tick from the stream wins when it is newer than
-- fx_live_max_age_hours; otherwise seed_fx_rates is the fallback. This keeps
-- the seed as the interface contract: models call to_usd and never see where
-- the rate came from. Materialized as a table on purpose: to_usd looks it up
-- once per row, so it must be a few rows, not a scan of the tick history.
--
-- The latest tick is read with one LATERAL ... LIMIT 1 per seed currency, so
-- each lookup is a single probe of stream_fx_rates_norm_currency_event_time_idx
-- (migration 20261023000000_stream_indexes). A DISTINCT ON over the whole 48 h
-- window reads every tick in it and never uses that index: 1.5 s against
-- 0.19 ms on 2M ticks (docs/optimizations/README.md, optimization 1). Keep the
-- UPPER(TRIM(currency)) expression and the usd_per_unit > 0 filter as they are;
-- they are what the index matches. The seed drives the list of currencies, so
-- a currency must be in seed_fx_rates to get a live rate.

WITH seed AS (
    SELECT
        UPPER(TRIM(currency)) AS currency,
        CAST(usd_per_unit AS DOUBLE PRECISION) AS usd_per_unit
    FROM {{ ref('seed_fx_rates') }}
),

live AS (
    SELECT
        s.currency,
        t.usd_per_unit,
        t.event_time,
        t.provider_updated_at
    FROM seed AS s
    CROSS JOIN LATERAL (
        SELECT f.usd_per_unit, f.event_time, f.provider_updated_at
        FROM {{ source('silver', 'stream_fx_rates') }} AS f
        WHERE UPPER(TRIM(f.currency)) = s.currency
          AND f.usd_per_unit > 0
          AND f.event_time >= NOW() - INTERVAL '{{ var("fx_live_max_age_hours", 48) }} hours'
        ORDER BY UPPER(TRIM(f.currency)), f.event_time DESC
        LIMIT 1
    ) AS t
)

SELECT
    s.currency,
    COALESCE(l.usd_per_unit, s.usd_per_unit) AS usd_per_unit,
    CASE WHEN l.currency IS NOT NULL THEN 'stream' ELSE 'seed' END AS rate_source,
    l.event_time AS rate_event_time,
    l.provider_updated_at,
    s.usd_per_unit AS seed_usd_per_unit
FROM seed AS s
LEFT JOIN live AS l ON l.currency = s.currency

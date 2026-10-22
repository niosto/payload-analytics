{{ config(materialized='table') }}

-- One row per currency: the rate to_usd applies.
-- The latest live tick from the stream wins when it is newer than
-- fx_live_max_age_hours; otherwise seed_fx_rates is the fallback. This keeps
-- the seed as the interface contract: models call to_usd and never see where
-- the rate came from. Materialized as a table on purpose: to_usd looks it up
-- once per row, so it must be a few rows, not a scan of the tick history.

WITH live AS (
    SELECT DISTINCT ON (UPPER(TRIM(currency)))
        UPPER(TRIM(currency)) AS currency,
        usd_per_unit,
        event_time,
        provider_updated_at
    FROM {{ source('silver', 'stream_fx_rates') }}
    WHERE usd_per_unit > 0
      AND event_time >= NOW() - INTERVAL '{{ var("fx_live_max_age_hours", 48) }} hours'
    ORDER BY UPPER(TRIM(currency)), event_time DESC
),

seed AS (
    SELECT
        UPPER(TRIM(currency)) AS currency,
        CAST(usd_per_unit AS DOUBLE PRECISION) AS usd_per_unit
    FROM {{ ref('seed_fx_rates') }}
)

SELECT
    COALESCE(l.currency, s.currency) AS currency,
    COALESCE(l.usd_per_unit, s.usd_per_unit) AS usd_per_unit,
    CASE WHEN l.currency IS NOT NULL THEN 'stream' ELSE 'seed' END AS rate_source,
    l.event_time AS rate_event_time,
    l.provider_updated_at,
    s.usd_per_unit AS seed_usd_per_unit
FROM live AS l
FULL OUTER JOIN seed AS s ON l.currency = s.currency

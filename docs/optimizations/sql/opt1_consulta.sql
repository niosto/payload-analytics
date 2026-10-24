SELECT DISTINCT ON (UPPER(TRIM(currency)))
       UPPER(TRIM(currency)) AS currency, usd_per_unit, event_time, provider_updated_at
FROM perf.stream_fx_rates
WHERE usd_per_unit > 0 AND event_time >= NOW() - INTERVAL '48 hours'
ORDER BY UPPER(TRIM(currency)), event_time DESC;

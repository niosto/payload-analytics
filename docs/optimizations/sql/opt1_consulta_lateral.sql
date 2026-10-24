SELECT c.currency, l.usd_per_unit, l.event_time, l.provider_updated_at
FROM (VALUES ('ARS'),('BRL'),('CLP'),('COP'),('EUR'),('MXN'),('PEN'),('USD'),('UYU')) AS c(currency)
CROSS JOIN LATERAL (
    SELECT usd_per_unit, event_time, provider_updated_at
    FROM perf.stream_fx_rates s
    WHERE UPPER(TRIM(s.currency)) = c.currency
      AND s.usd_per_unit > 0
      AND s.event_time >= NOW() - INTERVAL '48 hours'
    ORDER BY UPPER(TRIM(s.currency)), s.event_time DESC
    LIMIT 1
) AS l;

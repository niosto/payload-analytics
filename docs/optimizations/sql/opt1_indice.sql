CREATE INDEX fx_norm_currency_event_time_idx ON perf.stream_fx_rates
  (UPPER(TRIM(currency)), event_time DESC)
  INCLUDE (usd_per_unit, provider_updated_at) WHERE usd_per_unit > 0;

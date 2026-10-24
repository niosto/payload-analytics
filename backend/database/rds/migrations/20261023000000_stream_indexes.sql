-- depends: 20261015000000_silver_streaming

-- Opt 1: matches the UPPER(TRIM(currency)) expression used by fx_rates_current
CREATE INDEX IF NOT EXISTS stream_fx_rates_norm_currency_event_time_idx
    ON silver.stream_fx_rates (UPPER(TRIM(currency)), event_time DESC)
    INCLUDE (usd_per_unit, provider_updated_at)
    WHERE usd_per_unit > 0;

-- Opt 2: incremental filter of fct_live_transactions
CREATE INDEX IF NOT EXISTS stream_transactions_processed_at_idx
    ON silver.stream_transactions (processed_at);
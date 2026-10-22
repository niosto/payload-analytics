-- depends: 20260930000000_silver_staging

-- Written by the local Spark Structured Streaming job (streaming/src/spark).
-- Business fields stay VARCHAR, the same contract as silver.stg_*: dbt casts
-- and cleans them. Event metadata is typed so latency can be measured in SQL:
--   kafka_ts     - when Redpanda accepted the event
--   processed_at - when the streaming job wrote the row

CREATE TABLE silver.stream_fx_rates (
    event_id            VARCHAR PRIMARY KEY,
    currency            VARCHAR NOT NULL,
    base                VARCHAR,
    units_per_usd       DOUBLE PRECISION,
    usd_per_unit        DOUBLE PRECISION,
    provider            VARCHAR,
    provider_updated_at TIMESTAMPTZ,
    event_time          TIMESTAMPTZ NOT NULL,
    kafka_ts            TIMESTAMPTZ,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX stream_fx_rates_currency_event_time_idx
    ON silver.stream_fx_rates (currency, event_time DESC);

CREATE TABLE silver.stream_transactions (
    transaction_id          VARCHAR PRIMARY KEY,
    customer_id             VARCHAR,
    account_id              VARCHAR,
    date                    VARCHAR,
    amount                  VARCHAR,
    currency                VARCHAR,
    type                    VARCHAR,
    category                VARCHAR,
    merchant                VARCHAR,
    channel                 VARCHAR,
    status                  VARCHAR,
    description             VARCHAR,
    event_id                VARCHAR,
    source                  VARCHAR,
    event_time              TIMESTAMPTZ NOT NULL,
    kafka_ts                TIMESTAMPTZ,
    -- FX tick joined in the stream: the latest rate for the currency in the
    -- minute before event_time. NULL when no tick was in that window.
    fx_usd_per_unit         DOUBLE PRECISION,
    fx_event_time           TIMESTAMPTZ,
    fx_provider_updated_at  TIMESTAMPTZ,
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX stream_transactions_event_time_idx
    ON silver.stream_transactions (event_time);

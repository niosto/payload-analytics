{{ config(materialized='view') }}

-- Live transactions from the stream, cleaned with the same macros as the batch
-- transactions model. A view: fct_live_transactions reads it incrementally, so
-- only the new rows are cleaned on each run.
--
-- USD conversion: the rate joined in the stream at event time (the FX tick in
-- the minute before the transaction). When the stream had no tick in that
-- window, to_usd falls back to fx_rates_current.

SELECT
    TRIM(t.customer_id) AS customer_id,
    TRIM(t.transaction_id) AS transaction_id,
    TRIM(t.account_id) AS account_id,
    {{ format_date('t.date') }} AS transaction_date,
    EXTRACT(DOW FROM {{ format_date('t.date') }}::date) IN (0, 6) AS is_weekend,
    TO_CHAR({{ format_date('t.date') }}::date, 'Day') AS day_of_week,
    {{ clean_amount('t.amount') }} AS amount,
    CASE
        WHEN t.fx_usd_per_unit IS NOT NULL THEN {{ clean_amount('t.amount') }} * t.fx_usd_per_unit
        ELSE {{ to_usd(clean_amount('t.amount'), 't.currency') }}
    END AS amount_usd,
    COALESCE(
        t.fx_usd_per_unit,
        (SELECT r.usd_per_unit FROM {{ ref('fx_rates_current') }} AS r
         WHERE r.currency = UPPER(TRIM(t.currency)) LIMIT 1)
    ) AS fx_usd_per_unit,
    CASE WHEN t.fx_usd_per_unit IS NOT NULL THEN 'event_window' ELSE 'fx_rates_current' END AS fx_rate_source,
    t.fx_event_time,
    t.fx_provider_updated_at,
    UPPER(TRIM(t.currency)) AS currency,
    {{ match_seed('t.type', 'seed_transaction_type', 'transaction_type_fuzzy', 'transaction_type_clean') }} AS type,
    {{ normalize_null('t.category') }} AS category,
    {{ normalize_null('t.merchant') }} AS merchant,
    LOWER(TRIM(t.channel)) AS channel,
    LOWER(TRIM(t.status)) AS status,
    {{ normalize_null('t.description') }} AS description,
    t.event_time,
    t.kafka_ts,
    t.processed_at,
    EXTRACT(EPOCH FROM t.processed_at - t.event_time) AS latency_seconds

FROM {{ source('silver', 'stream_transactions') }} AS t

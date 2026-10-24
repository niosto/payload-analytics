{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['transaction_id'], 'unique': True},
      {'columns': ['event_time']},
      {'columns': ['processed_at']},
      {'columns': ['customer_id', 'transaction_date']},
      {'columns': ['date_id']},
      {'columns': ['account_id']}
    ]
) }}

-- Live transaction fact, same grain and columns as fct_transactions plus the
-- stream metadata. Incremental by processed_at with a lookback, so rows whose
-- FX columns the stream updated after the last run are rebuilt too.

select
    t.transaction_id,
    t.customer_id,
    t.account_id,
    TO_CHAR(t.transaction_date, 'YYYYMMDD')::integer as date_id,
    t.transaction_date,
    t.day_of_week,
    t.is_weekend,
    c.country as customer_country,
    t.amount_usd as amount,
    t.amount as amount_native,
    t.currency,
    t.fx_usd_per_unit,
    t.fx_rate_source,
    t.fx_event_time,
    t.fx_provider_updated_at,
    t.type,
    t.category,
    t.merchant,
    t.channel,
    t.status,
    LOWER(t.status) = 'failed' as is_failed,
    {{ is_international_currency('t.currency', 'c.country') }} as is_international,
    coalesce({{ transaction_revenue_amount('t.amount_usd', 't.type', 't.status') }}, 0) as revenue_amount,
    coalesce({{ transaction_revenue_amount('t.amount_usd', 't.type', 't.status') }}, 0) > 0 as is_revenue,
    t.description,
    t.event_time,
    t.kafka_ts,
    t.processed_at,
    t.latency_seconds
from {{ ref('live_transactions') }} t
left join {{ ref('customers') }} c on t.customer_id = c.customer_id

{% if is_incremental() %}
where t.processed_at > (
    select coalesce(max(processed_at), '-infinity'::timestamptz) from {{ this }}
) - interval '{{ var("live_lookback_minutes", 10) }} minutes'
{% endif %}

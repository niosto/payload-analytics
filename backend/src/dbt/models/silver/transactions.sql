{{ config(materialized='table') }}

SELECT
    TRIM(t.customer_id) AS customer_id,
    TRIM(t.transaction_id) AS transaction_id,
    TRIM(t.account_id) AS account_id,
    {{ format_date('t.date') }} AS transaction_date,
    DATE_PART('year', {{ format_date('t.date') }}::date) AS transaction_year,
    DATE_PART('month', {{ format_date('t.date') }}::date)::integer AS transaction_month,
    DATE_PART('week', {{ format_date('t.date') }}::date)::integer AS transaction_week,
    EXTRACT(DOW FROM {{ format_date('t.date') }}::date) IN (0, 6) AS is_weekend,
    TO_CHAR({{ format_date('t.date') }}::date, 'Day') AS day_of_week,
    {{ clean_amount('t.amount') }} AS amount,
    {{ to_usd(clean_amount('t.amount'), 't.currency') }} AS amount_usd,
    UPPER(TRIM(t.currency)) AS currency,
    {{ match_seed('t.type', 'seed_transaction_type', 'transaction_type_fuzzy', 'transaction_type_clean') }} AS type,
    {{ normalize_null('t.category') }} AS category,
    {{ normalize_null('t.merchant') }} AS merchant,
    LOWER(TRIM(t.channel)) AS channel,
    LOWER(TRIM(t.status)) AS status,
    {{ normalize_null('t.description') }} AS description,
    CAST(t.load_timestamp AS timestamp) AS load_timestamp

FROM {{ source('silver', 'stg_transactions') }} AS t

CREATE SCHEMA IF NOT EXISTS perf;

-- Ticks FX: 2M filas, ordenadas por tiempo (como un stream real)
CREATE TABLE perf.stream_fx_rates (LIKE silver.stream_fx_rates INCLUDING DEFAULTS);
INSERT INTO perf.stream_fx_rates (event_id, currency, base, units_per_usd, usd_per_unit, provider, provider_updated_at, event_time, processed_at)
SELECT 'fx-'||g, (ARRAY['BRL','ARS','MXN','COP','EUR','USD','CLP','PEN','UYU'])[1 + g % 9], 'USD',
       1/(0.0001+random()), 0.0001+random(), 'sim',
       now() - interval '600000 seconds' + g * interval '0.3 seconds',
       now() - interval '600000 seconds' + g * interval '0.3 seconds',
       now() - interval '600000 seconds' + g * interval '0.3 seconds'
FROM generate_series(1, 2000000) g;
CREATE INDEX ON perf.stream_fx_rates (currency, event_time DESC);   -- el índice que hoy existe

-- Transacciones en vivo: 1M filas
CREATE TABLE perf.stream_transactions (LIKE silver.stream_transactions INCLUDING DEFAULTS);
INSERT INTO perf.stream_transactions (transaction_id, customer_id, account_id, date, amount, currency, type, status, event_id, source, event_time, processed_at)
SELECT 'LTX-'||g, 'C'||(g % 5000), 'A'||(g % 17000), to_char(now(),'YYYY-MM-DD'),
       (random()*500)::numeric(10,2)::text, 'COP', 'fee',
       (ARRAY['completed','pending','failed','reversed'])[1 + g % 4], 'ev-'||g, 'sim',
       now() - interval '600000 seconds' + g * interval '0.6 seconds',
       now() - interval '600000 seconds' + g * interval '0.6 seconds' + interval '3 seconds'
FROM generate_series(1, 1000000) g;
ALTER TABLE perf.stream_transactions ADD PRIMARY KEY (transaction_id);       -- existe hoy
CREATE INDEX st_event_time_btree ON perf.stream_transactions (event_time);   -- existe hoy

-- Hecho de transacciones: x20 (~1.8M filas), conserva la distribución real
CREATE TABLE perf.fct_transactions AS
SELECT f.* FROM gold.fct_transactions f CROSS JOIN generate_series(1,20);
CREATE INDEX ON perf.fct_transactions (date_id);                    -- existen hoy
CREATE INDEX ON perf.fct_transactions (customer_id, transaction_date);
CREATE INDEX ON perf.fct_transactions (account_id);

VACUUM (ANALYZE) perf.stream_fx_rates, perf.stream_transactions, perf.fct_transactions;

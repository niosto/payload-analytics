# Linaje de datos

Del dato crudo al reporte. El tramo antes de dbt (S3 → Glue y fuentes → Redpanda → Spark) es fijo; cada flecha entre tablas sale de las dependencias reales de dbt (`ref` y `source`, incluidas las que usan macros como `to_usd` → `fx_rates_current`), así que el diagrama cambia solo cuando cambian los modelos. Se regenera con `build_governance_docs.py`.

El linaje a nivel de columna y el SQL de cada modelo están en el catálogo de dbt: abre `catalog.html` en el navegador y usa el botón del grafo de linaje (abajo a la derecha).

```mermaid
flowchart LR
  json["JSON clientes<br/>S3 raw/ (Bronze)"] --> glue["Glue Spark<br/>aplana y deduplica"]
  binance["Binance WebSocket<br/>BRL ARS MXN COP EUR"] --> redpanda
  erapi["open.er-api<br/>USD CLP PEN UYU"] --> redpanda
  sim["Simulador de<br/>transacciones"] --> redpanda
  redpanda["Redpanda<br/>fx.rates · fintech.live_transactions"] --> spark["Spark Structured<br/>Streaming"]
  bi["Power BI"]
  subgraph stg["Silver staging (Spark batch)"]
    silver_stg_accounts["silver.stg_accounts"]
    silver_stg_customers["silver.stg_customers"]
    silver_stg_loans["silver.stg_loans"]
    silver_stg_transactions["silver.stg_transactions"]
  end
  subgraph stream["Silver streaming (Spark)"]
    silver_stream_fx_rates["silver.stream_fx_rates"]
    silver_stream_transactions["silver.stream_transactions"]
  end
  subgraph seed["Seeds"]
    seeds_seed_city["seeds.seed_city"]
    seeds_seed_currency_map["seeds.seed_currency_map"]
    seeds_seed_customer_segment["seeds.seed_customer_segment"]
    seeds_seed_fx_rates["seeds.seed_fx_rates"]
    seeds_seed_phone_codes["seeds.seed_phone_codes"]
    seeds_seed_status["seeds.seed_status"]
    seeds_seed_transaction_type["seeds.seed_transaction_type"]
  end
  subgraph silver["Silver (dbt)"]
    silver_accounts["silver.accounts"]
    silver_credit_info["silver.credit_info"]
    silver_customers["silver.customers"]
    silver_digital_engagement["silver.digital_engagement"]
    silver_fx_rates_current["silver.fx_rates_current"]
    silver_live_transactions["silver.live_transactions"]
    silver_loans["silver.loans"]
    silver_transactions["silver.transactions"]
  end
  subgraph gold["Gold (dbt)"]
    gold_dim_account["gold.dim_account"]
    gold_dim_customer["gold.dim_customer"]
    gold_dim_date["gold.dim_date"]
    gold_fct_live_transactions["gold.fct_live_transactions"]
    gold_fct_loans["gold.fct_loans"]
    gold_fct_transactions["gold.fct_transactions"]
    gold_mart_customer_summary["gold.mart_customer_summary"]
  end
  glue --> silver_stg_customers
  glue --> silver_stg_accounts
  glue --> silver_stg_transactions
  glue --> silver_stg_loans
  spark --> silver_stream_fx_rates
  spark --> silver_stream_transactions
  gold_dim_account --> gold_mart_customer_summary
  gold_dim_customer --> gold_mart_customer_summary
  gold_fct_loans --> gold_mart_customer_summary
  gold_fct_transactions --> gold_mart_customer_summary
  seeds_seed_city --> silver_customers
  seeds_seed_currency_map --> gold_fct_live_transactions
  seeds_seed_currency_map --> gold_fct_transactions
  seeds_seed_currency_map --> silver_accounts
  seeds_seed_customer_segment --> silver_customers
  seeds_seed_fx_rates --> silver_fx_rates_current
  seeds_seed_phone_codes --> silver_customers
  seeds_seed_status --> silver_accounts
  seeds_seed_status --> silver_customers
  seeds_seed_transaction_type --> silver_live_transactions
  seeds_seed_transaction_type --> silver_transactions
  silver_accounts --> gold_dim_account
  silver_accounts --> gold_fct_transactions
  silver_credit_info --> gold_dim_customer
  silver_customers --> gold_dim_account
  silver_customers --> gold_dim_customer
  silver_customers --> gold_fct_live_transactions
  silver_customers --> gold_fct_transactions
  silver_customers --> silver_accounts
  silver_digital_engagement --> gold_dim_customer
  silver_fx_rates_current --> silver_accounts
  silver_fx_rates_current --> silver_credit_info
  silver_fx_rates_current --> silver_live_transactions
  silver_fx_rates_current --> silver_loans
  silver_fx_rates_current --> silver_transactions
  silver_live_transactions --> gold_fct_live_transactions
  silver_loans --> gold_fct_loans
  silver_stg_accounts --> silver_accounts
  silver_stg_customers --> silver_credit_info
  silver_stg_customers --> silver_customers
  silver_stg_customers --> silver_digital_engagement
  silver_stg_loans --> silver_loans
  silver_stg_transactions --> silver_transactions
  silver_stream_fx_rates --> silver_fx_rates_current
  silver_stream_transactions --> silver_live_transactions
  silver_transactions --> gold_fct_transactions
  gold --> bi
```

## Cómo leerlo

- **Batch:** un JSON en `raw/` dispara Glue, que escribe `silver.stg_*`. dbt limpia esas tablas en Silver y construye la estrella en Gold.
- **Streaming:** Binance, la API diaria y el simulador publican en Redpanda. Spark cruza cada transacción con su tasa y escribe `silver.stream_*`.
- **Donde se juntan:** `fx_rates_current` toma la última tasa del stream (o el seed si no hay ticks recientes), y todos los montos en USD del batch pasan por ella vía `to_usd`.
- **Seeds:** tablas de referencia versionadas en el repo (ciudades, estados, tipos, prefijos, monedas, tasas de respaldo).

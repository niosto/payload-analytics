# Reglas de calidad implementadas

Hay **206 reglas** que corren como tests de dbt en cada ejecución del pipeline (`dbt test`, dentro de `run.sh` en Fargate). Si una falla, la tarea de Fargate falla, Step Functions mueve el archivo a `failed/` y SNS envía un correo. Ninguna regla se debilita para que una corrida pase.

## Por tipo

| Tipo de regla | Qué verifica | Cantidad |
| --- | --- | --- |
| `not_null` | La columna nunca está vacía | 90 |
| `is_non_negative` | El valor es mayor o igual a 0 (montos en USD) | 30 |
| `accepted_values` | El valor está en la lista permitida | 19 |
| `relationships` | La clave foránea existe en la dimensión | 17 |
| `unique` | No hay duplicados (clave primaria) | 14 |
| `accepted_range` | El valor está dentro de un rango | 11 |
| `is_valid_date_format` | La fecha quedó en formato válido | 6 |
| `is_positive` | El valor es mayor que 0 (montos nativos) | 5 |
| Singulares (reglas de negocio) | Consistencia entre columnas y capas | 14 |

## Reglas de negocio (tests singulares)

| Test | Capa | Regla |
| --- | --- | --- |
| `gold_fct_live_transactions_fx_window` | Gold | a rate joined in the stream must come from the window before the transaction: fx_event_time in [event_time - live_fx_join_seconds, event_time]. |
| `gold_fct_live_transactions_revenue_logic` | Gold | revenue_amount and is_revenue follow the same definition as fct_transactions: amount when type is fee and status is completed, else 0. |
| `gold_fct_live_transactions_usd_conversion` | Gold | amount (USD) must equal amount_native * fx_usd_per_unit. |
| `gold_fct_loans_is_delinquent_consistency` | Gold | is_delinquent must be true when status is delinquent or default |
| `gold_fct_transactions_date_id_consistency` | Gold | date_id must match transaction_date as YYYYMMDD integer |
| `gold_fct_transactions_revenue_logic` | Gold | revenue_amount and is_revenue must match gold revenue definition Revenue = amount_usd when type is fee and status is completed; otherwise 0 |
| `gold_mart_customer_summary_metrics_reconciliation` | Gold | mart metrics must match recomputed sums/counts from gold facts and dims Returns customers where any metric differs |
| `silver_customers_date_chronology` | Silver | registration_date and date_of_birth must be valid ISO dates (YYYY-MM-DD) The format_date macro already casts to date; this confirms no nulls crept in and that dates are not in the future for date_of_birth |
| `silver_digital_engagement_is_digital_consistency` | Silver | is_digital must match preferred_channel (mobile or web) |
| `silver_fx_rates_current_covers_seed` | Silver | every currency in seed_fx_rates must have a rate in fx_rates_current, so switching to live rates never turns a converted amount into NULL. |
| `silver_fx_rates_current_sane_vs_seed` | Silver | a live rate must stay within fx_max_ratio_vs_seed of the seed rate. Catches a units_per_usd / usd_per_unit inversion or a bad provider value before it rescales every USD amount in Silver and Gold. |
| `silver_loans_is_delinquent_consistency` | Silver | is_delinquent must be true when status is delinquent or default Mirrors gold_fct_loans_is_delinquent_consistency so the two layers cannot drift |
| `silver_transactions_date_not_future` | Silver | transaction_date must not be in the future |
| `silver_transactions_is_weekend_consistency` | Silver | is_weekend must be consistent with transaction_date Saturday (DOW=6) and Sunday (DOW=0) → is_weekend must be TRUE Monday–Friday → is_weekend must be FALSE |

## Reglas por tabla

### `gold.dim_account`

| Columna | Regla |
| --- | --- |
| `account_id` | not_null |
| `account_id` | unique |
| `balance` | is_non_negative |
| `customer_id` | not_null |
| `customer_id` | relationships → dim_customer.customer_id |
| `is_credit_product` | not_null |

### `gold.dim_customer`

| Columna | Regla |
| --- | --- |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `customer_id` | unique |

### `gold.dim_date`

| Columna | Regla |
| --- | --- |
| `date_id` | not_null |
| `date_id` | unique |
| `day_of_week` | accepted_range [0, 6] |
| `full_date` | not_null |
| `full_date` | unique |
| `is_month_end` | not_null |
| `is_weekend` | not_null |
| `month_number` | accepted_range [1, 12] |
| `quarter` | accepted_range [1, 4] |
| `year` | accepted_range [2000, 2050] |

### `gold.fct_live_transactions`

| Columna | Regla |
| --- | --- |
| `account_id` | not_null |
| `account_id` | relationships → dim_account.account_id |
| `amount` | is_non_negative |
| `amount` | not_null |
| `customer_id` | not_null |
| `customer_id` | relationships → dim_customer.customer_id |
| `date_id` | not_null |
| `date_id` | relationships → dim_date.date_id |
| `fx_rate_source` | accepted_values (event_window, fx_rates_current) |
| `fx_usd_per_unit` | is_positive |
| `fx_usd_per_unit` | not_null |
| `is_failed` | not_null |
| `is_revenue` | not_null |
| `latency_seconds` | is_non_negative |
| `latency_seconds` | not_null |
| `revenue_amount` | is_non_negative |
| `transaction_id` | not_null |
| `transaction_id` | unique |

### `gold.fct_loans`

| Columna | Regla |
| --- | --- |
| `customer_id` | not_null |
| `customer_id` | relationships → dim_customer.customer_id |
| `date_id` | relationships → dim_date.date_id |
| `days_past_due` | is_non_negative |
| `estimated_monthly_interest` | is_non_negative |
| `is_delinquent` | not_null |
| `loan_age_months` | is_non_negative |
| `loan_id` | not_null |
| `loan_id` | unique |
| `monthly_payment` | is_non_negative |
| `outstanding_balance` | is_non_negative |
| `pct_outstanding` | accepted_range [0, 1] |
| `principal` | is_non_negative |

### `gold.fct_transactions`

| Columna | Regla |
| --- | --- |
| `account_id` | relationships → dim_account.account_id |
| `amount` | is_non_negative |
| `customer_id` | not_null |
| `customer_id` | relationships → dim_customer.customer_id |
| `date_id` | relationships → dim_date.date_id |
| `is_failed` | not_null |
| `is_revenue` | not_null |
| `revenue_amount` | is_non_negative |
| `transaction_id` | not_null |
| `transaction_id` | unique |

### `gold.mart_customer_summary`

| Columna | Regla |
| --- | --- |
| `account_count` | is_non_negative |
| `customer_id` | not_null |
| `customer_id` | relationships → dim_customer.customer_id |
| `customer_id` | unique |
| `loan_count` | is_non_negative |
| `product_count` | is_non_negative |
| `total_balance` | is_non_negative |
| `total_revenue` | is_non_negative |

### `silver.accounts`

| Columna | Regla |
| --- | --- |
| `account_age_months` | is_non_negative |
| `account_age_months` | not_null |
| `account_id` | not_null |
| `account_id` | unique |
| `account_type` | accepted_values (checking, savings, credit_card, investment) |
| `account_type` | not_null |
| `balance` | not_null |
| `balance_usd` | is_non_negative |
| `branch_code` | not_null |
| `currency` | not_null |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `load_timestamp` | not_null |
| `opened_date` | is_valid_date_format |
| `status` | accepted_values (active, closed, frozen) |
| `status` | not_null |

### `silver.credit_info`

| Columna | Regla |
| --- | --- |
| `bankruptcy_flag` | not_null |
| `credit_score` | accepted_range [350, 800] |
| `credit_score_band` | accepted_values (poor, fair, good, very_good, exceptional) |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `inquiries_6m` | is_non_negative |
| `inquiries_6m` | not_null |
| `late_payments_12m` | is_non_negative |
| `late_payments_12m` | not_null |
| `load_timestamp` | not_null |
| `num_credit_accounts` | is_non_negative |
| `num_credit_accounts` | not_null |
| `total_limit_usd` | is_non_negative |
| `total_used_usd` | is_non_negative |
| `utilization_pct` | accepted_range [0, 100] |
| `utilization_pct` | not_null |
| `utilization_tier` | accepted_values (low, medium, high) |

### `silver.customers`

| Columna | Regla |
| --- | --- |
| `age` | accepted_range [18, 120] |
| `age` | not_null |
| `age_bucket` | accepted_values (18-25, 26-35, 36-50, 51-65, 65+) |
| `country` | not_null |
| `customer_id` | not_null |
| `customer_id` | unique |
| `customer_segment` | accepted_values (retail, private_banking, sme, premium) |
| `date_of_birth` | is_valid_date_format |
| `email` | not_null |
| `email_status` | accepted_values (valid, invalid) |
| `email_status` | not_null |
| `first_name` | not_null |
| `kyc_status` | accepted_values (verified, pending, rejected, expired) |
| `kyc_status` | not_null |
| `last_name` | not_null |
| `load_timestamp` | not_null |
| `registration_date` | is_valid_date_format |
| `risk_score` | accepted_range [0, 100] |
| `risk_score` | not_null |
| `risk_tier` | accepted_values (low, medium, high, critical) |
| `status` | accepted_values (active, inactive, closed, suspended, frozen) |
| `status` | not_null |
| `tenure_days` | is_non_negative |

### `silver.digital_engagement`

| Columna | Regla |
| --- | --- |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `days_since_last_login` | is_non_negative |
| `days_since_last_login` | not_null |
| `is_digital` | not_null |
| `last_login_date` | is_valid_date_format |
| `load_timestamp` | not_null |
| `mobile_app_registered` | not_null |
| `web_banking_registered` | not_null |

### `silver.fx_rates_current`

| Columna | Regla |
| --- | --- |
| `currency` | not_null |
| `currency` | unique |
| `rate_source` | accepted_values (stream, seed) |
| `rate_source` | not_null |
| `usd_per_unit` | is_positive |
| `usd_per_unit` | not_null |

### `silver.live_transactions`

| Columna | Regla |
| --- | --- |
| `amount` | is_positive |
| `amount` | not_null |
| `fx_rate_source` | accepted_values (event_window, fx_rates_current) |
| `fx_rate_source` | not_null |
| `transaction_id` | not_null |
| `transaction_id` | unique |

### `silver.loans`

| Columna | Regla |
| --- | --- |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `days_past_due` | is_non_negative |
| `days_past_due` | not_null |
| `dpd_bucket` | accepted_values (current, 1-30, 31-60, 61-90, 90+) |
| `end_date` | is_valid_date_format |
| `interest_rate` | not_null |
| `is_delinquent` | not_null |
| `load_timestamp` | not_null |
| `loan_id` | not_null |
| `loan_id` | unique |
| `outstanding_balance` | not_null |
| `outstanding_balance_usd` | is_non_negative |
| `principal` | is_positive |
| `principal` | not_null |
| `principal_usd` | is_non_negative |
| `start_date` | is_valid_date_format |
| `status` | accepted_values (current, delinquent, default, paid_off) |
| `status` | not_null |
| `term_months` | not_null |
| `type` | accepted_values (mortgage, personal, auto, education, business) |
| `type` | not_null |

### `silver.transactions`

| Columna | Regla |
| --- | --- |
| `account_id` | not_null |
| `account_id` | relationships → accounts.account_id |
| `amount` | is_positive |
| `amount` | not_null |
| `amount_usd` | is_non_negative |
| `channel` | accepted_values (atm, branch, mobile, pos, web) |
| `channel` | not_null |
| `currency` | not_null |
| `customer_id` | not_null |
| `customer_id` | relationships → customers.customer_id |
| `day_of_week` | not_null |
| `is_weekend` | not_null |
| `load_timestamp` | not_null |
| `status` | accepted_values (completed, pending, failed, reversed) |
| `status` | not_null |
| `transaction_date` | not_null |
| `transaction_id` | not_null |
| `transaction_id` | unique |
| `transaction_month` | accepted_range [1, 12] |
| `transaction_month` | not_null |
| `transaction_week` | accepted_range [1, 53] |
| `transaction_week` | not_null |
| `transaction_year` | not_null |
| `type` | accepted_values (refund, transfer, withdrawal, fee, deposit, payment) |
| `type` | not_null |

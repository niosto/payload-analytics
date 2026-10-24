# Diccionario de datos

Una entrada por columna de cada tabla del warehouse (`payload_analytics` en RDS PostgreSQL). El tipo es el que reporta Postgres; la descripción y los tests salen de los `schema.yml` de dbt. Se regenera con `build_governance_docs.py`.

**Dueño del dato:** Nicolás Ospina. Ver `raci.md`.

## Clasificación de sensibilidad

| Clase | Qué incluye | Columnas en el warehouse | Regla de uso |
| --- | --- | --- | --- |
| PII directa | Identifica a una persona por sí sola: nombre, correo, teléfono, dirección, coordenadas | 17 | Solo en Silver y en `gold.dim_customer`. No se exporta ni se comparte fuera del equipo. Pendiente: enmascarar con hash SHA-256 antes de Gold. |
| PII indirecta | Identifica combinada con otros datos: fecha de nacimiento, género, nacionalidad, ciudad, edad | 16 | Reportar agregada (rangos de edad, país), nunca fila a fila con PII directa. |
| Financiera sensible | Score crediticio, riesgo, saldos, cupos, mora | 43 | Uso analítico interno. Los reportes externos van agregados. |
| Interno | Identificadores técnicos, fechas, categorías, montos de transacción | 299 | Sin restricción dentro del equipo. |

## Fuentes (staging de Spark y streaming)

### `silver.stg_accounts` (tabla)

Table with accounts data

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. |  |
| `account_id` | character varying | Interno | Primary key — one row per account. |  |
| `account_type` | character varying | Interno | Type of financial account. |  |
| `currency` | character varying | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `balance` | character varying | Financiera sensible | Account balance in USD. |  |
| `credit_limit` | character varying | Financiera sensible | Credit limit of the account; NULL for non-credit products. |  |
| `interest_rate` | character varying | Interno | Annual interest rate (numeric). |  |
| `opened_date` | character varying | Interno | Date the account was opened (YYYY-MM-DD). |  |
| `status` | character varying | Interno | Account lifecycle status. |  |
| `branch_code` | character varying | Interno | Branch identifier. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. |  |

### `silver.stg_customers` (tabla)

Table with customers' data (includes credit_info and digital_engagement as JSONB columns)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. |  |
| `first_name` | character varying | PII directa | Customer first name in Proper Case. |  |
| `last_name` | character varying | PII directa | Customer last name in Proper Case. |  |
| `email` | character varying | PII directa | Cleaned customer e-mail address. |  |
| `phone_number` | character varying | PII directa | Phone number; normalized with the country prefix from seed_phone_codes in Silver. |  |
| `date_of_birth` | character varying | PII indirecta | Date of birth (YYYY-MM-DD). |  |
| `gender` | character varying | PII indirecta | Customer gender as declared; disguised nulls normalized. |  |
| `nationality` | character varying | PII indirecta | Customer nationality as declared. |  |
| `city` | character varying | PII indirecta | Customer city, mapped to its canonical name through seed_city. |  |
| `country` | character varying | Interno | ISO country code (UPPERCASED). |  |
| `address` | character varying | PII directa | Street address of the customer. |  |
| `lat` | character varying | PII directa | Latitude of the customer address. |  |
| `lon` | character varying | PII directa | Longitude of the customer address. |  |
| `registration_date` | character varying | Interno | Date the customer registered (YYYY-MM-DD). |  |
| `kyc_status` | character varying | Interno | Know-Your-Customer verification status. |  |
| `risk_score` | character varying | Financiera sensible | Numeric risk score in range 0–100. |  |
| `customer_segment` | character varying | Interno | Standardised customer segment. |  |
| `relationship_manager` | character varying | Interno | Bank employee assigned to the customer. |  |
| `status` | character varying | Interno | Account lifecycle status. |  |
| `credit_info` | character varying | Financiera sensible | Credit bureau object as JSON text; flattened into silver.credit_info. |  |
| `digital_engagement` | character varying | Interno | Digital channel usage object as JSON text; flattened into silver.digital_engagement. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. |  |

### `silver.stg_loans` (tabla)

Table with loans data

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. |  |
| `loan_id` | character varying | Interno | Primary key — one row per loan. |  |
| `type` | character varying | Interno | Loan product type. |  |
| `currency` | character varying | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `principal` | character varying | Interno | Original principal in USD. |  |
| `outstanding_balance` | character varying | Financiera sensible | Remaining balance in USD. |  |
| `interest_rate` | character varying | Interno | Annual interest rate (numeric). |  |
| `term_months` | character varying | Interno | Contracted loan term in months. |  |
| `monthly_payment` | character varying | Interno | Monthly payment in USD. |  |
| `start_date` | character varying | Interno | Loan origination date (YYYY-MM-DD). |  |
| `end_date` | character varying | Interno | Loan maturity date (YYYY-MM-DD). |  |
| `status` | character varying | Interno | Account lifecycle status. |  |
| `days_past_due` | character varying | Financiera sensible | Days the loan payment is overdue. |  |
| `collateral_type` | character varying | Interno | Collateral backing the loan; NULL for unsecured loans. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. |  |

### `silver.stg_transactions` (tabla)

Table with transactions' data

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. |  |
| `transaction_id` | character varying | Interno | Primary key — one row per live transaction. |  |
| `account_id` | character varying | Interno | Primary key — one row per account. |  |
| `date` | character varying | Interno | Transaction date as received, in one of four source formats; parsed by format_date in Silver. |  |
| `amount` | character varying | Interno | Transaction amount in USD. |  |
| `currency` | character varying | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `type` | character varying | Interno | Loan product type. |  |
| `category` | character varying | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | character varying | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | character varying | Interno | Transaction channel. |  |
| `status` | character varying | Interno | Account lifecycle status. |  |
| `description` | character varying | Interno | Free-text description of the transaction; disguised nulls normalized. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. |  |

### `silver.stream_fx_rates` (tabla)

FX ticks written by the streaming job (one row per currency per tick, usd_per_unit like seed_fx_rates)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `event_id` | character varying | Interno | Unique id of the Kafka event that carried the row. |  |
| `currency` | character varying | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `base` | character varying | Interno | Base currency of the rate; always USD. |  |
| `units_per_usd` | double precision | Interno | Units of the currency for one USD. |  |
| `usd_per_unit` | double precision | Interno | USD for one unit of the currency. |  |
| `provider` | character varying | Interno | Where the rate came from: binance (market trades) or open.er-api.com (daily API). |  |
| `provider_updated_at` | timestamp with time zone | Interno | When the provider set this price: the Binance trade time, or the daily API update time. |  |
| `event_time` | timestamp with time zone | Interno | When the event happened, set by the producer (UTC). |  |
| `kafka_ts` | timestamp with time zone | Interno | When Redpanda accepted the event (UTC). |  |
| `processed_at` | timestamp with time zone | Interno | When the streaming job wrote the row (UTC); processed_at - event_time is the end-to-end latency. |  |

### `silver.stream_transactions` (tabla)

Live transactions written by the streaming job, joined with the FX tick in effect at event_time

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `transaction_id` | character varying | Interno | Primary key — one row per live transaction. |  |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. |  |
| `account_id` | character varying | Interno | Primary key — one row per account. |  |
| `date` | character varying | Interno | Transaction date as received, in one of four source formats; parsed by format_date in Silver. |  |
| `amount` | character varying | Interno | Transaction amount in USD. |  |
| `currency` | character varying | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `type` | character varying | Interno | Loan product type. |  |
| `category` | character varying | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | character varying | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | character varying | Interno | Transaction channel. |  |
| `status` | character varying | Interno | Account lifecycle status. |  |
| `description` | character varying | Interno | Free-text description of the transaction; disguised nulls normalized. |  |
| `event_id` | character varying | Interno | Unique id of the Kafka event that carried the row. |  |
| `source` | character varying | Interno | Producer that generated the event (simulator for live transactions). |  |
| `event_time` | timestamp with time zone | Interno | When the event happened, set by the producer (UTC). |  |
| `kafka_ts` | timestamp with time zone | Interno | When Redpanda accepted the event (UTC). |  |
| `fx_usd_per_unit` | double precision | Interno | Rate applied to convert amount_native to USD. |  |
| `fx_event_time` | timestamp with time zone | Interno | event_time of the FX tick joined to the transaction; NULL when no tick was in the window. |  |
| `fx_provider_updated_at` | timestamp with time zone | Interno | When the joined FX price was actually set by its provider (trade time or API update). |  |
| `processed_at` | timestamp with time zone | Interno | When the streaming job wrote the row (UTC); processed_at - event_time is the end-to-end latency. |  |

## Silver (dbt)

### `silver.accounts` (tabla)

Cleaned account fact table. One row per account.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Foreign key to customers. | not_null, relationships → customers.customer_id |
| `account_id` | character varying | Interno | Primary key — unique identifier for each account. | not_null, unique |
| `account_type` | text | Interno | Type of financial account. | accepted_values (checking, savings, credit_card, investment), not_null |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). | not_null |
| `is_currency_mismatch` | boolean | Interno | True when the account currency differs from the expected currency of the customer's country. |  |
| `balance` | double precision | Financiera sensible | Current account balance. | not_null |
| `balance_usd` | double precision | Financiera sensible | Account balance converted to USD via seed_fx_rates. | is_non_negative |
| `credit_limit` | double precision | Financiera sensible | Credit limit of the account; NULL for non-credit products. |  |
| `credit_limit_usd` | double precision | Financiera sensible | credit_limit converted to USD with to_usd. |  |
| `interest_rate` | double precision | Interno | Annual interest rate (numeric). |  |
| `opened_date` | date | Interno | Date the account was opened (YYYY-MM-DD). | is_valid_date_format |
| `account_age_months` | integer | Interno | Months since the account was opened (derived). | is_non_negative, not_null |
| `status` | text | Interno | Account lifecycle status. | accepted_values (active, closed, frozen), not_null |
| `branch_code` | text | Interno | Branch identifier. | not_null |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |

### `silver.credit_info` (tabla)

Credit bureau attributes extracted from the credit_info JSONB field in stg_customers. One row per customer that has credit data.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | text | Interno | Foreign key to customers. | not_null, relationships → customers.customer_id |
| `credit_score` | integer | Financiera sensible | Numeric credit score (300–850). | accepted_range [350, 800] |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `utilization_pct` | double precision | Financiera sensible | Credit utilisation as a percentage (0–100). | accepted_range [0, 100], not_null |
| `total_limit` | double precision | Financiera sensible | Total credit limit across the customer's credit accounts, in USD in Gold. |  |
| `total_limit_usd` | double precision | Financiera sensible | Total credit limit converted to USD via seed_fx_rates. | is_non_negative |
| `total_used` | double precision | Financiera sensible | Total credit used across the customer's credit accounts, in USD in Gold. |  |
| `total_used_usd` | double precision | Financiera sensible | Total credit used converted to USD via seed_fx_rates. | is_non_negative |
| `num_credit_accounts` | integer | Interno | Number of open credit accounts. | is_non_negative, not_null |
| `oldest_account_age_months` | integer | Interno | Age in months of the customer's oldest credit account. |  |
| `late_payments_12m` | integer | Financiera sensible | Number of late payments in the last 12 months. | is_non_negative, not_null |
| `inquiries_6m` | integer | Financiera sensible | Number of credit enquiries in the last 6 months. | is_non_negative, not_null |
| `bankruptcy_flag` | boolean | Financiera sensible | Boolean — customer has declared bankruptcy. | not_null |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |
| `credit_score_band` | text | Financiera sensible | Credit quality band derived from credit_score. | accepted_values (poor, fair, good, very_good, exceptional) |
| `utilization_tier` | text | Financiera sensible | Utilisation risk tier derived from utilization_pct. | accepted_values (low, medium, high) |

### `silver.customers` (tabla)

Cleaned and standardized customer master data. One row per customer. Includes demographic, contact, and derived behavioural columns.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Primary key — unique identifier for each customer. | not_null, unique |
| `first_name` | text | PII directa | Customer first name in Proper Case. | not_null |
| `last_name` | text | PII directa | Customer last name in Proper Case. | not_null |
| `email` | text | PII directa | Cleaned customer e-mail address. | not_null |
| `email_status` | text | Interno | Result of e-mail validation macro. | accepted_values (valid, invalid), not_null |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |
| `gender` | text | PII indirecta | Customer gender as declared; disguised nulls normalized. |  |
| `nationality` | text | PII indirecta | Customer nationality as declared. |  |
| `city` | text | PII indirecta | Customer city, mapped to its canonical name through seed_city. |  |
| `country` | text | Interno | ISO country code (UPPERCASED). | not_null |
| `address` | text | PII directa | Street address of the customer. |  |
| `lat` | double precision | PII directa | Latitude of the customer address. |  |
| `lon` | double precision | PII directa | Longitude of the customer address. |  |
| `registration_date` | date | Interno | Date the customer registered (YYYY-MM-DD). | is_valid_date_format |
| `tenure_days` | integer | Interno | Days since registration_date (derived). | is_non_negative |
| `tenure_years` | integer | Interno | Whole years since registration_date. |  |
| `kyc_status` | text | Interno | Know-Your-Customer verification status. | accepted_values (verified, pending, rejected, expired), not_null |
| `risk_score` | double precision | Financiera sensible | Numeric risk score in range 0–100. | accepted_range [0, 100], not_null |
| `risk_tier` | text | Financiera sensible | Risk band derived from risk_score (derived). | accepted_values (low, medium, high, critical) |
| `customer_segment` | text | Interno | Standardised customer segment. | accepted_values (retail, private_banking, sme, premium) |
| `relationship_manager` | text | Interno | Bank employee assigned to the customer. |  |
| `status` | text | Interno | Account lifecycle status. | accepted_values (active, inactive, closed, suspended, frozen), not_null |
| `phone_number` | text | PII directa | Phone number; normalized with the country prefix from seed_phone_codes in Silver. |  |
| `date_of_birth` | date | PII indirecta | Date of birth (YYYY-MM-DD). | is_valid_date_format |
| `age` | integer | PII indirecta | Customer age in whole years (derived from date_of_birth). | accepted_range [18, 120], not_null |
| `age_bucket` | text | PII indirecta | Age cohort bucket (derived). | accepted_values (18-25, 26-35, 36-50, 51-65, 65+) |

### `silver.digital_engagement` (tabla)

Digital channel preferences and login activity, extracted from the digital_engagement JSONB field in stg_customers.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | text | Interno | Foreign key to customers. | not_null, relationships → customers.customer_id |
| `mobile_app_registered` | boolean | Interno | Boolean — customer is enrolled in mobile banking. | not_null |
| `web_banking_registered` | boolean | Interno | Boolean — customer is enrolled in web banking. | not_null |
| `last_login_date` | date | Interno | Date of the customer's last digital login. | is_valid_date_format |
| `avg_monthly_logins` | integer | Interno | Average digital logins per month. |  |
| `preferred_channel` | text | Interno | Channel the customer uses most (mobile, web, branch...). |  |
| `push_notifications` | boolean | Interno | True when push notifications are enabled. |  |
| `paperless_statements` | boolean | Interno | True when the customer receives statements digitally only. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |
| `is_digital` | boolean | Interno | True if mobile or web is the preferred channel (derived). | not_null |
| `days_since_last_login` | integer | Interno | Days elapsed since last_login_date (derived). | is_non_negative, not_null |

### `silver.fx_rates_current` (tabla)

Rate to_usd applies, one row per currency. Latest live tick from stream_fx_rates when newer than fx_live_max_age_hours, else seed_fx_rates.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `currency` | text | Interno | ISO currency code, upper case. | not_null, unique |
| `usd_per_unit` | double precision | Interno | USD for one unit of the currency. | is_positive, not_null |
| `rate_source` | text | Interno | stream when a recent live tick exists, seed otherwise. | accepted_values (stream, seed), not_null |
| `rate_event_time` | timestamp with time zone | Interno | event_time of the live tick behind usd_per_unit; NULL when the seed is used. |  |
| `provider_updated_at` | timestamp with time zone | Interno | When the provider set this price: the Binance trade time, or the daily API update time. |  |
| `seed_usd_per_unit` | double precision | Interno | Static rate from seed_fx_rates, kept to compare against the live one. |  |

### `silver.live_transactions` (vista)

Live transactions from the stream, cleaned with the batch macros. A view; gold.fct_live_transactions reads it incrementally.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | text | Interno | Foreign key to dim_customer. |  |
| `transaction_id` | text | Interno | Live transaction id (LTX- prefix). | not_null, unique |
| `account_id` | text | Interno | Primary key — one row per account. |  |
| `transaction_date` | date | Interno | Date of the transaction (YYYY-MM-DD). |  |
| `is_weekend` | boolean | Interno | True on Saturday or Sunday. |  |
| `day_of_week` | text | Interno | Day of week (0=Sunday, 6=Saturday). |  |
| `amount` | double precision | Interno | Native amount after clean_amount. | is_positive, not_null |
| `amount_usd` | double precision | Interno | Transaction amount converted to USD via seed_fx_rates. |  |
| `fx_usd_per_unit` | double precision | Interno | Rate applied to convert amount_native to USD. |  |
| `fx_rate_source` | text | Interno | event_window when the stream joined a tick, else fx_rates_current. | accepted_values (event_window, fx_rates_current), not_null |
| `fx_event_time` | timestamp with time zone | Interno | event_time of the FX tick joined to the transaction; NULL when no tick was in the window. |  |
| `fx_provider_updated_at` | timestamp with time zone | Interno | When the joined FX price was actually set by its provider (trade time or API update). |  |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `type` | text | Interno | Loan product type. |  |
| `category` | text | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | text | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | text | Interno | Transaction channel. |  |
| `status` | text | Interno | Account lifecycle status. |  |
| `description` | text | Interno | Free-text description of the transaction; disguised nulls normalized. |  |
| `event_time` | timestamp with time zone | Interno | When the event happened, set by the producer (UTC). |  |
| `kafka_ts` | timestamp with time zone | Interno | When Redpanda accepted the event (UTC). |  |
| `processed_at` | timestamp with time zone | Interno | When the streaming job wrote the row (UTC); processed_at - event_time is the end-to-end latency. |  |
| `latency_seconds` | numeric | Interno | processed_at minus event_time, end-to-end stream latency. |  |

### `silver.loans` (tabla)

Cleaned loan portfolio fact table. One row per loan.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | text | Interno | Foreign key to customers. | not_null, relationships → customers.customer_id |
| `loan_id` | text | Interno | Primary key — unique identifier for each loan. | not_null, unique |
| `type` | text | Interno | Loan product type. | accepted_values (mortgage, personal, auto, education, business), not_null |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `principal` | double precision | Interno | Original loan principal amount. | is_positive, not_null |
| `principal_usd` | double precision | Interno | Principal converted to USD via seed_fx_rates. | is_non_negative |
| `outstanding_balance` | double precision | Financiera sensible | Remaining balance owed. | not_null |
| `outstanding_balance_usd` | double precision | Financiera sensible | Outstanding balance converted to USD via seed_fx_rates. | is_non_negative |
| `interest_rate` | double precision | Interno | Annual interest rate (numeric). | not_null |
| `term_months` | integer | Interno | Contracted loan term in months. | not_null |
| `monthly_payment` | double precision | Interno | Monthly payment in USD. |  |
| `monthly_payment_usd` | double precision | Interno | monthly_payment converted to USD with to_usd. |  |
| `start_date` | date | Interno | Loan origination date (YYYY-MM-DD). | is_valid_date_format |
| `end_date` | date | Interno | Loan maturity date (YYYY-MM-DD). | is_valid_date_format |
| `status` | text | Interno | Current loan status. | accepted_values (current, delinquent, default, paid_off), not_null |
| `is_delinquent` | boolean | Financiera sensible | True when status is 'delinquent' or 'default' (derived). | not_null |
| `days_past_due` | integer | Financiera sensible | Number of days the loan payment is overdue. | is_non_negative, not_null |
| `dpd_bucket` | text | Interno | Days-past-due bucket (derived). | accepted_values (current, 1-30, 31-60, 61-90, 90+) |
| `collateral_type` | text | Interno | Collateral backing the loan; NULL for unsecured loans. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |

### `silver.transactions` (tabla)

Cleaned and standardised transaction fact table. One row per transaction event.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | text | Interno | Foreign key to customers. | not_null, relationships → customers.customer_id |
| `transaction_id` | text | Interno | Primary key — unique identifier for each transaction. | not_null, unique |
| `account_id` | text | Interno | Foreign key to accounts. | not_null, relationships → accounts.account_id |
| `transaction_date` | date | Interno | Date of the transaction (YYYY-MM-DD). | not_null |
| `transaction_year` | double precision | Interno | Calendar year extracted from transaction_date (derived). | not_null |
| `transaction_month` | integer | Interno | Calendar month (1–12) extracted from transaction_date (derived). | accepted_range [1, 12], not_null |
| `transaction_week` | integer | Interno | ISO week number (1–53) extracted from transaction_date (derived). | accepted_range [1, 53], not_null |
| `is_weekend` | boolean | Interno | True if the transaction occurred on Saturday or Sunday (derived). | not_null |
| `day_of_week` | text | Interno | Full day name (e.g. 'Monday ') derived from transaction_date. | not_null |
| `amount` | double precision | Interno | Transaction amount (positive numeric). | is_positive, not_null |
| `amount_usd` | double precision | Interno | Transaction amount converted to USD via seed_fx_rates. | is_non_negative |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). | not_null |
| `type` | text | Interno | Standardised transaction type. | accepted_values (refund, transfer, withdrawal, fee, deposit, payment), not_null |
| `category` | text | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | text | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | text | Interno | Transaction channel. | accepted_values (atm, branch, mobile, pos, web), not_null |
| `status` | text | Interno | Transaction processing status. | accepted_values (completed, pending, failed, reversed), not_null |
| `description` | text | Interno | Free-text description of the transaction; disguised nulls normalized. |  |
| `load_timestamp` | timestamp without time zone | Interno | Timestamp when the record was loaded. | not_null |

## Gold (dbt)

### `gold.dim_account` (tabla)

Account dimension with USD balances and customer context.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `account_id` | character varying | Interno | Primary key — one row per account. | not_null, unique |
| `customer_id` | character varying | Interno | Foreign key to dim_customer. | not_null, relationships → dim_customer.customer_id |
| `account_type` | text | Interno | Type of financial account. |  |
| `is_credit_product` | boolean | Interno | True when account_type is credit_card. | not_null |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `balance` | double precision | Financiera sensible | Account balance in USD. | is_non_negative |
| `credit_limit` | double precision | Financiera sensible | Credit limit of the account; NULL for non-credit products. |  |
| `interest_rate` | double precision | Interno | Annual interest rate (numeric). |  |
| `opened_date` | date | Interno | Date the account was opened (YYYY-MM-DD). |  |
| `account_age_months` | integer | Interno | Months since the account was opened (derived). |  |
| `status` | text | Interno | Account lifecycle status. |  |
| `branch_code` | text | Interno | Branch identifier. |  |
| `customer_segment` | text | Interno | Standardised customer segment. |  |
| `country` | text | Interno | ISO country code (UPPERCASED). |  |

### `gold.dim_customer` (tabla)

Customer dimension with credit and digital engagement attributes.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Primary key — one row per customer. | not_null, relationships → customers.customer_id, unique |
| `first_name` | text | PII directa | Customer first name in Proper Case. |  |
| `last_name` | text | PII directa | Customer last name in Proper Case. |  |
| `email` | text | PII directa | Cleaned customer e-mail address. |  |
| `country` | text | Interno | ISO country code (UPPERCASED). |  |
| `city` | text | PII indirecta | Customer city, mapped to its canonical name through seed_city. |  |
| `gender` | text | PII indirecta | Customer gender as declared; disguised nulls normalized. |  |
| `nationality` | text | PII indirecta | Customer nationality as declared. |  |
| `date_of_birth` | date | PII indirecta | Date of birth (YYYY-MM-DD). |  |
| `age` | integer | PII indirecta | Customer age in whole years (derived from date_of_birth). |  |
| `age_bucket` | text | PII indirecta | Age cohort bucket (derived). |  |
| `registration_date` | date | Interno | Date the customer registered (YYYY-MM-DD). |  |
| `registration_month` | date | Interno | First day of the registration month, for cohort analysis. |  |
| `tenure_days` | integer | Interno | Days since registration_date (derived). |  |
| `tenure_years` | integer | Interno | Whole years since registration_date. |  |
| `kyc_status` | text | Interno | Know-Your-Customer verification status. |  |
| `risk_score` | double precision | Financiera sensible | Numeric risk score in range 0–100. |  |
| `risk_tier` | text | Financiera sensible | Risk band derived from risk_score (derived). |  |
| `customer_segment` | text | Interno | Standardised customer segment. |  |
| `status` | text | Interno | Account lifecycle status. |  |
| `relationship_manager` | text | Interno | Bank employee assigned to the customer. |  |
| `credit_score` | integer | Financiera sensible | Numeric credit score (300–850). |  |
| `credit_score_band` | text | Financiera sensible | Credit quality band derived from credit_score. |  |
| `utilization_pct` | double precision | Financiera sensible | Credit utilisation as a percentage (0–100). |  |
| `utilization_tier` | text | Financiera sensible | Utilisation risk tier derived from utilization_pct. |  |
| `total_limit` | double precision | Financiera sensible | Total credit limit across the customer's credit accounts, in USD in Gold. |  |
| `total_used` | double precision | Financiera sensible | Total credit used across the customer's credit accounts, in USD in Gold. |  |
| `late_payments_12m` | integer | Financiera sensible | Number of late payments in the last 12 months. |  |
| `bankruptcy_flag` | boolean | Financiera sensible | Boolean — customer has declared bankruptcy. |  |
| `is_digital` | boolean | Interno | True if mobile or web is the preferred channel (derived). |  |
| `preferred_channel` | text | Interno | Channel the customer uses most (mobile, web, branch...). |  |
| `avg_monthly_logins` | integer | Interno | Average digital logins per month. |  |
| `days_since_last_login` | integer | Interno | Days elapsed since last_login_date (derived). |  |
| `mobile_app_registered` | boolean | Interno | Boolean — customer is enrolled in mobile banking. |  |

### `gold.dim_date` (tabla)

Calendar dimension (2000-01-01 through 2050-12-31).

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `date_id` | integer | Interno | Surrogate key YYYYMMDD as integer. | not_null, unique |
| `full_date` | date | Interno | Calendar date. | not_null, unique |
| `day_name` | text | Interno | Day of the week name. |  |
| `day_of_week` | numeric | Interno | Day of week (0=Sunday, 6=Saturday). | accepted_range [0, 6] |
| `week_number` | integer | Interno | ISO week of the year. |  |
| `month_number` | integer | Interno | Calendar month (1-12). | accepted_range [1, 12] |
| `month_name` | text | Interno | Month name. |  |
| `quarter` | integer | Interno | Calendar quarter (1-4). | accepted_range [1, 4] |
| `year` | integer | Interno | Calendar year. | accepted_range [2000, 2050] |
| `is_weekend` | boolean | Interno | True on Saturday or Sunday. | not_null |
| `is_month_end` | boolean | Interno | True on the last day of the month. | not_null |

### `gold.fct_live_transactions` (tabla)

Live transaction fact from the stream. Same columns as fct_transactions, plus the FX rate applied, its source, and the stream latency.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `transaction_id` | text | Interno | Primary key — one row per live transaction. | not_null, unique |
| `customer_id` | text | Interno | Foreign key to dim_customer. | not_null, relationships → dim_customer.customer_id |
| `account_id` | text | Interno | Foreign key to dim_account. | not_null, relationships → dim_account.account_id |
| `date_id` | integer | Interno | Foreign key to dim_date (YYYYMMDD integer). | not_null, relationships → dim_date.date_id |
| `transaction_date` | date | Interno | Date of the transaction (YYYY-MM-DD). |  |
| `day_of_week` | text | Interno | Day of week (0=Sunday, 6=Saturday). |  |
| `is_weekend` | boolean | Interno | True on Saturday or Sunday. |  |
| `customer_country` | text | Interno | Country of the customer who made the transaction (ISO code), from dim_customer. |  |
| `amount` | double precision | Interno | Transaction amount in USD. | is_non_negative, not_null |
| `amount_native` | double precision | Interno | Transaction amount in its own currency, after clean_amount. |  |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `fx_usd_per_unit` | double precision | Interno | Rate applied to convert amount_native to USD. | is_positive, not_null |
| `fx_rate_source` | text | Interno | event_window (tick joined in the stream) or fx_rates_current (fallback). | accepted_values (event_window, fx_rates_current) |
| `fx_event_time` | timestamp with time zone | Interno | event_time of the FX tick joined to the transaction; NULL when no tick was in the window. |  |
| `fx_provider_updated_at` | timestamp with time zone | Interno | When the joined FX price was actually set by its provider (trade time or API update). |  |
| `type` | text | Interno | Loan product type. |  |
| `category` | text | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | text | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | text | Interno | Transaction channel. |  |
| `status` | text | Interno | Account lifecycle status. |  |
| `is_failed` | boolean | Interno | True when transaction status is failed. | not_null |
| `is_international` | boolean | Interno | True when the transaction currency differs from the expected currency of the customer's country. |  |
| `revenue_amount` | double precision | Interno | Revenue in USD (completed fee transactions only). | is_non_negative |
| `is_revenue` | boolean | Interno | True when revenue_amount is positive. | not_null |
| `description` | text | Interno | Free-text description of the transaction; disguised nulls normalized. |  |
| `event_time` | timestamp with time zone | Interno | When the event happened, set by the producer (UTC). |  |
| `kafka_ts` | timestamp with time zone | Interno | When Redpanda accepted the event (UTC). |  |
| `processed_at` | timestamp with time zone | Interno | When the streaming job wrote the row (UTC); processed_at - event_time is the end-to-end latency. |  |
| `latency_seconds` | numeric | Interno | processed_at minus event_time, end-to-end stream latency. | is_non_negative, not_null |

### `gold.fct_loans` (tabla)

Loan fact with USD balances and delinquency flags.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `loan_id` | text | Interno | Primary key — one row per loan. | not_null, unique |
| `customer_id` | text | Interno | Foreign key to dim_customer. | not_null, relationships → dim_customer.customer_id |
| `date_id` | integer | Interno | Foreign key to dim_date (loan start date as YYYYMMDD). | relationships → dim_date.date_id |
| `loan_type` | text | Interno | Loan product (mortgage, personal, auto...). |  |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `principal` | double precision | Interno | Original principal in USD. | is_non_negative |
| `outstanding_balance` | double precision | Financiera sensible | Remaining balance in USD. | is_non_negative |
| `interest_rate` | double precision | Interno | Annual interest rate (numeric). |  |
| `term_months` | integer | Interno | Contracted loan term in months. |  |
| `monthly_payment` | double precision | Interno | Monthly payment in USD. | is_non_negative |
| `loan_age_months` | integer | Interno | Months since loan start date. | is_non_negative |
| `status` | text | Interno | Account lifecycle status. |  |
| `is_delinquent` | boolean | Financiera sensible | True when status is delinquent or default. | not_null |
| `days_past_due` | integer | Financiera sensible | Days the loan payment is overdue. | is_non_negative |
| `dpd_bucket` | text | Interno | Days-past-due bucket (derived). |  |
| `pct_outstanding` | double precision | Interno | Outstanding balance as a fraction of principal. | accepted_range [0, 1] |
| `estimated_monthly_interest` | double precision | Interno | Estimated monthly interest in USD. | is_non_negative |
| `collateral_type` | text | Interno | Collateral backing the loan; NULL for unsecured loans. |  |

### `gold.fct_transactions` (tabla)

Transaction fact with USD amounts and revenue flags.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `transaction_id` | text | Interno | Primary key — one row per transaction. | not_null, unique |
| `customer_id` | text | Interno | Foreign key to dim_customer. | not_null, relationships → dim_customer.customer_id |
| `account_id` | text | Interno | Foreign key to dim_account. | relationships → dim_account.account_id |
| `date_id` | integer | Interno | Foreign key to dim_date (YYYYMMDD integer). | relationships → dim_date.date_id |
| `transaction_date` | date | Interno | Date of the transaction (YYYY-MM-DD). |  |
| `day_of_week` | text | Interno | Day of week (0=Sunday, 6=Saturday). |  |
| `is_weekend` | boolean | Interno | True on Saturday or Sunday. |  |
| `customer_country` | text | Interno | Country of the customer who made the transaction (ISO code), from dim_customer. |  |
| `amount` | double precision | Interno | Transaction amount in USD. | is_non_negative |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `type` | text | Interno | Loan product type. |  |
| `category` | text | Interno | Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized. |  |
| `merchant` | text | Interno | Merchant name; NULL when the source sent an empty or disguised null. |  |
| `channel` | text | Interno | Transaction channel. |  |
| `status` | text | Interno | Account lifecycle status. |  |
| `is_failed` | boolean | Interno | True when transaction status is failed. | not_null |
| `is_international` | boolean | Interno | True when the transaction currency differs from the expected currency of the customer's country. |  |
| `revenue_amount` | double precision | Interno | Revenue in USD (completed fee transactions only). | is_non_negative |
| `is_revenue` | boolean | Interno | True when revenue_amount is positive. | not_null |
| `description` | text | Interno | Free-text description of the transaction; disguised nulls normalized. |  |

### `gold.mart_customer_summary` (vista)

Per-customer metrics; monetary fields are USD.

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_id` | character varying | Interno | Primary key — one row per customer. | not_null, relationships → dim_customer.customer_id, unique |
| `customer_segment` | text | Interno | Standardised customer segment. |  |
| `country` | text | Interno | ISO country code (UPPERCASED). |  |
| `total_revenue` | double precision | Interno | Sum of revenue_amount (USD). | is_non_negative |
| `account_count` | bigint | Interno | Number of accounts per customer. | is_non_negative |
| `total_balance` | double precision | Financiera sensible | Sum of balance (USD). | is_non_negative |
| `loan_count` | bigint | Interno | Number of loans per customer. | is_non_negative |
| `product_count` | bigint | Interno | account_count + loan_count. | is_non_negative |

## Seeds (tablas de referencia)

### `seeds.seed_city` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `city_fuzzy` | text | Interno | Variant as it appears in the source (any case, Spanish, typos). |  |
| `city_clean` | text | Interno | Canonical value the variant maps to. |  |

### `seeds.seed_currency_map` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `country` | text | Interno | ISO country code (UPPERCASED). |  |
| `expected_currency` | text | Interno | Local currency expected for customers of that country. |  |

### `seeds.seed_customer_segment` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `customer_segment_fuzzy` | text | Interno | Variant as it appears in the source (any case, Spanish, typos). |  |
| `customer_segment_clean` | text | Interno | Canonical value the variant maps to. |  |

### `seeds.seed_fx_rates` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `currency` | text | Interno | ISO 4217 currency code (UPPERCASED). |  |
| `usd_per_unit` | double precision | Interno | USD for one unit of the currency. |  |

### `seeds.seed_phone_codes` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `country_code` | text | Interno | Country ISO code. |  |
| `phone_code` | integer | Interno | International dialing prefix of the country. |  |

### `seeds.seed_status` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `status_fuzzy` | text | Interno | Variant as it appears in the source (any case, Spanish, typos). |  |
| `status_clean` | text | Interno | Canonical value the variant maps to. |  |

### `seeds.seed_transaction_type` (tabla)

| Columna | Tipo | Clasificación | Descripción | Tests |
| --- | --- | --- | --- | --- |
| `transaction_type_fuzzy` | text | Interno | Variant as it appears in the source (any case, Spanish, typos). |  |
| `transaction_type_clean` | text | Interno | Canonical value the variant maps to. |  |

# CLAUDE.md: Payload Analytics

## What this project is

ELT pipeline for a LATAM fintech dataset (customers, accounts, transactions, loans, credit bureau attributes, digital engagement). A JSON object in S3 is Bronze. A Glue Spark job flattens it into `silver.stg_*` on RDS PostgreSQL. A Fargate task runs dbt (`seed`, `run`, `test`) to build cleaned Silver and a Gold star schema. Step Functions chains those steps and archives the file. Power BI reads Gold.

Human-facing setup lives in `README.md`. This file is the agent guide: how to run it, what must not be "fixed", and where the code lives.

## How to work in this repository

Do not deploy, `git push`, drop schemas, or change AWS resources unless the developer asks. Give them the command. Do not print AWS keys, the Serverless key, or the database password.

Env files are gitignored: `.envs/aws.env`, `.envs/sls.env`, `infrastructure/.envs/config.env`, `backend/.envs/config.env`, `streaming/.envs/config.env`. Compose injects them only when the container is created. After an edit, recreate that container (`docker compose -f .docker/docker-compose.yml up -d --force-recreate`). Do not rebuild the image for an env change.

`DEPLOY_APP=payload-analytics` is set by each dev container. Region comes from `AWS_DEFAULT_REGION` in `.envs/aws.env`.

## How to run the project

Two containers. The infrastructure one creates S3, RDS, ECR, and alerts. The backend one applies the migration and creates Glue, the dbt task, and Step Functions. Full commands, VPC lookup, and Power BI settings are in `README.md`. Order:

1. Fill `.envs/aws.env`, `.envs/sls.env`, and both `config.env` files. `DEVELOPER` is lowercase. `DEFAULT_VPC_SUBNET_IDS` is comma-separated with no spaces. `ALERTS_EMAIL` is only on the infrastructure file.
2. Start `infrastructure/` (`docker compose -f .docker/docker-compose.yml up -d --build`, then the entrypoint). Deploy `storage`, then `rds`, then `registry`, then `alerts`. Confirm the SNS email.
3. Start `backend/` the same way. Run `./database/rds/migrate.sh`, `./serverless/spark/deploy.sh`, `./serverless/dbt/deploy.sh`, then `serverless deploy` in `serverless/orchestration`.
4. Copy `data/raw/fintech_banking_dataset.json` to `s3://<DataLakeBucket>/raw/`. The bucket name is the SSM parameter `/payload-analytics-storage/DataLakeBucketName`.
5. Watch the state machine `payload-analytics-backend-orchestration`. Success moves the object to `processed/`. Failure moves it to `failed/` and emails the alerts topic.

`dbt/deploy.sh` tags the image with `git rev-parse HEAD`. Orchestration reads the task definition ARN at its own deploy time, so a new dbt image needs orchestration deployed again.

RDS endpoint: SSM `/payload-analytics-rds/DBInstanceEndpoint`. Database `payload_analytics`, user `payload_analytics_admin`, port `5432`, schema `gold`. Password is the `password` field of secret `/payload-analytics-rds/MasterCredentials`.

## Networking

No NAT Gateway. RDS is a public `db.t4g.micro` in the default VPC (`PubliclyAccessible: true`, port 5432 from `0.0.0.0/0`). Glue has no Glue Connection; it uses Glue's managed network, which can reach S3 and the public database. The Fargate task uses the default public subnets and `AssignPublicIp: ENABLED` so it can pull from ECR, read Secrets Manager, and reach RDS. Do not add a NAT Gateway or a Glue Connection to "fix" networking.

## Repository structure

- **`.docker/`**: shared image. `img-infrastructure` is the Serverless toolchain. `img-backend` adds boto3, yoyo, psycopg2, dbt, and the Docker CLI. The backend compose file mounts the host Docker socket.
- **`infrastructure/serverless/storage`**: data lake bucket (EventBridge on) and artifacts bucket. SSM under `/payload-analytics-storage/`.
- **`infrastructure/serverless/rds`**: Postgres 18, `db.t4g.micro`, master user `payload_analytics_admin` (no hyphen; RDS rejects hyphens). Secret name `/payload-analytics-rds/MasterCredentials`. The secret template includes `username` and `dbname`; the RDS attachment adds `host` and `port`.
- **`infrastructure/serverless/registry`**: ECR repository. SSM `/payload-analytics-registry/DbtRepositoryUri`.
- **`infrastructure/serverless/alerts`**: SNS email topic and a monthly budget. SSM `/payload-analytics-alerts/PipelineAlertsTopicArn`.
- **`backend/database/rds/`**: yoyo migration `20260930000000_silver_staging` creates `silver` and the four `stg_*` tables. `20261015000000_silver_streaming` (depends on it) adds `silver.stream_fx_rates` and `silver.stream_transactions`. `20261023000000_stream_indexes` adds the expression index `stream_fx_rates_norm_currency_event_time_idx` (used by `fx_rates_current`) and `stream_transactions_processed_at_idx` (the incremental filter). `migrate.sh` reads the master secret. Rollbacks run newest first: streaming drops its two tables, staging drops the `stg_*` tables and the schema.
- **`backend/src/spark/transformations.py`**: `deduplicate` and `build_silver_frames`. Shared by the Glue job.
- **`backend/src/spark/glue_job.py`**: Glue entrypoint. `--source_path` is the S3 URI. `--secret_name` is the master secret. Reads JSON with `SCHEMA`, adds `load_timestamp`, writes each frame with `mode("overwrite")` and `truncate=true`.
- **`backend/src/spark/schemas/spark_schema.py`**: every field is `StringType` on purpose.
- **`backend/serverless/spark/`**: Glue 5.0, `G.1X` × 2, timeout 30 minutes, `MaxConcurrentRuns: 1`. `deploy.sh` zips `transformations.py` and `schemas/`, uploads the script, the JDBC jar, and the zip, then `serverless deploy`.
- **`backend/src/dbt/`**: dbt project. `profiles.yml` reads `DBT_HOST`, `DBT_USER`, `DBT_PASSWORD`, `DBT_PORT`, `DBT_DBNAME`, `DBT_THREADS`. `Dockerfile` plus `run.sh` (`dbt seed`, `dbt run`, `dbt test`). `DBT_PROFILES_DIR=/dbt`.
- **`backend/serverless/dbt/`**: Fargate cluster, 512 CPU / 1024 MB, egress-only security group. `architecture.js` maps the build machine to `X86_64` or `ARM64`. Secrets inject the five `DBT_*` variables from the master secret JSON keys `host`, `username`, `password`, `dbname`, `port`.
- **`backend/serverless/orchestration/`**: JSONata state machine. EventBridge `Object Created` on prefix `raw/`. Glue sync, then ECS sync, then S3 copy to `processed/` and delete. Catch copies to `failed/`, publishes to SNS, then fails. Glue `ConcurrentRunsExceededException` retries with backoff (30s, rate 2, 6 attempts).
- **`data/raw/fintech_banking_dataset.json`**: sample source. `*.json` is gitignored except `!data/raw/*.json`.
- **`powerbi/`**: dashboard against Gold.
- **`streaming/`**: local Docker stack, not deployed. Redpanda, Redpanda Console (host port 8088), a topic-init job, the two producers in `src/producers/`, and the Spark Structured Streaming job in `src/spark/` (Spark UI on 4040). See the Streaming section.

Every stack uses `region: ${env:AWS_DEFAULT_REGION}`, `stackName: ${env:DEPLOY_APP}-${self:service}`, and publishes outputs to SSM at `/${AWS::StackName}/...`, which other stacks read as `/${DEPLOY_APP}-<service>/...`.

**Stack names do not include `DEVELOPER`; bucket names do.** The team shares one AWS account, so a `serverless deploy` with a different `DEVELOPER` updates the existing stack and tries to replace its buckets (and, for `rds`, the database). Before any deploy, run `aws cloudformation list-stacks` and check whose stacks exist. Use the `DEVELOPER` of the existing stacks, or do not deploy.

## Streaming

Runs locally with `docker compose -f .docker/docker-compose.yml up -d --build` from `streaming/`. Brokers: `redpanda:9092` inside the compose network, `localhost:19092` from the host. `redpanda_init` creates the topics with 3 partitions and is safe to re-run.

| Topic | Key | Producer | Payload |
| --- | --- | --- | --- |
| `fx.rates` | currency | `fx_market_producer.py` (BRL, ARS, MXN, COP, EUR) and `fx_producer.py` (USD, CLP, PEN, UYU) | `event_id`, `event_time`, `currency`, `base` (USD), `units_per_usd`, `usd_per_unit`, `provider`, `provider_updated_at` |
| `fintech.live_transactions` | customer_id | `transactions_simulator.py` | the 12 `stg_transactions` fields as strings, plus `event_id`, `event_time`, `source` |

- **FX market** (`fx_market_producer.py`): Binance public market-data WebSocket `data-stream.binance.vision` (no key; this domain also works where `api.binance.com` is geo-blocked), `@aggTrade` streams. `FX_MARKET_SYMBOLS` is `CURRENCY:SYMBOL:inverse|direct`; `inverse` = local per USDT (USDTBRL), `direct` = USDT per unit (EURUSDT). USDT is treated as USD. Publishes at most one tick per currency per `FX_MARKET_PUBLISH_MS`, plus a heartbeat with the last price every `FX_MARKET_HEARTBEAT_SECONDS`; without it, thin pairs (MXN, COP) leave the 60 s join window empty. Seeds each pair from REST `/trades?limit=1` on start. `provider_updated_at` is the trade time, so on a heartbeat it is older than `event_time`; do not "fix" that. ARS from this source is the crypto dollar, not the official rate.
- **One provider per currency**: `FX_CURRENCIES` (daily producer) and `FX_MARKET_SYMBOLS` must not overlap; the join and `fx_rates_current` take the latest tick regardless of provider, so overlapping sources would alternate prices.
- **FX daily** (`fx_producer.py`): the free API updates about daily. The producer fetches every `FX_FETCH_SECONDS` and republishes the cached rates every `FX_PUBLISH_SECONDS`. `provider_updated_at` is the rate's real age; `event_time` is the tick. On API failure it keeps the last good rates and backs off. It never fabricates a rate. `usd_per_unit` has the same meaning as `seed_fx_rates.usd_per_unit`.
- **Transactions**: accounts and templates come from `data/raw/fintech_banking_dataset.json` (mounted read-only at `/data`), so ids exist in Silver. Template fields are copied from a real transaction in the account's currency, dirt included, on purpose: do not clean them in the producer. Dates use the four source masks with weights 81.2 / 6.3 / 6.3 / 6.2. Ids are `LTX-` + 12 hex, so they never collide with batch `TXN-` ids.
- Both producers use `acks=all`, idempotence, and flush on SIGTERM.

### Spark Structured Streaming (`src/spark/stream_job.py`)

Image `apache/spark:3.5.3` with the Kafka connector jars baked in (no `--packages` at runtime), plus boto3 and psycopg2. `spark-submit --master local[2]`. It writes to the team RDS, so the `20261015000000_silver_streaming` migration must be applied first; without it the container crash-loops.

| Query | Source | Sink | Upsert key |
| --- | --- | --- | --- |
| `fx_sink` | `fx.rates` | `silver.stream_fx_rates` | `event_id`, `DO NOTHING` |
| `tx_sink` | `fintech.live_transactions` LEFT JOIN `fx.rates` | `silver.stream_transactions` | `transaction_id` |

- **Join**: `fx_currency = upper(trim(currency)) AND fx_event_time BETWEEN event_time - FX_JOIN_SECONDS AND event_time`, watermark `WATERMARK` on both sides. Several ticks match; `foreachBatch` keeps the latest per `transaction_id` in plain Python on the driver (a Spark window there adds a shuffle per batch). Left outer on purpose: a transaction with no tick is still written, with NULL FX, after the watermark passes (about 2 minutes later). Do not change it to an inner join; that drops transactions during an FX outage.
- **Upsert**: rows are collected on the driver and written with `psycopg2.execute_values`. On conflict, `tx_sink` only replaces the FX columns when the new tick is later, and never rewrites `processed_at`, which is the latency measurement. Batches are small (seconds of events); do not switch to JDBC `append`, it would duplicate on replay.
- **Credentials**: the RDS master secret via boto3 (`.envs/aws.env` is in the service's `env_file`), `sslmode=require`. If `DB_HOST` is set, the `DB_*` variables are used instead and the secret is not read; that is how the job is tested against a throwaway local Postgres.
- **Session zone**: Spark `spark.sql.session.timeZone=UTC`, container `TZ=UTC`, and the psycopg2 session `timezone=UTC`, because `collect()` returns naive datetimes.
- **Checkpoint**: named volume `payload-analytics-spark-checkpoints` at `/checkpoints/{fx_sink,tx_sink}`. `STARTING_OFFSETS` only applies when there is no checkpoint. Deleting the volume restarts from `latest`.
- **Latency**: with `TRIGGER_SECONDS=1`, against the team RDS: FX p95 about 2.8 s (the PDF KPI is FX under 5 s, met); transactions p50 3–4 s, p95 about 7 s. Locally, transactions p95 was 4.9 s. A 5 s trigger pushed transactions p95 to 9 s. Per batch, `spark_ms` (the stateful join, about 2 s on `local[2]`) dominates `db_ms` (about 0.3 s), so the "batch is falling behind" warnings at 1 s are expected. Next levers if needed: more cores (`local[4]`) or `spark.sql.shuffle.partitions=1`; the latter only takes effect with a fresh checkpoint, because stateful queries keep the partition count stored in it.
- From Git Bash on Windows, `docker run -e CHECKPOINT_DIR=/tmp/...` gets rewritten to a `C:/` path; prefix with `MSYS_NO_PATHCONV=1`.

## Spark

`build_silver_frames` expects one row per customer, nested arrays still present, plus `load_timestamp`.

| Staging table | How it is built | Dedupe key |
| --- | --- | --- |
| `silver.stg_customers` | Drop array cols; `to_json` on `credit_info` / `digital_engagement` | `customer_id` |
| `silver.stg_accounts` | `explode(accounts)` | `account_id` |
| `silver.stg_transactions` | `explode(transactions)` | `transaction_id` |
| `silver.stg_loans` | `explode(loans)` | `loan_id` |

**Why `to_json` on the two structs**: the JDBC writer cannot store `StructType` in a text column. They land as JSON strings. dbt extracts fields with `(col::jsonb)->>'field'`. Do not switch those columns to JSONB in Spark without changing the migration and the dbt sources.

**Why `StringType`**: the source is dirty (letters in amounts, mixed dates, Spanish labels). Casting belongs in dbt.

**Dedup**: `row_number()` over `(pk ORDER BY load_timestamp DESC)`, keep row 1. Inside one file every row shares the Glue `current_timestamp()`, so this only collapses duplicate keys in that file. The next file replaces the tables (`overwrite` + `truncate`); it does not append.

The migration must exist before Glue runs. `truncate=true` keeps the column definitions. Do not let Spark create the tables.

## dbt

### Schema naming

`macros/generate_schema_name.sql` uses a custom `+schema` as-is (`silver`, `gold`, `seeds`). Without it, models land in `public_silver` / `public_gold` and cannot see `silver.stg_*`. Do not delete the macro. The `custom_schema_name is none` branch is unused because every model and seed sets `+schema`. `profiles.yml` `schema: public` is only the default namespace.

### Silver (`+materialized: table`, `+schema: silver`)

Sources in `models/silver/src_silver.yml` point at the Spark tables.

- `customers` — `INITCAP` names, cleaned email + `email_status`, city/segment/status via `match_seed`, phone via `normalize_phone`, dates via `format_date`, derived tenure, age, and risk tier.
- `accounts` — `clean_amount` / `to_usd`, `is_currency_mismatch` against `seed_currency_map`, `account_age_months`.
- `transactions` — date parts, `amount_usd`, type via `seed_transaction_type`.
- `fx_rates_current` — **table** on purpose (`to_usd` runs a scalar lookup per row; a view over the tick history would be scanned ~175k times). One row per currency: latest `stream_fx_rates` tick newer than `var('fx_live_max_age_hours', 48)`, else `seed_fx_rates`. The seed drives the currency list: one `CROSS JOIN LATERAL (... ORDER BY UPPER(TRIM(currency)), event_time DESC LIMIT 1)` per seed currency, so each lookup is one probe of `stream_fx_rates_norm_currency_event_time_idx`. Do not go back to `DISTINCT ON`: it reads the whole 48 h window and ignores the index (1.5 s vs 0.19 ms on 2M ticks). Keep `UPPER(TRIM(currency))` and `usd_per_unit > 0` as written; the index matches those expressions. A currency missing from the seed gets no live rate. Columns `rate_source` (`stream`/`seed`), `rate_event_time`, `provider_updated_at`, `seed_usd_per_unit`.
- `live_transactions` — **view** over `source('silver','stream_transactions')`, same cleaning macros as `transactions`. `amount_usd` uses the stream's `fx_usd_per_unit` (rate at event time); when NULL, `to_usd` (fx_rates_current). `fx_rate_source` is `event_window` or `fx_rates_current`. Adds `event_time`, `kafka_ts`, `processed_at`, `latency_seconds`.
- `loans` — USD principals, balances, and payments; `is_delinquent` when status is `delinquent` or `default`; `dpd_bucket`.
- `credit_info` / `digital_engagement` — one row per customer that has the object, extracted with `::jsonb`. `credit_score` outside 350–800 becomes NULL.

**Dirty-data convention**: keep the row, normalize when safe, flag otherwise. `match_seed` is `COALESCE(mapped, original)`. Invalid emails keep the original string and set `email_status = 'invalid'`. Currency mismatches keep the recorded currency. USD is a reporting scale, not a rewrite of the native amount.

`email_status` can return the string `'null'` for a NULL email, but `email` is `not_null` and `accepted_values` are only `valid`/`invalid`. Leave that branch alone unless both tests change together.

### Macros

| Macro | Role |
| --- | --- |
| `format_date` | `TO_DATE` with a `LIKE`-picked mask. Separator `-` means MDY; `/` means DMY. That is intentional. |
| `clean_amount` | `normalize_null`, comma to dot, strip `$` / letters / spaces, `float`. |
| `to_usd` | `amount * fx_rates_current.usd_per_unit` (live tick, or seed fallback). Unknown currency → NULL, not 0. Batch USD values therefore move with the live rate; with `--vars '{fx_live_max_age_hours: 0}'` every currency uses the seed again. |
| `clean_email` / `email_status` | `unaccent` is nested `REPLACE`, not the Postgres extension. No `CREATE EXTENSION`. |
| `normalize_phone` | No leading `+` → prefix `seed_phone_codes.phone_code`; else strip `() -`. |
| `normalize_boolean` | `true/1/yes/y/si` vs `false/0/no/n`. |
| `normalize_null` | `na` / `none` / `null` / `n/a` / empty / `-` → SQL NULL. |
| `match_seed` | Fuzzy lookup; unmatched values pass through. |
| `is_international_currency` | Transaction currency ≠ expected currency for the customer country. |
| `transaction_revenue_amount` | Amount only when `type = fee` and `status = completed`; else 0. That is the definition of revenue. |

Generic tests in `macros/generic_tests/`: `is_positive` fails `<= 0`; `is_non_negative` fails `< 0`. Native amounts use `is_positive`. USD amounts use `is_non_negative`.

### Seeds (`+schema: seeds`)

`seed_city`, `seed_customer_segment`, `seed_status`, `seed_transaction_type`, `seed_phone_codes` (AR/BR/CL/CO/MX/PE/UY), `seed_currency_map` (those countries → COP/ARS/MXN/CLP/PEN/BRL/UYU), `seed_fx_rates`. `.gitignore` ignores `*.csv` except `!backend/src/dbt/seeds/*`.

### Gold (`+schema: gold`)

Money is already USD and drops the `_usd` suffix. Native currency stays on Silver.

- `dim_date` — `generate_series('2000-01-01','2050-12-31')`. `date_id` is `YYYYMMDD`. Postgres `DOW` 0 is Sunday.
- `dim_customer` — customers left join credit info and digital engagement.
- `dim_account` — `is_credit_product` when `account_type = 'credit_card'`, plus denormalized segment and country.
- `fct_transactions` — `date_id` from `transaction_date`, `is_failed`, `is_international`, revenue from the fee-and-completed macro.
- `fct_loans` — `date_id` from **start_date**, `pct_outstanding` capped at 1, `estimated_monthly_interest = outstanding * rate/100 / 12`.
- `mart_customer_summary` — the only Gold model that is a **view** (set in the SQL file, not `dbt_project.yml`).
- `fct_live_transactions` — the only **incremental** model. `delete+insert` on `transaction_id`; the incremental filter takes `processed_at` newer than the table max minus `var('live_lookback_minutes', 10)`, so rows whose FX columns the stream upserted after the last run are rebuilt. Same columns as `fct_transactions` plus `amount_native`, `fx_usd_per_unit`, `fx_rate_source`, `fx_event_time`, `fx_provider_updated_at`, `event_time`, `kafka_ts`, `processed_at`, `latency_seconds`. Does not join `accounts` (not needed for any column). Its `processed_at` index is a `post_hook` (`transaction: False`), not in `indexes`: dbt only builds `indexes` when an incremental table is created.

The dbt project now reads `silver.stream_*`. Those tables come from migration `20261015000000_silver_streaming`; a database without it fails at `fx_rates_current`.

Singular tests added with streaming: `silver_fx_rates_current_covers_seed` (no seed currency lost), `silver_fx_rates_current_sane_vs_seed` (live rate within `var('fx_max_ratio_vs_seed', 10)`× of the seed; catches a `units_per_usd`/`usd_per_unit` inversion), and three on `fct_live_transactions`: FX tick inside `[event_time - var('live_fx_join_seconds', 60) s, event_time]`, `amount = amount_native * fx_usd_per_unit`, and the batch revenue rule.

Most Gold tables declare btree indexes in `{{ config(indexes=[...]) }}`. The mart does not.

Indexes the `indexes` config cannot express (partial, `INCLUDE`) or that an existing incremental table needs are `post_hook`s with a fixed name and `create index if not exists`, and they **must** use `{"sql": "...", "transaction": False}`. Inside the transaction, dbt's `__dbt_backup` copy of the previous table still owns an index with that name, so `if not exists` skips and dropping the backup drops the index: it exists only on every other run. `fct_transactions_revenue_idx` hit exactly this. Measurements and the reasoning per index are in `docs/optimizations/README.md`.

### Tests

Schema tests in `silver_schema.yml` and `models/gold/schema.yml`. Singular tests under `backend/src/dbt/tests/` select the failing rows. A failing test fails `run.sh`, which fails the Fargate task, which fails the state machine. Do not weaken a test to make a run green.

## Power BI

`powerbi/dashboard.pbix` reads schema `gold` on the RDS instance. Treat the `.pbix` as a deliverable. Correctness is dbt tests plus queries against Postgres.

## Documentation convention

`README.md` never references this file. When a human needs a procedure, put it in `README.md`.

## Commit convention

One subject line, Conventional Commits, imperative, no trailing period. English or Spanish both appear in history. Examples:

```text
feat: add gold models in dbt
fix: unificar is_delinquent e indexar Gold
docs: update readme, including data diagrams, pipeline execution and findings in the data
```

Prefer one concern per commit. `CHANGELOG.md` follows Keep a Changelog with `DD/MM/YYYY` dates; add user-visible changes under `[Unreleased]`.

**Never add Claude co-authorship to commits in this repository.**

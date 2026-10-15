# Payload Analytics

An ELT pipeline for a LATAM fintech and banking dataset. A JSON file dropped in S3 is flattened with Spark, cleaned and modeled with dbt on PostgreSQL, and left ready for reporting.

The dataset covers customers, accounts, transactions, loans, credit indicators, and digital engagement. The pipeline keeps the file as it arrived, then builds a cleaned Silver layer and a Gold star schema for reporting.

## Architecture

```text
JSON upload
  → S3 raw/
  → EventBridge
  → Step Functions
  → Glue Spark job          writes silver.stg_*
  → Fargate dbt task        seed, run, test
  → S3 processed/           or failed/ plus an email
```

| Piece | Role |
| --- | --- |
| S3 data lake | Bronze. `raw/` is the inbox, `processed/` is the archive, `failed/` keeps files that did not finish. |
| Glue | Reads one JSON file from S3 and replaces the four `silver.stg_*` tables. |
| RDS PostgreSQL | Warehouse. Silver staging, cleaned Silver, seeds, and Gold. |
| Fargate | Runs `dbt seed`, `dbt run`, and `dbt test` from a container image in ECR. |
| Step Functions | Runs Glue, then dbt, then moves the file. |
| SNS | Emails you when a run fails. |

There is no NAT Gateway. The database is a public `db.t4g.micro` in the default VPC. Glue uses AWS's managed network. The dbt task runs in the default public subnets with a public IP so it can pull its image, read the database secret, and reach PostgreSQL.

Each new file replaces `silver.stg_*` and dbt rebuilds the tables on top. Two uploads close together do not run at once: Glue allows one run, and Step Functions waits and retries.

## Layers

- **Bronze** is the JSON object in S3. Nested accounts, transactions, loans, credit info, and digital engagement stay intact.
- **Silver staging** (`silver.stg_*`) is the flattened Spark output: customers, accounts, transactions, and loans. Values stay strings. Credit info and digital engagement stay JSON text on the customer row.
- **Silver** is the dbt layer: typed amounts, parsed dates, seed lookups, and flags for bad emails and currency mismatches.
- **Gold** is the star schema for reporting: customer, account, and date dimensions, transaction and loan facts, and a customer summary.

![Silver layer](imgs/silver_diagram.jpeg)

![Gold layer](imgs/gold_diagram.jpeg)

## Project layout

```text
payload-analytics/
├── .docker/                         # Image used by both dev containers
├── .envs/                           # AWS credentials and the Serverless key (not committed)
├── infrastructure/                  # S3, RDS, ECR, alerts
│   └── serverless/
│       ├── storage/
│       ├── rds/
│       ├── registry/
│       └── alerts/
├── backend/
│   ├── database/rds/                # SQL migration for silver.stg_*
│   ├── src/spark/                   # Glue job and shared transforms
│   ├── src/dbt/                     # dbt project and its image
│   └── serverless/
│       ├── spark/
│       ├── dbt/
│       └── orchestration/
├── streaming/                       # Local Redpanda and producers (runs on your machine)
│   ├── .docker/
│   └── src/producers/
└── data/raw/                        # Sample JSON used to start a run
```

## Before you start

- Docker Desktop running
- An AWS account, with permission to create S3, RDS, ECR, Glue, ECS, Step Functions, SNS, and IAM
- A Serverless Framework access key

Nothing else is installed on the host. Serverless, the AWS CLI, and Docker (for the dbt image) run inside the dev containers.

## 1. Fill in the environment files

These files are gitignored. Create them if they are missing.

`.envs/aws.env`

```text
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-2
AWS_DEFAULT_OUTPUT=json
```

`.envs/sls.env`

```text
SERVERLESS_ACCESS_KEY=
```

`infrastructure/.envs/config.env` and `backend/.envs/config.env` share the developer name and the VPC. `ALERTS_EMAIL` is only on the infrastructure file. `DEVELOPER` must be lowercase; it is part of the S3 bucket name.

```text
DEVELOPER=
DEFAULT_VPC_ID=
DEFAULT_VPC_SUBNET_IDS=
ALERTS_EMAIL=
```

`DEFAULT_VPC_SUBNET_IDS` is a comma-separated list with no spaces. You can look the default VPC up after the infrastructure container is running:

```bash
aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text
aws ec2 describe-subnets --filters Name=vpc-id,Values=vpc-xxxxxxxx Name=default-for-az,Values=true --query "Subnets[].SubnetId" --output text
```

Paste that VPC id into `DEFAULT_VPC_ID`. Join the subnet ids with commas for `DEFAULT_VPC_SUBNET_IDS`. Put the same three values in the backend config file.

If you change any of these files after a container is already running, recreate that container. Compose only reads env files when the container is created. Recreate it; do not rebuild the image:

```bash
docker compose -f .docker/docker-compose.yml up -d --force-recreate
```

## 2. Start the infrastructure container

From a terminal:

```bash
cd payload-analytics/infrastructure
docker compose -f .docker/docker-compose.yml up -d --build
docker compose -f .docker/docker-compose.yml exec payload_analytics_infrastructure bash -lc "sed -i 's/\r$//' .docker/entrypoint.sh && sudo chmod +x .docker/entrypoint.sh && .docker/entrypoint.sh"
docker compose -f .docker/docker-compose.yml exec payload_analytics_infrastructure bash
```

Check that the VPC values arrived:

```bash
echo "$DEFAULT_VPC_ID"
echo "$DEFAULT_VPC_SUBNET_IDS"
echo "$ALERTS_EMAIL"
```

Then create the stacks, in this order. RDS takes several minutes.

```bash
cd /app/infrastructure/serverless/storage && serverless deploy
cd /app/infrastructure/serverless/rds && serverless deploy
cd /app/infrastructure/serverless/registry && serverless deploy
cd /app/infrastructure/serverless/alerts && serverless deploy
```

Open the SNS confirmation email and confirm the subscription before you rely on failure alerts.

| Stack | What it creates |
| --- | --- |
| storage | Private data lake bucket and artifacts bucket. EventBridge is enabled on the data lake. |
| rds | Public Postgres. Database `payload_analytics`, master user `payload_analytics_admin`. |
| registry | ECR repository for the dbt image. |
| alerts | Email topic and a monthly cost budget. |

## 3. Start the backend container

From a terminal:

```bash
cd payload-analytics/backend
docker compose -f .docker/docker-compose.yml up -d --build
docker compose -f .docker/docker-compose.yml exec payload_analytics_backend bash -lc "sed -i 's/\r$//' .docker/entrypoint.sh && sudo chmod +x .docker/entrypoint.sh && .docker/entrypoint.sh"
docker compose -f .docker/docker-compose.yml exec payload_analytics_backend bash
```

This image includes the Docker CLI and the host Docker socket, which the dbt image build uses. Inside the container:

```bash
cd /app/backend && ./database/rds/migrate.sh
cd /app/backend && ./serverless/spark/deploy.sh
cd /app/backend && ./serverless/dbt/deploy.sh
cd /app/backend/serverless/orchestration && serverless deploy
```

`migrate.sh` creates the `silver` schema and the four `stg_*` tables. Spark later replaces the rows and keeps those definitions.

`serverless/spark/deploy.sh` uploads the Glue script, the Postgres JDBC jar, and the shared Python, then creates the Glue job.

`serverless/dbt/deploy.sh` builds the dbt image, pushes it to ECR tagged with the current git commit, and creates the Fargate task for that tag. This step is the slow one.

`serverless/orchestration` is the state machine. It has to go last, because it reads the bucket, the Glue job, and the task definition created above. If you build a new dbt image later, run `serverless/dbt/deploy.sh` again and then deploy orchestration again so the state machine points at the new task.

## 4. Run the pipeline

Do this from the backend container, after the infrastructure stacks, the migration, Spark, dbt, and orchestration have all deployed. Confirm the SNS subscription first if you want a failure email.

The bucket name is in SSM. Copy the sample file into `raw/`:

```bash
BUCKET=$(aws ssm get-parameter --name /payload-analytics-storage/DataLakeBucketName --query Parameter.Value --output text)
aws s3 cp /app/data/raw/fintech_banking_dataset.json "s3://${BUCKET}/raw/"
```

That upload is the trigger. EventBridge sees the new object under `raw/` and starts Step Functions. Nothing else has to be invoked by hand.

In the AWS console, switch to the same region as `AWS_DEFAULT_REGION`. Open **Step Functions → State machines → `payload-analytics-backend-orchestration` → Executions**. The new execution is the run for that file. Its graph is Glue (`RunSparkJob`), then the dbt task on Fargate (`RunDbt`), then the copy that archives the object.

| Result | Where the file goes | What you have |
| --- | --- | --- |
| Execution **Succeeded** | `processed/` | `silver.stg_*` replaced, dbt tests passed, Gold rebuilt |
| Execution **Failed** | `failed/` | SNS email, if the subscription was confirmed |

Check the prefixes from the same container:

```bash
aws s3 ls "s3://${BUCKET}/processed/"
aws s3 ls "s3://${BUCKET}/failed/"
```

`raw/` should be empty for that key after the execution finishes. A later upload of the same name runs the pipeline again and replaces `silver.stg_*`; it does not append.

## 5. Connect to RDS

The instance is public on port `5432`. Use any PostgreSQL client. Connect to the database `payload_analytics`. The default database `postgres` only has an empty `public` schema; the warehouse is not there.

Look up the host and the password from the backend container. Do not paste them into the repo.

```bash
aws ssm get-parameter --name /payload-analytics-rds/DBInstanceEndpoint --query Parameter.Value --output text
aws secretsmanager get-secret-value \
  --secret-id /payload-analytics-rds/MasterCredentials \
  --query SecretString --output text
```

The second command prints a JSON object. Use its `password` field.

| Setting | Value |
| --- | --- |
| Host | SSM `/payload-analytics-rds/DBInstanceEndpoint` |
| Port | `5432` |
| Database | `payload_analytics` |
| User | `payload_analytics_admin` |
| Password | `password` in secret `/payload-analytics-rds/MasterCredentials` |

| Schema | What is in it |
| --- | --- |
| `silver` | `stg_*` from Spark, then the cleaned dbt models |
| `seeds` | Lookup tables loaded by `dbt seed` |
| `gold` | Dimensions, facts, and the customer summary used for reporting |

From the backend container:

```bash
ENDPOINT=$(aws ssm get-parameter --name /payload-analytics-rds/DBInstanceEndpoint --query Parameter.Value --output text)
psql "host=${ENDPOINT} port=5432 dbname=payload_analytics user=payload_analytics_admin sslmode=require"
```

`psql` asks for the password. Then:

```sql
\dn
\dt silver.*
\dt gold.*
```

RDS lists its PostgreSQL output under log files named `error/postgresql.log`. That name is the log path. It does not mean the pipeline failed. Use the Step Functions execution status for that.

## 6. Run the streaming producers

The streaming side runs on your machine with Docker, not in AWS. Nothing here is deployed with Serverless, so it adds no AWS cost.

| Service | Role |
| --- | --- |
| `redpanda` | Kafka-compatible broker. Inside Docker it is `redpanda:9092`; from the host it is `localhost:19092`. |
| `redpanda_init` | Creates the topics `fx.rates` and `fintech.live_transactions` (3 partitions each), then exits. |
| `console` | Redpanda Console at http://localhost:8088 to browse topics and messages. |
| `fx_producer` | Publishes USD exchange rates for the nine currencies to `fx.rates`, keyed by currency. |
| `transactions_simulator` | Publishes live transactions to `fintech.live_transactions`, keyed by customer. |

Create `streaming/.envs/config.env` (gitignored). Every value has a default, so an empty file also works:

```text
FX_API_URL=https://open.er-api.com/v6/latest/USD
FX_CURRENCIES=USD,EUR,COP,ARS,MXN,CLP,PEN,BRL,UYU
FX_FETCH_SECONDS=600
FX_PUBLISH_SECONDS=10
TX_EVENTS_PER_SECOND=2
TX_SEED=
```

Start it from the `streaming` folder:

```bash
cd payload-analytics/streaming
docker compose -f .docker/docker-compose.yml up -d --build
```

Check that events are flowing:

```bash
docker exec PayloadAnalyticsRedpanda rpk topic consume fx.rates -n 3
docker exec PayloadAnalyticsRedpanda rpk topic consume fintech.live_transactions -n 3
```

Stop it with `docker compose -f .docker/docker-compose.yml stop`. The producers flush pending events before they exit. `down -v` also deletes the topic data.

**FX rates.** The free provider refreshes its rates about once a day. The producer calls the API every `FX_FETCH_SECONDS` and republishes the last rates every `FX_PUBLISH_SECONDS`, so the stream always has a recent tick. `provider_updated_at` is the provider's own timestamp, the real age of the rate; `event_time` is when the tick was published. `usd_per_unit` has the same meaning as in `seed_fx_rates`. If the API fails, the producer keeps the last good rates and retries with backoff. It never makes up a rate.

**Live transactions.** Customers and accounts come from `data/raw/fintech_banking_dataset.json`, so every event points to a customer and account that exist in Silver and Gold. Amount, type, category, merchant, channel, status, and description are copied from a real transaction in the same currency, dirt included. The date is today in one of the four source formats, with the source's frequencies. Live ids start with `LTX-`, batch ids with `TXN-`. All fields are strings, the same contract as `silver.stg_transactions`.

## What the models do

Spark only explodes and deduplicates. dbt does the cleaning.

| Staging table | How it is built | Dedupe key |
| --- | --- | --- |
| `silver.stg_customers` | Drop the arrays. Credit info and digital engagement become JSON text. | `customer_id` |
| `silver.stg_accounts` | Explode `accounts`. | `account_id` |
| `silver.stg_transactions` | Explode `transactions`. | `transaction_id` |
| `silver.stg_loans` | Explode `loans`. | `loan_id` |

Silver models then standardize names, emails, phones, dates, amounts, and categories. Gold keeps money in USD and drops the `_usd` suffix. Native currency stays on Silver.

Seeds under `backend/src/dbt/seeds/` map cities, segments, statuses, transaction types, phone codes, expected currency by country, and static FX rates.

## Data quality

The source is dirty on purpose. The pipeline keeps the row, normalizes what it can, and flags the rest.

- **Emails.** Invalid addresses stay in the column and `email_status` is `invalid`.
- **Dates.** `YYYY-MM-DD`, `YYYYMMDD`, `DD/MM/YYYY`, and `MM-DD-YYYY` are parsed. The separator chooses day-first versus month-first.
- **Currency.** A Colombian customer can still have a USD account. The recorded currency stays, a mismatch flag is set, and amounts are also converted to USD.
- **Amounts.** Symbols, letters, and mixed decimal separators are stripped. The original currency column is unchanged.
- **Labels.** Spanish values and typos map through seeds. Unknown values pass through instead of becoming null.
- **Duplicates inside one file.** The newest `load_timestamp` wins for each business key. A later file replaces the staging tables entirely.

## Business assumptions

- Keep a row when the value can be normalized.
- Flag suspicious values instead of deleting them.
- Leave the original file in S3.
- Report cross-country money in USD.
- Revenue is a completed fee. Anything else is zero.

## Quality checks

The project includes dbt tests for:

- primary key uniqueness
- not-null constraints
- relationships between fact and dimension tables
- accepted values and valid ranges
- transactional date integrity
- customer and revenue consistency rules

These tests validate both the warehouse structure and the business logic used in reporting.

## Typical use cases

This dataset and pipeline are appropriate for:

- customer portfolio analysis
- transaction trend monitoring
- revenue and fee analysis
- account and product mix reporting
- delinquency and risk screening
- international payment and FX exposure analysis

## Evidence

![Evidence](imgs/evidence.png)


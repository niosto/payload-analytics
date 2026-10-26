# Backend

Spark job, dbt image, database migration, and the Step Functions pipeline. Run these from the backend devcontainer, after the infrastructure stacks in [infrastructure/README.md](../infrastructure/README.md).

`AWS_DEFAULT_REGION` must match the infrastructure deploy. `DEFAULT_VPC_ID` and `DEFAULT_VPC_SUBNET_IDS` are the same default-VPC values used for RDS; the Fargate task uses those subnets and a public IP.

The repo root is mounted at `/app`. The working directory is `/app/backend`. The sample file is `/app/data/raw/fintech_banking_dataset.json`.

```bash
cd /app/backend && ./database/rds/migrate.sh
cd /app/backend && ./serverless/spark/deploy.sh
cd /app/backend && ./serverless/dbt/deploy.sh
cd /app/backend/serverless/orchestration && serverless deploy
```

`migrate.sh` creates `silver` and the four `stg_*` tables. It reads the RDS master secret `/${DEPLOY_APP}-rds/MasterCredentials`. Pass `--downgrade` to roll the migration back.

`serverless/spark/deploy.sh` uploads `glue_job.py`, the Postgres JDBC jar, and a zip of `transformations.py` plus `schemas/`, then deploys the Glue job.

`serverless/dbt/deploy.sh` builds `src/dbt`, pushes the image to ECR tagged with the current git SHA, and deploys the Fargate task definition for that tag. This step is the slow one. Run it again after dbt changes. If Step Functions should start that new revision, deploy orchestration again afterwards.

`serverless/orchestration` is the state machine. It has to go last, because it reads the bucket, the Glue job, and the task definition created above.

## Run the pipeline

Confirm the SNS subscription first if you want a failure email. Copy the sample file into `raw/`. The bucket name is in SSM.

```bash
BUCKET=$(aws ssm get-parameter --name /${DEPLOY_APP}-storage/DataLakeBucketName --query Parameter.Value --output text)
aws s3 cp /app/data/raw/fintech_banking_dataset.json "s3://${BUCKET}/raw/"
```

That upload is the trigger. EventBridge sees the new object under `raw/` and starts Step Functions. Nothing else has to be invoked by hand.

In the AWS console, switch to `AWS_DEFAULT_REGION`. Open **Step Functions → State machines → `${DEPLOY_APP}-backend-orchestration` → Executions**. The new execution is the run for that file. Its graph is Glue (`RunSparkJob`), then the dbt task on Fargate (`RunDbt`), then the copy that archives the object.

| Result | Where the file goes | What you have |
| --- | --- | --- |
| Execution **Succeeded** | `processed/` | `silver.stg_*` replaced, dbt tests passed, Gold rebuilt |
| Execution **Failed** | `failed/` | SNS email, if the subscription was confirmed |

```bash
aws s3 ls "s3://${BUCKET}/processed/"
aws s3 ls "s3://${BUCKET}/failed/"
```

`raw/` should be empty for that key after the execution finishes. A later upload of the same name runs the pipeline again and replaces `silver.stg_*`; it does not append.

## Fault files

`/app/data/faults/` holds small sources that are not the sample. Each one replaces `silver.stg_*` and dbt rebuilds Gold from that file alone. Run them one at a time, and only when the current warehouse can be discarded. The same Step Functions execution tells you the result: `processed/` or `failed/`.

```bash
BUCKET=$(aws ssm get-parameter --name /${DEPLOY_APP}-storage/DataLakeBucketName --query Parameter.Value --output text)
aws s3 cp /app/data/faults/fault_malformed.json "s3://${BUCKET}/raw/"
aws s3 cp /app/data/faults/fault_bad_keys.json "s3://${BUCKET}/raw/"
aws s3 cp /app/data/faults/fault_data_quality.json "s3://${BUCKET}/raw/"
aws s3 cp /app/data/faults/fault_data_quality_rejected.json "s3://${BUCKET}/raw/"
```

Wait for the execution to finish before copying the next file. Two objects under `raw/` at once do not run together: Glue allows one run, and Step Functions waits and retries.

| File | What is wrong | Expected result |
| --- | --- | --- |
| `fault_malformed.json` | Not valid JSON | Glue fails. Object moves to `failed/`. |
| `fault_bad_keys.json` | Root object and field names that are not in the schema | The row lands with a null `customer_id`. dbt `not_null` fails. Object moves to `failed/`. |
| `fault_data_quality.json` | Invalid email, symbols in the amount, Spanish labels, a USD account on a CO customer, credit score 900, duplicate `customer_id` | Execution succeeds. The row is kept and flagged. Spark keeps one of the two duplicate customers. |
| `fault_data_quality_rejected.json` | Null name and email, age 11, risk score 250, unknown status, negative amount, `lat` set to `"no"` | dbt errors or a test fails. Object moves to `failed/`. |

Reload the sample when you want the full warehouse back:

```bash
aws s3 cp /app/data/raw/fintech_banking_dataset.json "s3://${BUCKET}/raw/"
```

## Connect to RDS

The instance is public on port `5432`. Use any PostgreSQL client. Connect to the database `payload_analytics`. The default database `postgres` only has an empty `public` schema; the warehouse is not there.

```bash
aws ssm get-parameter --name /${DEPLOY_APP}-rds/DBInstanceEndpoint --query Parameter.Value --output text
aws secretsmanager get-secret-value \
  --secret-id /${DEPLOY_APP}-rds/MasterCredentials \
  --query SecretString --output text
```

The second command prints a JSON object. Use its `password` field. Do not paste the host or the password into the repo.

| Setting | Value |
| --- | --- |
| Host | SSM `/${DEPLOY_APP}-rds/DBInstanceEndpoint` |
| Port | `5432` |
| Database | `payload_analytics` |
| User | `payload_analytics_admin` |
| Password | `password` in secret `/${DEPLOY_APP}-rds/MasterCredentials` |

| Schema | What is in it |
| --- | --- |
| `silver` | `stg_*` from Spark, then the cleaned dbt models |
| `seeds` | Lookup tables loaded by `dbt seed` |
| `gold` | Dimensions, facts, and the customer summary used for reporting |

```bash
ENDPOINT=$(aws ssm get-parameter --name /${DEPLOY_APP}-rds/DBInstanceEndpoint --query Parameter.Value --output text)
psql "host=${ENDPOINT} port=5432 dbname=payload_analytics user=payload_analytics_admin sslmode=require"
```

`psql` asks for the password. Then:

```sql
\dn
\dt silver.*
\dt gold.*
```

RDS lists its PostgreSQL output under log files named `error/postgresql.log`. That name is the log path. It does not mean the pipeline failed. Use the Step Functions execution status for that.

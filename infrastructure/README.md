# Infrastructure

Serverless stacks for the payload-analytics data lake, database, image registry, and alerts. Run these from the infrastructure devcontainer, after `.envs/aws.env`, `.envs/sls.env`, and `infrastructure/.envs/config.env` are filled in.

`AWS_DEFAULT_REGION` selects the region. `DEVELOPER` is part of the dev-stage bucket names, so keep it lowercase. `DEFAULT_VPC_ID` and `DEFAULT_VPC_SUBNET_IDS` (comma-separated, no spaces) place the public RDS instance. `ALERTS_EMAIL` receives the SNS subscription and the budget notices.

Copy `DEVELOPER`, `DEFAULT_VPC_ID`, and `DEFAULT_VPC_SUBNET_IDS` into `backend/.envs/config.env`. Leave `ALERTS_EMAIL` only on the infrastructure file.

Look up the default VPC from this container, then paste the id into `DEFAULT_VPC_ID` and join the subnet ids with commas:

```bash
aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text
aws ec2 describe-subnets --filters Name=vpc-id,Values=vpc-xxxxxxxx Name=default-for-az,Values=true --query "Subnets[].SubnetId" --output text
```

Compose reads env files only when the container is created. After an edit, recreate it; do not rebuild the image:

```bash
docker compose -f .docker/docker-compose.yml up -d --force-recreate
```

Check the values, then deploy in this order. RDS takes several minutes.

```bash
echo "$DEFAULT_VPC_ID"
echo "$DEFAULT_VPC_SUBNET_IDS"
echo "$ALERTS_EMAIL"

cd /app/infrastructure/serverless/storage && serverless deploy
cd /app/infrastructure/serverless/rds && serverless deploy
cd /app/infrastructure/serverless/registry && serverless deploy
cd /app/infrastructure/serverless/alerts && serverless deploy
```

Confirm the SNS subscription email before relying on failure alerts.

| Stack | What it creates | SSM prefix |
| --- | --- | --- |
| storage | Private data lake bucket (`raw/`, `processed/`, `failed/`, EventBridge on) and artifacts bucket | `/${DEPLOY_APP}-storage/` |
| rds | Public `db.t4g.micro` Postgres, master user `payload_analytics_admin`, database `payload_analytics` | `/${DEPLOY_APP}-rds/` |
| registry | ECR repository for the dbt image | `/${DEPLOY_APP}-registry/` |
| alerts | Email topic and a monthly cost budget | `/${DEPLOY_APP}-alerts/` |

The database listens on port `5432`. Its host is the SSM parameter `/${DEPLOY_APP}-rds/DBInstanceEndpoint`. The password is the `password` field of the secret `/${DEPLOY_APP}-rds/MasterCredentials`. Clients must use the database `payload_analytics`. The default database `postgres` stays empty. Schemas `silver`, `seeds`, and `gold` appear after the backend pipeline runs.

The backend devcontainer deploys Glue, the dbt task, and Step Functions, then uploads a file to `raw/` to start a run. See [backend/README.md](../backend/README.md).

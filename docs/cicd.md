# CI/CD

One GitHub Actions workflow, [`.github/workflows/ci.yml`](../.github/workflows/ci.yml), with two jobs.

| Trigger | `test` | `deploy` |
| --- | --- | --- |
| Pull request to `main` | yes | no |
| Push to `main` (a merged PR) | yes | yes, after `test` passes |
| Manual run (`workflow_dispatch`) on `main` | yes | yes, after `test` passes |

Pull requests never get AWS credentials. Only `main` can deploy.

## `test`

Runs on `ubuntu-latest` with Python 3.11, Java 17, and a `postgres:16` service container. No AWS, no Kafka, no network calls from the tests.

1. **Unit tests** (`pytest`, `tests/`):
   - `streaming/src/producers/fx_market_producer.py`: `parse_symbols`, `to_rates`, `ms_to_iso`, `MarketState.update` / `due` (coalescing and heartbeat).
   - `streaming/src/producers/transactions_simulator.py`: `as_text`, `load_source`, `build_event` (event contract = `stg_transactions` fields + `event_id`, `event_time`, `source`; `LTX-` ids; template from the account's currency).
   - `backend/src/spark/transformations.py`: `build_silver_frames` on `tests/fixtures/tiny_customers.json` with local PySpark 3.5 (row counts after explode and dedupe, every column a string, columns equal to the `silver.stg_*` columns in the migration, structs as JSON text) and `deduplicate`.
   - `confluent_kafka` and `websockets` are stubbed in `tests/conftest.py` when they are not installed.
2. **dbt integration test** (`scripts/ci/dbt_integration.sh`):
   1. `yoyo apply` of `backend/database/rds/migrations` (the same migrations `migrate.sh` applies to RDS).
   2. `scripts/ci/load_fixture.py` reads `tests/fixtures/fintech_banking_sample.json` (the first 300 customers of `data/raw/fintech_banking_dataset.json`, one of them duplicated) with the Glue schema, runs the real `build_silver_frames`, and writes `silver.stg_*` through the JDBC jar `backend/src/spark/jars/postgresql-42.7.3.jar` with the Glue job's options (`overwrite` + `truncate`).
   3. It inserts one fresh tick per currency of `seed_fx_rates.csv` into `silver.stream_fx_rates` (2% off the seed) and six live transactions on fixture accounts into `silver.stream_transactions` (`processed_at` after `event_time`, joined tick inside the 60 s window; the last one has no tick, so it takes the `fx_rates_current` fallback).
   4. `dbt seed`, `dbt run`, `dbt test` with `backend/src/dbt/profiles.yml` (`DBT_*` variables), then `dbt run -s fct_live_transactions` and its tests again so the incremental branch runs too.

   Every dbt test runs; none is skipped or relaxed. Result on the fixture: seed 7, run 15, test 206 PASS, 0 ERROR.

Run it locally against a throwaway database (Java 17 needed for Spark):

```bash
docker run -d --name pa-ci-pg -p 5432:5432 -e POSTGRES_USER=payload_analytics_admin \
  -e POSTGRES_PASSWORD=ci -e POSTGRES_DB=payload_analytics postgres:16
pip install -r requirements-dev.txt
pytest
DBT_HOST=localhost DBT_PASSWORD=ci bash scripts/ci/dbt_integration.sh
docker rm -f pa-ci-pg
```

## `deploy`

Runs only on `main`, after `test`, with `concurrency: deploy-aws-backend` (`cancel-in-progress: false`): two deploys never overlap and a running deploy is never cancelled. If several merges queue up, GitHub keeps only the newest pending run, which deploys the newest commit.

Environment (hardcoded in the workflow; none of it is secret): `DEPLOY_APP=payload-analytics`, `DEVELOPER=nico11` (the owner of the shared account's stacks; another value would try to replace their buckets), `AWS_DEFAULT_REGION=us-east-2`, the default VPC and subnets, and the deploy role ARN.

Steps, in the README's order:

1. `aws-actions/configure-aws-credentials` assumes the deploy role through OIDC (1 h session).
2. Reads the Serverless key from Secrets Manager into `SERVERLESS_ACCESS_KEY`, masked with `::add-mask::`.
3. `backend/database/rds/migrate.sh` (yoyo against RDS; already-applied migrations are skipped).
4. `backend/serverless/spark/deploy.sh`, only when `backend/src/spark/**` or `backend/serverless/spark/**` changed between `github.event.before` and the pushed commit (always on a manual run).
5. Builds `backend/src/dbt` and pushes it to ECR as `<DbtRepositoryUri>:<github.sha>`.
6. `serverless deploy --param="imageTag=${GITHUB_SHA}"` in `backend/serverless/dbt` (new task definition revision).
7. `serverless deploy` in `backend/serverless/orchestration`, which reads the new task definition ARN.

The job summary shows the image and the task definition ARN that were deployed.

## AWS side: stack `payload-analytics-cicd`

`infrastructure/serverless/cicd/`, same conventions as the other stacks. Deployed once from a machine with admin credentials:

```bash
cd infrastructure && npm install
cd serverless/cicd && serverless deploy      # DEVELOPER=nico11 DEPLOY_APP=payload-analytics
aws secretsmanager put-secret-value --secret-id /payload-analytics-cicd/ServerlessAccessKey \
  --secret-string "$SERVERLESS_ACCESS_KEY"   # from .envs/sls.env
```

| Resource | What it is |
| --- | --- |
| `GitHubOidcProvider` | IAM OIDC provider for `token.actions.githubusercontent.com`, audience `sts.amazonaws.com`. The account had none. |
| `GitHubDeployRole` | `payload-analytics-cicd-github-deploy`. Trust: `sts:AssumeRoleWithWebIdentity` only when `aud = sts.amazonaws.com` and `sub = repo:niosto/payload-analytics:ref:refs/heads/main`. Other repos, branches, forks, and pull requests cannot assume it. |
| `ServerlessAccessKeySecret` | `/payload-analytics-cicd/ServerlessAccessKey`. Created empty by the template; the value is put once with the CLI, so it is never in git or in the template. To rotate, run `put-secret-value` again. |
| SSM | `/payload-analytics-cicd/DeployRoleArn`, `/GitHubOidcProviderArn`, `/ServerlessAccessKeySecretName`. |

No IAM users and no access keys: GitHub holds no AWS secret at all (the repo's collaborators cannot create GitHub secrets anyway).

### What the role can do (and the trade-off)

Scoped by name prefix rather than by exact ARN, so the stacks can add resources without editing this policy:

- CloudFormation on `payload-analytics-backend-*` stacks only (not `storage`, `rds`, `registry`, `alerts`, `monitoring`, or `cicd`).
- S3: the backend Serverless deployment buckets, and read/write on the artifacts bucket (Glue code). No access to the data lake.
- IAM on roles `payload-analytics-backend-*` only (create/update/inline policies/`PassRole`); managed policy attachment limited to `AmazonECSTaskExecutionRolePolicy` and `AWSGlueServiceRole`. The role cannot modify itself.
- Glue jobs, Step Functions state machines, EventBridge rules, ECS clusters with the `payload-analytics-` prefix; ECS task definition calls on `*` (those APIs have no resource-level permissions); EC2 security group calls on `*` (the dbt task's group); log groups `/ecs/payload-analytics-*`.
- SSM read/write on `/payload-analytics-*`; Secrets Manager read of the RDS master secret and the Serverless key; ECR push to `payload-analytics-dbt-*`.

The trade-off: anyone who can merge to `main` can change what the backend stacks deploy, and a backend role's inline policy is not constrained, so a malicious change merged to `main` could grant a backend role broad permissions. Branch protection and review on `main` are the control. Tightening further (exact ARNs, a CloudFormation service role, permission boundaries on the backend roles) was left out for time.

## When it fails

- **`test` red on a PR**: the PR shows the failed check. Open the run; `pytest -v` prints the failing assertion; dbt prints `Completed with N errors` and the failing test name, and the `dbt-logs` artifact has `dbt.log` and `run_results.json`. A failing dbt test means the change breaks a data rule: fix the model or the data, do not weaken the test.
- **`deploy` red on `main`**: the step name says where (migration, Glue, image push, `backend-dbt`, `backend-orchestration`). CloudFormation rolls a failed stack update back to the previous version, so the pipeline keeps running the last good task definition. An `AccessDenied` in the log names the missing IAM action; add it to `infrastructure/serverless/cicd/iam.yml` and redeploy that stack from an admin machine. Re-run with **Re-run jobs** or **Run workflow** on `main`.
- If `backend-dbt` deployed but `backend-orchestration` failed, the state machine still points at the previous task definition revision, which CloudFormation has deregistered, so `RunDbt` can fail until orchestration deploys. Re-run the job (every step is idempotent) or run `serverless deploy` in `backend/serverless/orchestration`.

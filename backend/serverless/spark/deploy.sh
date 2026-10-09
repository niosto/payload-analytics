#!/bin/bash
# Uploads the Glue script, JDBC jar, and shared Python, then deploys the stack.
# Run inside the backend devcontainer, after infrastructure storage and rds:
#   cd /app/backend && ./serverless/spark/deploy.sh
set -euo pipefail

EXEC_PATH=$(dirname "$(readlink -f "$0")")
REPO_ROOT=$(cd "$EXEC_PATH/../../.." && pwd)
SPARK_SRC="$REPO_ROOT/backend/src/spark"

ARTIFACTS_BUCKET=$(aws ssm get-parameter \
  --name "/${DEPLOY_APP}-storage/ArtifactsBucketName" \
  --query Parameter.Value --output text)

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT
(
  cd "$SPARK_SRC"
  zip -r "$WORKDIR/spark_libs.zip" transformations.py schemas \
    -x '*__pycache__*' '*.pyc'
)

aws s3 cp "$SPARK_SRC/glue_job.py" "s3://${ARTIFACTS_BUCKET}/glue/scripts/glue_job.py"
aws s3 cp "$SPARK_SRC/jars/postgresql-42.7.3.jar" \
  "s3://${ARTIFACTS_BUCKET}/glue/jars/postgresql-42.7.3.jar"
aws s3 cp "$WORKDIR/spark_libs.zip" "s3://${ARTIFACTS_BUCKET}/glue/libs/spark_libs.zip"

cd "$EXEC_PATH"
serverless deploy

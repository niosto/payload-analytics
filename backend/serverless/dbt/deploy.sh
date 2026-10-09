#!/bin/bash
# Builds the dbt image, pushes it to ECR tagged with the git SHA, then deploys
# the Fargate task definition for that tag.
# Run inside the backend devcontainer, after infrastructure registry and rds:
#   cd /app/backend && ./serverless/dbt/deploy.sh
set -euo pipefail

EXEC_PATH=$(dirname "$(readlink -f "$0")")
REPO_ROOT=$(cd "$EXEC_PATH/../../.." && pwd)

REPO_URI=$(aws ssm get-parameter \
  --name "/${DEPLOY_APP}-registry/DbtRepositoryUri" \
  --query Parameter.Value --output text)
IMAGE_TAG=$(git -C "$REPO_ROOT" rev-parse HEAD)
IMAGE="${REPO_URI}:${IMAGE_TAG}"

AWS_REGION="${AWS_DEFAULT_REGION:?AWS_DEFAULT_REGION is required}"
REGISTRY="${REPO_URI%%/*}"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

docker build -t "$IMAGE" \
  -f "$REPO_ROOT/backend/src/dbt/Dockerfile" \
  "$REPO_ROOT/backend/src/dbt"
docker push "$IMAGE"

cd "$EXEC_PATH"
serverless deploy --param="imageTag=${IMAGE_TAG}"

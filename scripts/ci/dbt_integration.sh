#!/bin/bash
# dbt integration test against a throwaway Postgres:
#   yoyo migrations -> fixture through build_silver_frames (local Spark + JDBC)
#   -> streaming rows -> dbt seed, run, test -> incremental run + test again.
#
# Uses the DBT_* variables of backend/src/dbt/profiles.yml. In CI the database
# is a postgres:16 service container. Locally:
#   docker run -d --name pa-ci-pg -p 5432:5432 -e POSTGRES_USER=payload_analytics_admin \
#     -e POSTGRES_PASSWORD=ci -e POSTGRES_DB=payload_analytics postgres:16
#   pip install -r requirements-dev.txt   # needs Java 17 for Spark
#   DBT_HOST=localhost DBT_PASSWORD=ci scripts/ci/dbt_integration.sh
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

export DBT_HOST="${DBT_HOST:-localhost}"
export DBT_PORT="${DBT_PORT:-5432}"
export DBT_DBNAME="${DBT_DBNAME:-payload_analytics}"
export DBT_USER="${DBT_USER:-payload_analytics_admin}"
export DBT_PASSWORD="${DBT_PASSWORD:?DBT_PASSWORD is required}"
export DBT_THREADS="${DBT_THREADS:-4}"
export DBT_PROFILES_DIR="$REPO_ROOT/backend/src/dbt"

echo "::group::Apply yoyo migrations"
DB_PASSWORD_URI=$(python -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$DBT_PASSWORD")
yoyo apply \
  --config "$REPO_ROOT/backend/database/rds/yoyo.ini" \
  --database "postgresql://${DBT_USER}:${DB_PASSWORD_URI}@${DBT_HOST}:${DBT_PORT}/${DBT_DBNAME}" \
  --batch \
  "$REPO_ROOT/backend/database/rds/migrations"
echo "::endgroup::"

echo "::group::Load fixture (Spark build_silver_frames + streaming rows)"
python "$REPO_ROOT/scripts/ci/load_fixture.py"
echo "::endgroup::"

cd "$REPO_ROOT/backend/src/dbt"

echo "::group::dbt debug"
dbt debug
echo "::endgroup::"

# Same sequence as run.sh in the Fargate task.
dbt seed
dbt run
dbt test

# Second pass: fct_live_transactions takes its incremental branch.
dbt run -s fct_live_transactions
dbt test -s fct_live_transactions

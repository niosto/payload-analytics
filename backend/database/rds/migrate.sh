#!/bin/bash
# Applies (or reverts) the RDS PostgreSQL schema migrations in ./migrations,
# tracked by yoyo-migrations in its own bookkeeping table so re-running this
# is always safe -- already-applied migrations are skipped automatically.
#
# Run inside the backend devcontainer:
#   cd /app/backend && ./database/rds/migrate.sh                                    # upgrade all pending
#   cd /app/backend && ./database/rds/migrate.sh --downgrade                        # revert ALL applied migrations
#   cd /app/backend && ./database/rds/migrate.sh --downgrade 20260930000000_silver_staging  # revert only that migration
set -e

EXEC_PATH=$(dirname "$(readlink -f "$0")")

YOYO_ACTION='apply'
if [ "$1" == '-d' ] || [ "$1" == '--downgrade' ]; then
  YOYO_ACTION='rollback'
fi

REVISION_FLAGS=()
if [ -n "$2" ]; then
  REVISION_FLAGS=('--revision' "$2")
fi

SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "/${DEPLOY_APP}-rds/MasterCredentials" \
  --query SecretString --output text)

DB_HOST=$(echo "$SECRET_JSON" | jq -r '.host')
DB_PORT=$(echo "$SECRET_JSON" | jq -r '.port')
DB_DBNAME=$(echo "$SECRET_JSON" | jq -r '.dbname')
DB_USERNAME=$(echo "$SECRET_JSON" | jq -r '.username')
# command substitution already strips the trailing newline jq's -r adds, so
# this URL-encodes the password as-is (no xargs word-splitting pitfalls for
# passwords containing spaces).
DB_PASSWORD_RAW=$(echo "$SECRET_JSON" | jq -r '.password')
DB_PASSWORD=$(printf '%s' "$DB_PASSWORD_RAW" | jq -sRr @uri)

yoyo "$YOYO_ACTION" \
  --config "$EXEC_PATH/yoyo.ini" \
  --database "postgresql://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_DBNAME}" \
  --batch \
  "${REVISION_FLAGS[@]}" \
  "$EXEC_PATH/migrations"

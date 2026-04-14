#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
WORKSPACE_DIR="$(CDPATH= cd -- "$PROJECT_DIR/.." && pwd)"
BIN_DIR="$WORKSPACE_DIR/.mamba/bin"
PSQL="$BIN_DIR/psql"

if [ "$("$PSQL" -h localhost -p 5432 -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'sales_db'")" != "1" ]; then
    "$BIN_DIR/createdb" -h localhost -p 5432 -U postgres sales_db
fi

"$PSQL" -h localhost -p 5432 -U postgres -d sales_db -f "$PROJECT_DIR/schema.sql"

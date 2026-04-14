#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
WORKSPACE_DIR="$(CDPATH= cd -- "$PROJECT_DIR/.." && pwd)"
BIN_DIR="$WORKSPACE_DIR/.mamba/bin"
STATE_DIR="$PROJECT_DIR/.postgres"
DATA_DIR="$STATE_DIR/data"
LOG_FILE="$STATE_DIR/postgres.log"
PG_ISREADY="$BIN_DIR/pg_isready"

mkdir -p "$STATE_DIR"

if "$PG_ISREADY" -h localhost -p 5432 >/dev/null 2>&1; then
    echo "PostgreSQL is already available on localhost:5432"
    exit 0
fi

if [ ! -f "$DATA_DIR/PG_VERSION" ]; then
    "$BIN_DIR/initdb" -D "$DATA_DIR" --username=postgres --auth=trust >/dev/null
fi

"$BIN_DIR/pg_ctl" -D "$DATA_DIR" -l "$LOG_FILE" -o "-p 5432" start

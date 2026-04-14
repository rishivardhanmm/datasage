#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
WORKSPACE_DIR="$(CDPATH= cd -- "$PROJECT_DIR/.." && pwd)"
BIN_DIR="$WORKSPACE_DIR/.mamba/bin"
DATA_DIR="$PROJECT_DIR/.postgres/data"

"$BIN_DIR/pg_ctl" -D "$DATA_DIR" stop


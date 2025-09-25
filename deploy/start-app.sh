#!/bin/sh
set -e

# 如果系统有 bash 就用 bash，否则用 sh
if command -v bash >/dev/null 2>&1; then
    SHELL_EXEC=bash
else
    SHELL_EXEC=sh
fi

echo "[start-app.sh] Using shell: $SHELL_EXEC"

if [ $# -eq 0 ]; then
    echo "[start-app.sh] No command provided, exiting."
    exit 1
fi

exec "$SHELL_EXEC" -c "$*"

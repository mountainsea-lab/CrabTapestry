#!/bin/bash
set -e

# ===============================
# 启动服务
# ===============================
if [ $# -eq 0 ]; then
  echo "[start-app.sh] No command provided, exiting."
  exit 1
fi

echo "[start-app.sh] Starting service: $*"
exec "$@"

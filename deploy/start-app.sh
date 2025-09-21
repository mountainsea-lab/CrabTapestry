#!/bin/bash
set -e

# ===============================
# 可选等待 MySQL / Redis 就绪
# ===============================
WAIT_FOR_MYSQL=${WAIT_FOR_MYSQL:-no}
WAIT_FOR_REDIS=${WAIT_FOR_REDIS:-no}
RETRY_INTERVAL=${RETRY_INTERVAL:-2}

function wait_for_mysql() {
  echo "[start-app.sh] Waiting for MySQL at $MYSQL_HOST:$MYSQL_PORT ..."
  until mysqladmin ping -h "$MYSQL_HOST" -P "$MYSQL_PORT" --silent; do
    echo "MySQL not ready yet, retrying in $RETRY_INTERVAL seconds..."
    sleep $RETRY_INTERVAL
  done
  echo "[start-app.sh] MySQL is ready!"
}

function wait_for_redis() {
  echo "[start-app.sh] Waiting for Redis at $REDIS_HOST:$REDIS_PORT ..."
  until redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping &>/dev/null; do
    echo "Redis not ready yet, retrying in $RETRY_INTERVAL seconds..."
    sleep $RETRY_INTERVAL
  done
  echo "[start-app.sh] Redis is ready!"
}

[ "$WAIT_FOR_MYSQL" = "yes" ] && wait_for_mysql
[ "$WAIT_FOR_REDIS" = "yes" ] && wait_for_redis

# ===============================
# 启动服务
# ===============================
if [ $# -eq 0 ]; then
  echo "[start-app.sh] No command provided, exiting."
  exit 1
fi

echo "[start-app.sh] Starting service: $*"
exec "$@"

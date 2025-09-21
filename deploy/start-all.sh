#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# start-all.sh
# 自动处理 dev/prod 环境
# ==========================================

# 1. 加载根目录 .env
if [[ -f ".env" ]]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env 文件不存在，请先创建"
    exit 1
fi

# 默认应用服务列表
SERVICES=("crab-data-event" "crab-hmds")

# ------------------------------------------
# 等待服务函数
# ------------------------------------------
wait_for_mysql() {
    echo "⏳ 等待 MySQL 就绪..."
    until docker exec mysql-server mysqladmin ping -h"localhost" --silent; do
        sleep 2
    done
    echo "✅ MySQL 已就绪"
}

wait_for_redis() {
    echo "⏳ 等待 Redis 就绪..."
    until docker exec redis-server redis-cli ping | grep -q PONG; do
        sleep 2
    done
    echo "✅ Redis 已就绪"
}

# ------------------------------------------
# 启动基础设施
# ------------------------------------------
if [[ "${START_INFRA:-yes}" == "yes" ]]; then
    if [[ -f "${INFRA_COMPOSE_FILE:-}" ]]; then
        echo "🚀 启动基础设施: $INFRA_COMPOSE_FILE ..."
        docker-compose -f "$INFRA_COMPOSE_FILE" up -d

        # 等待 MySQL/Redis 就绪
        wait_for_mysql
        wait_for_redis
    else
        echo "⚠️ INFRA_COMPOSE_FILE 未定义或文件不存在，跳过基础设施启动"
    fi
else
    echo "ℹ️ 跳过基础设施启动 (START_INFRA=no)"
fi

# ------------------------------------------
# 启动应用服务
# ------------------------------------------
APP_COMPOSE="${APP_COMPOSE_FILE:-docker-compose.yml}"
echo "🚀 启动应用服务 (${APP_COMPOSE}) ..."
for service in "${SERVICES[@]}"; do
    docker-compose -f "$APP_COMPOSE" up -d "$service" &
done
wait  # 等待所有服务启动完成

# ------------------------------------------
# 输出日志
# ------------------------------------------
echo "📜 输出应用服务日志 (Ctrl+C 退出)..."
docker-compose -f "$APP_COMPOSE" logs -f "${SERVICES[@]}"

#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# start-all.sh
# 一键启动 infra + 应用服务（支持 dev/prod）
# ==========================================

# 1. 加载根目录 .env
if [[ -f ".env" ]]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env 文件不存在，请先创建"
    exit 1
fi

# 默认配置
START_INFRA=${START_INFRA:-yes}    # 是否启动基础设施
SERVICES=("crab-data-event" "crab-hmds")   # 应用服务列表

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
# 启动 infra
# ------------------------------------------
if [[ "$START_INFRA" == "yes" ]]; then
    if [[ -f "$INFRA_COMPOSE_FILE" ]]; then
        echo "🚀 启动基础设施: $INFRA_COMPOSE_FILE ..."
        docker-compose -f "$INFRA_COMPOSE_FILE" up -d

        # 等待 MySQL/Redis 就绪
        wait_for_mysql
        wait_for_redis
    else
        echo "⚠️ INFRA_COMPOSE_FILE 未定义或文件不存在，跳过基础设施启动"
    fi
fi

# ------------------------------------------
# 启动应用服务（并行）
# ------------------------------------------
echo "🚀 启动应用服务..."
for service in "${SERVICES[@]}"; do
    docker-compose up -d "$service" &
done
wait  # 等待所有服务启动完成

# ------------------------------------------
# 输出日志
# ------------------------------------------
echo "📜 正在输出应用服务日志 (Ctrl+C 退出)..."
docker-compose logs -f "${SERVICES[@]}"

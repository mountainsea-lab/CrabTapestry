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
# 启动基础设施
# ------------------------------------------
if [[ "${START_INFRA:-yes}" == "yes" ]]; then
    if [[ -f "${INFRA_COMPOSE_FILE:-}" ]]; then
        echo "🚀 启动基础设施: $INFRA_COMPOSE_FILE ..."
        docker-compose -f "$INFRA_COMPOSE_FILE" up -d
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

if [ ${#SERVICES[@]:-0} -eq 0 ]; then
    # 没有传 SERVICES，默认启动所有服务
    docker compose -f "$APP_COMPOSE" up -d --build
else
    # 启动指定服务
    for service in "${SERVICES[@]}"; do
        docker compose -f "$APP_COMPOSE" up -d --build "$service" &
    done
    wait
fi

# ------------------------------------------
# 输出日志
# ------------------------------------------
echo "📜 输出应用服务日志 (Ctrl+C 退出)..."
docker-compose -f "$APP_COMPOSE" logs -f "${SERVICES[@]}"

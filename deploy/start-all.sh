#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# start-all.sh
# 自动处理 dev/prod 环境
# macOS + Linux 通用
# ==========================================

# ------------------------------------------
# 1️⃣ 加载根目录 .env
# ------------------------------------------
ENV_FILE=".env"
if [[ -f "$ENV_FILE" ]]; then
    echo "🔹 加载 .env 文件: $ENV_FILE"
    # 使用 set -a + source 可处理路径带空格的情况
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "❌ .env 文件不存在，请先创建"
    exit 1
fi

# 默认应用服务列表
SERVICES=("crab-data-event" "crab-hmds")

# ------------------------------------------
# 2️⃣ 启动基础设施
# ------------------------------------------
if [[ "${START_INFRA:-yes}" == "yes" ]]; then
    if [[ -n "${INFRA_COMPOSE_FILE:-}" && -f "$INFRA_COMPOSE_FILE" ]]; then
        echo "🚀 启动基础设施: $INFRA_COMPOSE_FILE ..."
        docker compose -f "$INFRA_COMPOSE_FILE" up -d
    else
        echo "⚠️ INFRA_COMPOSE_FILE 未定义或文件不存在，跳过基础设施启动"
    fi
else
    echo "ℹ️ 跳过基础设施启动 (START_INFRA=no)"
fi

# ------------------------------------------
# 3️⃣ 启动应用服务
# ------------------------------------------
APP_COMPOSE="${APP_COMPOSE_FILE:-docker-compose.yml}"
echo "🚀 启动应用服务 (${APP_COMPOSE}) ..."

# 检查 compose 文件是否存在
if [[ ! -f "$APP_COMPOSE" ]]; then
    echo "❌ 应用 compose 文件不存在: $APP_COMPOSE"
    exit 1
fi

# 启动指定服务
for service in "${SERVICES[@]}"; do
    echo "🔹 启动服务: $service ..."
    docker compose -f "$APP_COMPOSE" up -d --build "$service"
done

# ------------------------------------------
# 4️⃣ 输出日志并跟随
# ------------------------------------------
echo "📜 输出应用服务日志 (Ctrl+C 退出)..."
docker compose -f "$APP_COMPOSE" logs -f "${SERVICES[@]}"

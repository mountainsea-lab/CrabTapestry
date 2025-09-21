# syntax=docker/dockerfile:1.4

# ================================
# 第一阶段：cargo-chef 基础镜像
# ================================
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ================================
# 第二阶段：生成依赖构建计划 (planner)
# ================================
FROM chef AS planner

ARG SERVICE_NAME
WORKDIR /app

# 复制 workspace
COPY Cargo.toml Cargo.lock ./
COPY libs ./libs
COPY crab-data-event ./crab-data-event
COPY crab-hmds ./crab-hmds
# 🚨 如果未来新增服务，只要 COPY 它的目录即可，或者直接 COPY . . （灵活选择）

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path recipe.json --bin ${SERVICE_NAME}

# ================================
# 第三阶段：构建依赖缓存 + 构建项目 (builder)
# ================================
FROM chef AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

COPY . .

ARG SERVICE_NAME
WORKDIR /app/${SERVICE_NAME}
RUN cargo build --release --bin ${SERVICE_NAME}

# ================================
# 第四阶段：运行时镜像
# ================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

ARG SERVICE_NAME
COPY --from=builder /app/target/release/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# 共享启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

ENTRYPOINT ["/deploy/start-app.sh"]

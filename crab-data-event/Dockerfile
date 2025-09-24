# syntax=docker/dockerfile:1.4
ARG RUST_VERSION=1.87.0
ARG SERVICE_NAME=crab-data-event

########################################
# 第一阶段：cargo-chef 镜像（用于依赖缓存）
########################################
FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION} AS chef
WORKDIR /app

########################################
# Planner 阶段
########################################
FROM chef AS planner
ARG SERVICE_NAME
WORKDIR /app/${SERVICE_NAME}

# 只 COPY 服务的 Cargo.toml（Git 依赖会自动解析）
COPY ${SERVICE_NAME}/Cargo.toml ./

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path /app/recipe.json --bin ${SERVICE_NAME}

########################################
# Builder 阶段
########################################
FROM chef AS builder
ARG SERVICE_NAME
WORKDIR /app/${SERVICE_NAME}

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev build-essential \
  && rm -rf /var/lib/apt/lists/*

# COPY 服务 Cargo.toml 和 recipe.json
COPY ${SERVICE_NAME}/Cargo.toml ./
COPY --from=planner /app/recipe.json /app/recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path /app/recipe.json

# COPY 源码
COPY ${SERVICE_NAME} ./

# 编译服务二进制
RUN CARGO_TARGET_DIR=/tmp/cargo-target cargo build --release

# 拷贝编译好的二进制到统一目录
RUN mkdir -p /bin/server && \
    cp /tmp/cargo-target/release/${SERVICE_NAME} /bin/server/${SERVICE_NAME}

########################################
# Runtime 阶段
########################################
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# COPY 编译好的二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# COPY 启动脚本并加可执行权限
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

# ENTRYPOINT 调用原始启动脚本
ENTRYPOINT ["/deploy/start-app.sh"]

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
WORKDIR /app

# 复制整个 workspace 的清单文件（保证本地路径依赖能解析）Cargo.lock
COPY Cargo.toml  ./
COPY libs ./libs
COPY ${SERVICE_NAME}/Cargo.toml ${SERVICE_NAME}/

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path recipe.json --bin ${SERVICE_NAME}

########################################
# Builder 阶段
########################################
FROM chef AS builder
ARG SERVICE_NAME
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev build-essential \
  && rm -rf /var/lib/apt/lists/*

# 复制 recipe.json 并构建依赖缓存
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 再复制源码（workspace 所有子 crate）
COPY . .

# 编译指定服务
RUN CARGO_TARGET_DIR=/tmp/cargo-target cargo build --release -p ${SERVICE_NAME}

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

# 只复制目标服务的二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# 复制启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

ENTRYPOINT ["/deploy/start-app.sh"]

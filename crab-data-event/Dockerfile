# syntax=docker/dockerfile:1.4
ARG RUST_VERSION=1.87.0
ARG SERVICE_NAME=crab-data-event

########################################
# 基础 cargo-chef 镜像
########################################
FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION} AS chef
WORKDIR /app

########################################
# Planner 阶段
########################################
FROM chef AS planner
ARG SERVICE_NAME
WORKDIR /app

# COPY workspace 的最小结构（根 Cargo.toml + 各 crate 的 Cargo.toml）
COPY Cargo.toml Cargo.lock ./
COPY crab-*/Cargo.toml crab-*/
COPY libs/*/Cargo.toml libs/*/Cargo.toml


# 运行 cargo-chef prepare（必须在 workspace 根目录）
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

# 拷贝 recipe.json 和 Cargo.toml 文件
COPY Cargo.toml Cargo.lock ./
COPY crab-*/Cargo.toml crab-*/
COPY libs/*/Cargo.toml libs/*/
COPY --from=planner /app/recipe.json recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# COPY 全部源码（必须在 cook 之后）
COPY . .

# 编译服务二进制
RUN CARGO_TARGET_DIR=/tmp/cargo-target cargo build --release --bin ${SERVICE_NAME}

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

ENTRYPOINT ["/deploy/start-app.sh"]

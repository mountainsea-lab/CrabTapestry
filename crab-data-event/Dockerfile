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

# COPY 根 Cargo.toml + Cargo.lock
COPY Cargo.toml ./

# 自动找到 workspace 下所有 crate 的 Cargo.toml（libs/* + 服务目录）
# 注意：需要在 COPY 之前创建目标目录
RUN find . -maxdepth 1 -type d ! -name . -exec mkdir -p {} \;

# 使用 shell 循环拷贝所有一级目录的 Cargo.toml
# 这样不管后续新增 libs 或服务，都无需修改 Dockerfile
RUN for dir in */ ; do \
        if [ -f "$dir/Cargo.toml" ]; then \
            cp "$dir/Cargo.toml" "$dir"; \
        fi; \
    done

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path /app/recipe.json --bin ${SERVICE_NAME}

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

# COPY recipe.json 并构建依赖缓存
COPY --from=planner /app/recipe.json /app/recipe.json
RUN cargo chef cook --release --recipe-path /app/recipe.json

# COPY workspace 全部源码
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

# 只 COPY 目标服务二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# COPY 启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

ENTRYPOINT ["/deploy/start-app.sh"]

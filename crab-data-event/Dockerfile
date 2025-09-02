# syntax=docker/dockerfile:1.4

# ================================
# 第一阶段：cargo-chef 镜像
# ================================
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ================================
# 第二阶段：生成依赖构建计划 (planner)
# ================================
FROM chef AS planner

ARG SERVICE
ENV SERVICE_NAME=$SERVICE
WORKDIR /app

# 检查 SERVICE_NAME 是否传入
RUN if [ -z "$SERVICE_NAME" ]; then echo "SERVICE_NAME is empty" && exit 1; fi

# 复制 workspace 根 Cargo.toml 和 Cargo.lock
COPY Cargo.toml  ./

# 复制 libs 各 crate 的 Cargo.toml
COPY libs ./libs

# 复制目标服务模块的 Cargo.toml
COPY ${SERVICE}/Cargo.toml ./${SERVICE}/

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path /app/recipe.json --bin $SERVICE_NAME

# ================================
# 第三阶段：构建依赖缓存 + 项目构建 (builder)
# ================================
FROM chef AS builder

ARG SERVICE
ENV SERVICE_NAME=$SERVICE
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# 拷贝 recipe.json
COPY --from=planner /app/recipe.json /app/recipe.json

# 拷贝完整源码
COPY . .

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path /app/recipe.json

# 构建目标服务
WORKDIR /app/${SERVICE}
RUN cargo build --release --bin $SERVICE_NAME

# ================================
# 第四阶段：精简运行镜像
# ================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 拷贝 release 二进制
COPY --from=builder /app/${SERVICE}/target/release/${SERVICE} /usr/local/bin/

# ENTRYPOINT
ENTRYPOINT ["/usr/local/bin/${SERVICE}"]
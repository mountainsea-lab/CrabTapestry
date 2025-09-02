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
WORKDIR /app

# 拷贝 workspace 根配置Cargo.lock
COPY Cargo.toml  ./
COPY libs/*/Cargo.toml ./libs/

# 拷贝目标服务模块 Cargo.toml
ARG SERVICE
COPY ${SERVICE}/Cargo.toml ./${SERVICE}/Cargo.toml

# 生成 recipe.json（在 /app 根目录）
RUN cargo chef prepare --recipe-path recipe.json --bin ${SERVICE}

# ================================
# 第三阶段：构建依赖缓存 + 构建项目 (builder)
# ================================
FROM chef AS builder
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# 拷贝 recipe.json
COPY --from=planner /app/recipe.json recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# 拷贝源码
COPY . .

# 构建目标服务
ARG SERVICE
RUN cargo build --release -p ${SERVICE}

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
ARG SERVICE
COPY --from=builder /app/${SERVICE}/target/release/${SERVICE} /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/${SERVICE}"]

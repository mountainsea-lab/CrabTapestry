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

# 复制 workspace 根 Cargo.toml 及成员配置
COPY Cargo.toml Cargo.lock ./
COPY libs ./libs
COPY crab-data-event/Cargo.toml ./crab-data-event/

# 进入目标服务目录，生成 recipe.json
WORKDIR /app/crab-data-event
RUN cargo chef prepare --recipe-path recipe.json --bin crab-data-event

# ================================
# 第三阶段：构建依赖缓存 + 编译 (builder)
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
COPY --from=planner /app/crab-data-event/recipe.json recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# 拷贝完整源码
COPY . .

# 构建 release 二进制
WORKDIR /app/crab-data-event
RUN cargo build --release --bin crab-data-event

# ================================
# 第四阶段：运行镜像 (runtime)
# ================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 拷贝 release 二进制
COPY --from=builder /app/crab-data-event/target/release/crab-data-event /usr/local/bin/

# 运行时配置通过挂载卷或环境变量注入
ENTRYPOINT ["/usr/local/bin/crab-data-event"]

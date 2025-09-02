# syntax=docker/dockerfile:1.4

# ================================
# 第一阶段：准备 cargo-chef 镜像
# ================================
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ================================
# 第二阶段：生成依赖构建计划 (planner)
# ================================
FROM chef AS planner

# workspace 根的依赖信息
COPY Cargo.toml Cargo.lock ./

# 复制 libs 的 Cargo.toml
COPY libs/*/Cargo.toml ./libs/

# 复制目标服务模块的 Cargo.toml
COPY crab-data-event/Cargo.toml ./crab-data-event/

# 进入服务模块目录生成 recipe
WORKDIR /app/crab-data-event
RUN cargo chef prepare --recipe-path crab-data-event/recipe.json -p crab-data-event

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

# 安装 cargo-chef
RUN cargo install cargo-chef --locked

# 拷贝 recipe
COPY --from=planner /app/crab-data-event/recipe.json recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# 拷贝源码
COPY . .

# 构建目标服务
WORKDIR /app/crab-data-event
RUN cargo build --release -p crab-data-event

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
COPY --from=builder /app/crab-data-event/target/release/crab-data-event /usr/local/bin/

# 运行时通过挂载卷或环境变量注入配置
ENTRYPOINT ["/usr/local/bin/crab-data-event"]

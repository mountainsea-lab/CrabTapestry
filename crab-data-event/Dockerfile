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

# 服务名参数
ARG SERVICE
ENV SERVICE_NAME=$SERVICE

# 将整个 workspace 复制到镜像中
# 注意：COPY 要在 build context 为 workspace 根目录下执行Cargo.lock
COPY Cargo.toml  ./
COPY libs ./libs
COPY crab-data-event ./crab-data-event
# 如果未来有其他服务，只要放在 workspace 根即可

# 确保 WORKDIR 在 workspace 根
WORKDIR /app

# 生成 cargo-chef recipe.json
# 注意 cargo-chef 需要 workspace 根才能解析依赖
RUN cargo chef prepare --recipe-path /app/recipe.json --bin $SERVICE_NAME

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

# 复制 cargo-chef recipe.json
COPY --from=planner /app/recipe.json /app/recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path /app/recipe.json

# 复制整个 workspace 源码
COPY . .

# 构建目标服务
WORKDIR /app/$SERVICE_NAME
RUN cargo build --release -p $SERVICE_NAME

# ================================
# 第四阶段：精简运行镜像
# ================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 拷贝编译好的二进制
COPY --from=builder /app/$SERVICE_NAME/target/release/$SERVICE_NAME /usr/local/bin/

# 默认入口
ENTRYPOINT ["/usr/local/bin/$SERVICE_NAME"]

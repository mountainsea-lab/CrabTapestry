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
WORKDIR /app

# 1️⃣ workspace 根依赖信息
COPY Cargo.toml Cargo.lock ./

# 2️⃣ libs 的 Cargo.toml
COPY libs/*/Cargo.toml ./libs/

# 3️⃣ 各服务模块 Cargo.toml
COPY crab-data-event/Cargo.toml ./crab-data-event/
# 如果未来有服务，只需要再 COPY crab-strategy-server/Cargo.toml ./crab-strategy-server/

# 4️⃣ 生成服务模块依赖 recipe.json
# 保持 WORKDIR 在根目录
RUN cargo chef prepare --recipe-path crab-data-event/recipe.json -p crab-data-event
# 如果未来有服务，可添加：
# RUN cargo chef prepare --recipe-path crab-strategy-server/recipe.json -p crab-strategy-server

# ================================
# 第三阶段：构建依赖缓存 + 编译服务 (builder)
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
COPY --from=planner /app/crab-data-event/recipe.json crab-data-event/recipe.json
# 未来服务可同理：
# COPY --from=planner /app/crab-strategy-server/recipe.json crab-strategy-server/recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path crab-data-event/recipe.json

# 拷贝完整源码
COPY . .

# 构建目标服务
WORKDIR /app/crab-data-event
RUN cargo build --release -p crab-data-event

# 未来服务可添加：
# WORKDIR /app/crab-strategy-server
# RUN cargo build --release -p crab-strategy-server

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
# 未来服务可添加：
# COPY --from=builder /app/crab-strategy-server/target/release/crab-strategy-server /usr/local/bin/

# 运行时通过挂载卷或环境变量注入配置
ENTRYPOINT ["/usr/local/bin/crab-data-event"]
# 如果有多个服务，每个服务可以用独立 Dockerfile 或通过环境变量/脚本启动不同二进制

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

ARG SERVICE_NAME
WORKDIR /app

# 将整个 workspace 复制到镜像中Cargo.lock
COPY Cargo.toml  ./
COPY libs ./libs
COPY crab-data-event ./crab-data-event
COPY crab-hmds ./crab-hmds
# 如果未来有其他服务，只要放在 workspace 根即可

# 生成 cargo-chef recipe.json
RUN cargo chef prepare --recipe-path recipe.json --bin ${SERVICE_NAME}

# ================================
# 第三阶段：构建依赖缓存 + 构建项目 (builder)
# ================================
FROM chef AS builder
WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# 复制 recipe.json
COPY --from=planner /app/recipe.json recipe.json

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# 复制 workspace 源码
COPY . .

# 构建目标服务
ARG SERVICE_NAME
WORKDIR /app/${SERVICE_NAME}
RUN cargo build --release --bin ${SERVICE_NAME}

# ================================
# 第四阶段：精简运行镜像
# ================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 拷贝编译好的二进制（注意：COPY 不会展开变量，所以用构建时 ARG）
ARG SERVICE_NAME
COPY --from=builder /app/target/release/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}
# 默认入口
#ENTRYPOINT ["/usr/local/bin/crab-hmds"]
# 拷贝共享启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

# ENTRYPOINT 使用共享启动脚本
ENTRYPOINT ["/deploy/start-app.sh"]
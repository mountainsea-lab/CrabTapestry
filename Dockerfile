# syntax=docker/dockerfile:1.4
ARG RUST_VERSION=1.87.0
ARG SERVICE_NAME=crab-data-event

########################################
# cargo-chef 基础镜像（缓存依赖）
########################################
FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION} AS chef
WORKDIR /app

########################################
# Planner 阶段：生成依赖 recipe.json
########################################
FROM chef AS planner
ARG SERVICE_NAME
WORKDIR /app

# 拷贝 workspace 配置（必须包含根 Cargo.toml / Cargo.lock / libs / 服务 Cargo.toml）
COPY Cargo.toml  ./
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

# 生成依赖清单
RUN cargo chef prepare --recipe-path recipe.json --bin ${SERVICE_NAME}

########################################
# Builder 阶段：构建依赖 + 服务二进制
########################################
FROM chef AS builder
ARG SERVICE_NAME
WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev build-essential \
  && rm -rf /var/lib/apt/lists/*

# 拷贝 recipe.json 并构建缓存
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 再 COPY 全部源码（libs + 服务）
COPY . .

# 编译指定服务
RUN CARGO_TARGET_DIR=/tmp/target cargo build --release -p ${SERVICE_NAME} \
 && strip /tmp/target/release/${SERVICE_NAME} \
 && mkdir -p /bin/server \
 && cp /tmp/target/release/${SERVICE_NAME} /bin/server/${SERVICE_NAME}

########################################
# Runtime 阶段：最小运行环境
########################################
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# 运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 拷贝编译好的二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# 启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

ENTRYPOINT ["/deploy/start-app.sh"]

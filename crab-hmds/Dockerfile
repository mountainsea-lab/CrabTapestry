# syntax=docker/dockerfile:1.4
ARG RUST_VERSION=1.87.0
ARG SERVICE_NAME

########################################
# Builder 阶段
########################################
FROM rust:${RUST_VERSION}-bullseye AS builder
ARG SERVICE_NAME
WORKDIR /app

# 安装构建依赖（glibc + dev libs）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    libmariadb-dev \
    git \
    bash \
    make \
 && rm -rf /var/lib/apt/lists/*

# 构建缓存目录
RUN mkdir -p /tmp/cargo-target

# ------------------------------
# 依赖缓存优化
# ------------------------------
# 先拷贝 Cargo.toml/Cargo.lock 及 workspace 相关 Cargo.toml
# 这里假设 workspace 中 libs/crab-infras 也在源码目录下
COPY Cargo.toml Cargo.lock ./
COPY libs/crab-infras/Cargo.toml libs/crab-infras/Cargo.toml

# 生成依赖层缓存
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN CARGO_TARGET_DIR=/tmp/cargo-target cargo build --release --package ${SERVICE_NAME} || true
RUN rm -rf src

# ------------------------------
# 拷贝实际源代码
# ------------------------------
COPY . .

# 编译指定服务
RUN CARGO_TARGET_DIR=/tmp/cargo-target \
    cargo build --release --package ${SERVICE_NAME}

# 拷贝编译好的二进制
RUN mkdir -p /bin/server && \
    cp /tmp/cargo-target/release/${SERVICE_NAME} /bin/server/${SERVICE_NAME}

########################################
# Runtime 阶段
########################################
FROM debian:bookworm-slim AS runtime
ARG SERVICE_NAME
WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libssl3 \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 拷贝编译好的二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# 拷贝启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

# 暴露端口
EXPOSE 10088

# 启动服务
ENTRYPOINT ["/bin/bash", "/deploy/start-app.sh"]

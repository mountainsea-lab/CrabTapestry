# syntax=docker/dockerfile:1.4
ARG RUST_VERSION=1.87.0
ARG SERVICE_NAME

########################################
# Builder 阶段
########################################
FROM rust:${RUST_VERSION}-alpine AS builder
ARG SERVICE_NAME
WORKDIR /app

# 安装构建依赖
RUN apk add --no-cache \
    clang lld musl-dev pkgconfig openssl-dev git bash make

# 构建缓存目录
RUN mkdir -p /tmp/cargo-target

# 拷贝整个 workspace
COPY . .

# 编译指定服务，屏蔽默认 feature（如 mysql）
RUN CARGO_TARGET_DIR=/tmp/cargo-target \
    cargo build --release --package ${SERVICE_NAME} --no-default-features

# 拷贝编译好的二进制
RUN mkdir -p /bin/server && \
    cp /tmp/cargo-target/release/${SERVICE_NAME} /bin/server/${SERVICE_NAME}

########################################
# Runtime 阶段
########################################
FROM alpine:3.18 AS runtime
WORKDIR /app

# 拷贝二进制
COPY --from=builder /bin/server/${SERVICE_NAME} /usr/local/bin/${SERVICE_NAME}

# 拷贝启动脚本
COPY deploy/start-app.sh /deploy/start-app.sh
RUN chmod +x /deploy/start-app.sh

# 设置工作目录
WORKDIR /app

# 暴露端口（按服务实际需求）
EXPOSE 10086

# 启动服务（通过你的脚本）
ENTRYPOINT ["/bin/sh", "/deploy/start-app.sh"]


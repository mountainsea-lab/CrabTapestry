# ================================
# 第一阶段：准备 cargo-chef 镜像
# ================================
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ================================
# 第二阶段：生成依赖构建计划 (planner)
# ================================
FROM chef AS planner

# 复制 workspace 根配置（注意路径必须从 context 根开始，而不是 Dockerfile 相对路径）
COPY Cargo.toml Cargo.lock ./
COPY libs ./libs
COPY crab-data-event/Cargo.toml ./crab-data-event/

WORKDIR /app/crab-data-event
RUN cargo chef prepare --recipe-path recipe.json

# ================================
# 第三阶段：构建依赖缓存 + 构建项目 (builder)
# ================================
FROM chef AS builder
WORKDIR /app

COPY --from=planner /app/crab-data-event/recipe.json recipe.json

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# 构建依赖缓存
RUN cargo chef cook --release --recipe-path recipe.json

# 拷贝源码并构建 release binary
COPY . .
WORKDIR /app/crab-data-event
RUN cargo build --release -p crab-data-event

# ================================
# 第四阶段：精简运行镜像
# ================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 拷贝 release binary
COPY --from=builder /app/crab-data-event/target/release/crab-data-event /usr/local/bin/

# 配置文件通过挂载卷或环境变量注入
ENTRYPOINT ["/usr/local/bin/crab-data-event"]

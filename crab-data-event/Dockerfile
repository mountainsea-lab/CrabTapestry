# --- 第一阶段：准备 cargo-chef 镜像 ---
FROM lukemathwalker/cargo-chef:latest-rust-1 as chef
WORKDIR /app

# --- 第二阶段：生成依赖构建计划 ---
FROM chef AS planner
COPY . .
WORKDIR /app/crab-data-event
RUN cargo chef prepare --recipe-path recipe.json

# --- 第三阶段：构建依赖缓存 + 构建项目 ---
FROM chef AS builder
WORKDIR /app
COPY .cargo/config.toml /usr/local/cargo/config.toml
COPY --from=planner /app/crab-data-event/recipe.json recipe.json

# 添加 SSH known_hosts（如果有私有依赖）
RUN mkdir -p /root/.ssh && ssh-keyscan github.com >> /root/.ssh/known_hosts

# 构建依赖缓存
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

# 拷贝源码并构建二进制
COPY . .
WORKDIR /app/crab-data-event
RUN --mount=type=ssh cargo build --release

# --- 第四阶段：精简运行镜像 ---
FROM debian:bookworm-slim AS runtime

# 安装运行时依赖库
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 拷贝 release 二进制
COPY --from=builder /app/crab-data-event/target/release/crab-data-event /usr/local/bin/

# ✅ 不再拷贝 config 文件，运行时通过挂载或环境变量注入
# COPY config/ /app/config/

ENTRYPOINT ["/usr/local/bin/crab-data-event"]

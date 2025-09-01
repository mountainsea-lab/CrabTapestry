# 第一阶段：准备 cargo-chef 镜像
FROM lukemathwalker/cargo-chef:latest-rust-1 as chef
WORKDIR /app

  # 第二阶段：生成依赖构建计划
FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

  # 第三阶段：构建依赖缓存 + 使用 SSH 拉私有依赖
FROM chef as builder
WORKDIR /app

  # ✅ 添加 cargo 配置，启用 git-fetch-with-cli，放在最前面
COPY .cargo/config.toml /usr/local/cargo/config.toml

  # 拷贝构建计划
COPY --from=planner /app/recipe.json recipe.json

  # 添加 SSH known_hosts，避免 Host key verification 失败
RUN mkdir -p /root/.ssh && \
ssh-keyscan github.com >> /root/.ssh/known_hosts

  # 允许容器访问 SSH agent (需要用 BuildKit 并带 --ssh)
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

  # 拷贝项目源代码
COPY . .

  # 构建项目（同样使用 SSH agent）
RUN --mount=type=ssh cargo build --release

  # 第四阶段：精简运行镜像
FROM debian:bookworm-slim as runtime

  # 安装运行时依赖库
RUN apt-get update && apt-get install -y \
ca-certificates \
openssl \
libssl3 \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

  # 拷贝 release 构建产物
COPY --from=builder /app/target/release/crab-data-event /usr/local/bin/

  # 拷贝配置文件（如存在）
COPY config/ config/

ENTRYPOINT ["/usr/local/bin/crab-data-event"]
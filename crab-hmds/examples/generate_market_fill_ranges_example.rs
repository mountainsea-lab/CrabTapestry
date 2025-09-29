use crab_hmds::domain::service::generate_and_insert_fill_ranges;
use crab_hmds::global::init_global_services;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 设置临时的 DATABASE_URL 环境变量，包裹在 unsafe 块中
    unsafe {
        env::set_var(
            "DATABASE_URL",
            "mysql://root:root@mysql.infra.orb.local:3306/crabtapestry",
        );
    }

    // 初始化日志
    ms_tracing::setup_tracing();
    // 初始化全局变量
    setup().await;

    generate_and_insert_fill_ranges().await?;

    Ok(())
}

// 1. 初始化测试数据库和配置
async fn setup() {
    // 初始化全局服务，连接到数据库
    // init global comments domain
    let _ = init_global_services().await;
}

// Global init ; Use Arc to avoid cloning actual instances and allow shared ownership

use anyhow::Result;
use crab_infras::db::Database;
use std::sync::Arc;

/// 初始化全局服务
pub async fn init_global_services() -> Result<()> {
    // 1. 初始化 Postgres 连接池
    Database::initialize().await?;

    Ok(())
}

/// 获取共享数据库（Arc 包装，方便注入 Service）
pub fn global_db() -> Arc<Database> {
    Arc::new(Database::global().clone())
}

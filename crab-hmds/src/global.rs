// Global init ; Use Arc to avoid cloning actual instances and allow shared ownership

use anyhow::Result;
use crab_infras::db::mysql::{MySqlPool, make_mysql_pool};
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub static MYSQL_POOL: OnceCell<Arc<MySqlPool>> = OnceCell::new();

/// 初始化全局服务
pub async fn init_global_services() -> Result<()> {
    let mysql_pool = Arc::new(make_mysql_pool().expect("MySQL init failed"));
    let _ = set_mysql_pool(mysql_pool).await;

    Ok(())
}

/// 异步设置 Mysql Pool 实例
pub async fn set_mysql_pool(instance: Arc<MySqlPool>) -> Result<(), Arc<MySqlPool>> {
    MYSQL_POOL.set(instance)
}

/// 获取共享的 Mysql Pool 实例
pub fn get_mysql_pool() -> Arc<MySqlPool> {
    MYSQL_POOL.get().expect("MYSQL_POOL not initialized").clone()
}

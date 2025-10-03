// Global init ; Use Arc to avoid cloning actual instances and allow shared ownership

use crate::config::AppConfig;
use crate::load_app_config;
use anyhow::Result;
use crab_infras::db::mysql::{MySqlPool, make_mysql_pool};
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub static MYSQL_POOL: OnceCell<Arc<MySqlPool>> = OnceCell::new();
// 用于存储全局应用配置
pub static APP_CONFIG: OnceCell<AppConfig> = OnceCell::new();

/// 初始化全局服务
pub async fn init_global_services() -> Result<()> {
    // 1. 初始化 MySQL 连接池
    let mysql_pool = Arc::new(make_mysql_pool().expect("MySQL init failed"));
    let _ = set_mysql_pool(mysql_pool).await;

    // 2. 加载应用配置
    let app_config = load_app_config().expect("App config load failed");
    let _ = set_app_config(app_config).await;

    Ok(())
}

/// 异步设置 Mysql Pool 实例
pub async fn set_mysql_pool(instance: Arc<MySqlPool>) -> Result<(), Arc<MySqlPool>> {
    MYSQL_POOL.set(instance)
}

/// 异步设置应用配置
pub async fn set_app_config(config: AppConfig) -> Result<(), AppConfig> {
    APP_CONFIG.set(config)
}

/// 获取共享的 Mysql Pool 实例
pub fn get_mysql_pool() -> Arc<MySqlPool> {
    MYSQL_POOL.get().expect("MYSQL_POOL not initialized").clone()
}

/// 获取共享的应用配置
pub fn get_app_config() -> &'static AppConfig {
    APP_CONFIG.get().expect("APP_CONFIG not initialized")
}

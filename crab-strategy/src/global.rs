// Global init ; Use Arc to avoid cloning actual instances and allow shared ownership
use crate::config::strategy_config::StrategyConfigManager;
use anyhow::{Result, anyhow};
use ms_tracing::tracing_utils::internal::error;
use once_cell::sync::OnceCell;
use std::sync::Arc;

// 用于存储策略应用配置
static STRATEGY_CONFIG: OnceCell<Arc<StrategyConfigManager>> = OnceCell::new();
/// 初始化全局服务
pub async fn init_global_services() -> Result<()> {
    // 1️⃣ 从环境变量或默认路径加载配置
    let cfg = StrategyConfigManager::load_from_env_or_default().await?;
    let manager = Arc::new(StrategyConfigManager::new(cfg));

    // 2️⃣ 设置全局单例（只允许初始化一次）
    if let Err(_) = STRATEGY_CONFIG.set(manager.clone()) {
        error!("⚠️ STRATEGY_CONFIG 已初始化，重复调用被忽略。");
        return Err(anyhow!("STRATEGY_CONFIG already initialized"));
    }

    Ok(())
}

/// 异步设置应用配置
pub async fn set_strategy_config(config: Arc<StrategyConfigManager>) -> Result<(), Arc<StrategyConfigManager>> {
    STRATEGY_CONFIG.set(config)
}

/// 获取共享的应用配置
pub fn get_strategy_config() -> Arc<StrategyConfigManager> {
    STRATEGY_CONFIG.get().expect("STRATEGY_CONFIG not initialized").clone()
}

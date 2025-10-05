use crate::config::strategy_config::StrategyConfigManager;
use crate::trader::crab_trader::CrabTrader;
use anyhow::{Result, anyhow};
use ms_tracing::tracing_utils::internal::{error, info};
use once_cell::sync::OnceCell;
use std::sync::Arc;

/// 全局策略配置单例
static STRATEGY_CONFIG: OnceCell<Arc<StrategyConfigManager>> = OnceCell::new();

/// 全局交易器单例
static CRAB_TRADER: OnceCell<Arc<CrabTrader>> = OnceCell::new();

/// 初始化全局服务（配置 + 交易器）
pub async fn init_global_services() -> Result<()> {
    // 1️⃣ 初始化策略配置
    let cfg = StrategyConfigManager::load_from_env_or_default().await?;
    let manager = Arc::new(StrategyConfigManager::new(cfg));

    if STRATEGY_CONFIG.set(manager.clone()).is_err() {
        error!("⚠️ STRATEGY_CONFIG 已初始化，重复调用被忽略。");
        return Err(anyhow!("STRATEGY_CONFIG already initialized"));
    }
    info!("✅ 全局策略配置初始化成功。");

    // 2️⃣ 初始化交易器
    let trader = CrabTrader::create().await.map_err(|e| {
        error!("❌ CrabTrader 创建失败: {:?}", e);
        anyhow!("Failed to create CrabTrader: {:?}", e)
    })?;
    let trader_arc = Arc::new(trader);

    if CRAB_TRADER.set(trader_arc.clone()).is_err() {
        error!("⚠️ CRAB_TRADER 已初始化，重复调用被忽略。");
        return Err(anyhow!("CRAB_TRADER already initialized"));
    }
    info!("✅ CrabTrader 全局实例初始化成功。");

    Ok(())
}

/// 获取共享策略配置
pub fn get_strategy_config() -> Arc<StrategyConfigManager> {
    STRATEGY_CONFIG
        .get()
        .expect("❌ STRATEGY_CONFIG not initialized — 请先调用 init_global_services()")
        .clone()
}

/// 获取全局交易器实例
pub fn get_crab_trader() -> Arc<CrabTrader> {
    CRAB_TRADER
        .get()
        .expect("❌ CRAB_TRADER not initialized — 请先调用 init_global_services()")
        .clone()
}

use crab_infras::config::sub_config::{SubscriptionMap, load_subscriptions_map};
use ms_tracing::tracing_utils::internal::info;

pub mod ingestor;

/// 加载 subscriptions 配置
pub fn load_subscriptions_config() -> anyhow::Result<SubscriptionMap> {
    // 1. 优先使用环境变量 SUBSCRIPTIONS_CONFIG
    let config_path = if let Ok(path) = std::env::var("SUBSCRIPTIONS_CONFIG") {
        path
    } else {
        // 2. 尝试从当前工作目录加载（生产部署推荐）
        let cwd_path = std::env::current_dir()
            .map(|p| p.join("subscriptions.toml"))
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()));

        if let Some(path) = cwd_path {
            if std::path::Path::new(&path).exists() {
                path
            } else {
                // 3. 开发模式 fallback：crate 根目录
                format!("{}/subscriptions.toml", env!("CARGO_MANIFEST_DIR"))
            }
        } else {
            // 如果连 current_dir 都取不到，直接 fallback
            format!("{}/subscriptions.toml", env!("CARGO_MANIFEST_DIR"))
        }
    };

    info!("Loading subscriptions from: {}", config_path);
    load_subscriptions_map(&config_path)
}

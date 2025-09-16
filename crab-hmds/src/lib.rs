use crab_infras::config::sub_config::{SubscriptionMap, load_subscriptions_map};
use ms_tracing::tracing_utils::internal::info;

pub mod ingestor;

/// 加载 subscriptions 配置
pub fn load_subscriptions_config() -> anyhow::Result<SubscriptionMap> {
    let config_path = std::env::var("SUBSCRIPTIONS_CONFIG").unwrap_or_else(|_| {
        let manifest_dir = option_env!("CARGO_MANIFEST_DIR").map(|s| s.to_string()).unwrap_or_else(|| {
            std::env::current_dir()
                .expect("Failed to get current dir")
                .to_string_lossy()
                .to_string()
        });
        format!("{}/subscriptions.toml", manifest_dir)
    });

    info!("Loading subscriptions from: {}", config_path);
    load_subscriptions_map(&config_path)
}

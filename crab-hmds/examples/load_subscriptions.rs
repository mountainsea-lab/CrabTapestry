use crab_infras::config::sub_config::{Subscription, SubscriptionConfig, load_subscriptions_map};
use ms_tracing::tracing_utils::internal::info;

/// ==============================
///  配置方式获取交易所币种周期信息案例
/// ==============================
fn main() -> anyhow::Result<()> {
    // 初始化日志
    ms_tracing::setup_tracing();

    // 支持环境变量覆盖配置文件路径
    let config_path = std::env::var("SUBSCRIPTIONS_CONFIG").unwrap_or_else(|_| {
        // 先尝试 CARGO_MANIFEST_DIR
        let manifest_dir = option_env!("CARGO_MANIFEST_DIR").map(|s| s.to_string()).unwrap_or_else(|| {
            // 再 fallback 到当前工作目录
            std::env::current_dir()
                .expect("Failed to get current dir")
                .to_string_lossy()
                .to_string()
        });

        format!("{}/subscriptions.toml", manifest_dir)
    });

    info!("Loading subscriptions from: {}", config_path);

    // 假设 subscriptions.toml 在当前目录
    let subs_map = load_subscriptions_map(&config_path)?;

    // 遍历输出
    for entry in subs_map.iter() {
        let (key, sub) = entry.pair();
        info!("Exchange: {}, Symbol: {}, Periods: {:?}", key.0, key.1, sub.periods);
    }

    Ok(())
}

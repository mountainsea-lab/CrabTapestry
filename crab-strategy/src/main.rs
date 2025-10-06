use crab_strategy::server;
use std::env;

#[tokio::main]
pub async fn main() {
    // 本地运行
    unsafe {
        env::set_var(
            "STRATEGY_CONFIG_PATH",
            "crab_hmds-bar_cache/config/strategy_config.json",
        );
    }
    server::start().await;
}

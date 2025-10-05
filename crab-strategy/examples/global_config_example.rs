use crab_strategy::global;
use ms_tracing::tracing_utils::internal::info;
use std::env;

const FILE_PATH_SYSTEM_CONFIG: &str = "crab-strategy/config/strategy_config.json";
#[tokio::main]
async fn main() {
    // 设置临时的 STRATEGY_CONFIG_PATH 环境变量，包裹在 unsafe 块中
    unsafe {
        env::set_var("STRATEGY_CONFIG_PATH", FILE_PATH_SYSTEM_CONFIG);
    }

    // 初始化日志
    ms_tracing::setup_tracing();
    // 初始化全局配置
    let _ = global::init_global_services().await;

    // 配置获取
    let strategy_config = global::get_strategy_config().get();

    info!("{:#?}", strategy_config);
}

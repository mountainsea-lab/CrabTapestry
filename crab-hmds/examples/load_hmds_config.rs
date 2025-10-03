use crab_hmds::load_app_config;
use ms_tracing::tracing_utils::internal::info;

/// ==============================
///  应用配置信息获取
/// ==============================
#[tokio::main]
fn main() -> anyhow::Result<()> {
    // 初始化日志
    ms_tracing::setup_tracing();

    // 假设 hmds.toml 在当前目录
    let app_config = load_app_config().expect("系统应用配置信息读取失败");

    info!("app_config: {:?}", app_config);

    Ok(())
}

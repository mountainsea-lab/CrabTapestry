use std::env;
use std::path::PathBuf;

const FILE_PATH_SYSTEM_CONFIG: &str = "./mq-market-data/config/system_config.json";

const FILE_PATH_ASSET_CONFIG: &str = "./mq-market-data/config/asset_config.json";

/// 获取配置路径（支持环境变量覆盖，默认指向项目内 config/）
fn get_config_path(filename: &str) -> PathBuf {
    let base_path = env::var("MQ_CONFIG_PATH").unwrap_or_else(|_| "./config".to_string());
    PathBuf::from(base_path).join(filename)
}

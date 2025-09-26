use crate::config::AppConfig;
use crate::global::get_app_config;
use anyhow::Context;
use crab_infras::config::sub_config::{Subscription, SubscriptionMap, load_subscriptions_map};
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;
use std::{env, fs};

pub mod config;
pub mod domain;
pub mod global;
pub mod ingestor;
pub mod macros;
pub mod schema;
pub mod server;

/// 加载 subscriptions 配置
pub fn load_subscriptions_config() -> anyhow::Result<SubscriptionMap> {
    // 1. 优先使用环境变量 SUBSCRIPTIONS_CONFIG
    let config_path = if let Ok(path) = env::var("SUBSCRIPTIONS_CONFIG") {
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

/// 加载应用配置
pub fn load_app_config() -> anyhow::Result<AppConfig> {
    // 1. 优先使用环境变量 CONFIG_PATH
    let config_path = if let Ok(path) = env::var("CONFIG_PATH") {
        path
    } else {
        // 2. 尝试从当前工作目录加载（生产部署推荐）
        let cwd_path = env::current_dir()
            .map(|p| p.join("config/hmds.toml"))
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()));

        if let Some(path) = cwd_path {
            if std::path::Path::new(&path).exists() {
                path
            } else {
                // 3. 开发模式 fallback：crate 根目录
                format!("{}/config/hmds.toml", env!("CARGO_MANIFEST_DIR"))
            }
        } else {
            // 如果连 current_dir 都取不到，直接 fallback
            format!("{}/config/hmds.toml", env!("CARGO_MANIFEST_DIR"))
        }
    };

    // 打印配置文件路径
    info!("Loading app config from: {}", config_path);

    // 加载配置文件内容
    let config_content =
        fs::read_to_string(&config_path).context(format!("Unable to read the config file: {}", config_path))?;

    // 解析配置文件为 AppConfig 结构
    toml::de::from_str(&config_content).context("Unable to parse the config file") // 解析失败时抛出错误
}

/// 从 TOML 文件加载并初始化 SubscriptionMap
pub fn load_subscriptions() -> anyhow::Result<Arc<DashMap<(String, String), Subscription>>> {
    let map = Arc::new(DashMap::new());
    for exch_cfg in &get_app_config().subscriptions {
        for sub in exch_cfg.to_subscriptions() {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            map.insert(key, sub);
        }
    }
    Ok(map)
}

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use barter::system::config::SystemConfig;
use crab_infras::config::sub_config::SubscriptionConfig;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::time::sleep;

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    // 策略订阅资产信息
    pub system_config: SystemConfig,
    // 策略资产扩展需要的周期信息
    pub subscriptions: Vec<SubscriptionConfig>,
}

pub struct StrategyConfigManager {
    pub(crate) inner: ArcSwap<StrategyConfig>,
}

impl StrategyConfigManager {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self { inner: ArcSwap::from_pointee(cfg) }
    }

    pub fn get(&self) -> Arc<StrategyConfig> {
        self.inner.load_full()
    }

    pub fn reload(&self, cfg: StrategyConfig) {
        self.inner.store(Arc::new(cfg));
    }

    /// 异步从 JSON 文件加载配置
    pub async fn load_config_from_file(path: impl Into<PathBuf>) -> Result<StrategyConfig> {
        let path = path.into();
        let content = fs::read_to_string(&path)
            .await
            .with_context(|| format!("Failed to read config file: {:?}", path))?;
        let cfg = serde_json::from_str(&content)?;
        Ok(cfg)
    }

    /// 从环境变量获取路径加载配置,不存在报错
    pub async fn load_from_env_or_default() -> Result<StrategyConfig> {
        // 优先尝试从环境变量读取
        let config_path = match env::var("STRATEGY_CONFIG_PATH") {
            Ok(path) => PathBuf::from(path),
            Err(_) => {
                // ❌ 若环境变量不存在，返回错误
                return Err(anyhow::anyhow!(
                    "❌ STRATEGY_CONFIG_PATH 未设置，请在运行环境中指定配置文件路径（例如：/app/config/bar_cache.json）"
                ));
            }
        };
        Self::load_config_from_file(config_path).await
    }

    /// 异步文件热更新监听
    ///
    /// 使用 tokio 任务，不阻塞 runtime
    pub async fn watch_file_for_hot_reload(manager: Arc<Self>, path: impl Into<PathBuf>) -> Result<()> {
        let path = path.into();
        let path_clone = path.clone();
        let manager_clone = manager.clone();

        // 使用 notify watcher
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let mut watcher: RecommendedWatcher = Watcher::new(
            move |res| {
                let _ = tx.try_send(res);
            },
            Default::default(),
        )?;
        watcher.watch(&path, RecursiveMode::NonRecursive)?;

        tokio::spawn(async move {
            while let Some(res) = rx.recv().await {
                match res {
                    Ok(event) => {
                        if matches!(event.kind, EventKind::Modify(_)) {
                            println!("🔄 Config file modified, reloading...");
                            match StrategyConfigManager::load_config_from_file(&path_clone).await {
                                Ok(cfg) => {
                                    manager_clone.reload(cfg);
                                    println!("✅ Config hot-reloaded");
                                }
                                Err(e) => eprintln!("❌ Failed to reload config: {:?}", e),
                            }
                        }
                    }
                    Err(e) => eprintln!("❌ Watcher error: {:?}", e),
                }
                // 防抖
                sleep(Duration::from_millis(500)).await;
            }
        });

        Ok(())
    }
}

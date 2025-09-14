use anyhow::Result;
use chrono::{Duration, Utc};
use crab_hmds::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
use crab_hmds::ingestor::scheduler::BackfillDataType;
use crab_hmds::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crab_hmds::ingestor::scheduler::service::historical_backfill_service::HistoricalBackfillService;
use crab_hmds::ingestor::scheduler::service::{BackfillMetaStore, InMemoryBackfillMetaStore, MarketKey};
use crab_infras::config::sub_config::{SubscriptionMap, load_subscriptions_map};
use dashmap::DashMap;
use futures::future::join_all;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    ms_tracing::setup_tracing();
    // -------------------------------
    // 1️⃣ 初始化存储和调度器
    // -------------------------------
    let meta_store = Arc::new(InMemoryBackfillMetaStore::new());
    let fetcher = Arc::new(BinanceFetcher::new());
    let scheduler = BaseBackfillScheduler::new(fetcher.clone(), 3); // retry_limit=3

    let service = Arc::new(HistoricalBackfillService::new(
        scheduler.clone(),
        meta_store.clone(),
        4, // default_max_batch_hours
        3, // max_retries
    ));

    // -------------------------------
    // 2️⃣ 创建模拟订阅
    // -------------------------------
    let mut subscriptions = load_subscriptions_config()?;

    // -------------------------------
    // 3️⃣ 启动 worker
    // -------------------------------
    let (shutdown_tx, _notify) = service.clone().start_workers(4);

    // -------------------------------
    // 4️⃣ 初始化最近任务 (过去 2 小时)
    // -------------------------------
    service
        .init_recent_tasks(
            &subscriptions,
            2,    // past_hours
            1000, // step_millis
            BackfillDataType::OHLCV,
        )
        .await;

    // -------------------------------
    // 5️⃣ 模拟后台回溯任务 (过去 1 天)
    // -------------------------------
    service
        .backfill_historical(
            &subscriptions,
            1000, // step_millis
            BackfillDataType::OHLCV,
            1, // lookback_days
        )
        .await;

    // -------------------------------
    // 6️⃣ 等待一段时间让 worker 处理任务
    // -------------------------------
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // -------------------------------
    // 7️⃣ 停止 worker
    // -------------------------------
    let _ = shutdown_tx.send(());

    // -------------------------------
    // 8️⃣ 输出最终 meta
    // -------------------------------
    for (_key_tuple, sub) in subscriptions.iter().map(|e| (e.key().clone(), e.value().clone())) {
        for period in &sub.periods {
            let key = MarketKey {
                exchange: sub.exchange.to_string(),
                symbol: sub.symbol.to_string(),
                interval: period.to_string(),
            };
            let meta = meta_store.get_meta(&key).await.unwrap();
            info!("Market: {:?}, Meta: {:?}", key, meta);
        }
    }

    info!("Demo finished.");

    Ok(())
}

/// 加载 subscriptions 配置
pub fn load_subscriptions_config() -> Result<SubscriptionMap> {
    // 支持环境变量覆盖配置文件路径
    let config_path = std::env::var("SUBSCRIPTIONS_CONFIG").unwrap_or_else(|_| {
        // 先尝试 CARGO_MANIFEST_DIR
        let manifest_dir = option_env!("CARGO_MANIFEST_DIR").map(|s| s.to_string()).unwrap_or_else(|| {
            // fallback 到当前工作目录
            std::env::current_dir()
                .expect("Failed to get current dir")
                .to_string_lossy()
                .to_string()
        });

        format!("{}/subscriptions.toml", manifest_dir)
    });

    info!("Loading subscriptions from: {}", config_path);

    // 调用你的 subscriptions 读取函数
    load_subscriptions_map(&config_path)
}

use anyhow::Result;
use chrono::{Duration, Utc};
use crab_hmds::config::AppConfig;
use crab_hmds::global::get_app_config;
use crab_hmds::ingestor::dedup::Deduplicatable;
use crab_hmds::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
use crab_hmds::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crab_hmds::ingestor::scheduler::service::historical_backfill_service::HistoricalBackfillService;
use crab_hmds::ingestor::scheduler::service::{BackfillMetaStore, InMemoryBackfillMetaStore, MarketKey};
use crab_hmds::ingestor::scheduler::{BackfillDataType, HistoricalBatchEnum};
use crab_hmds::{load_app_config, load_subscriptions};
use crab_infras::config::sub_config::{Subscription, SubscriptionMap};
use dashmap::DashMap;
use dotenvy::dotenv;
use futures::future::join_all;
use ms_tracing::tracing_utils::internal::{info, warn};
use std::env;
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
        3, // default_max_batch_hours
    ));

    // -------------------------------
    // 2️⃣ 加载订阅配置
    // -------------------------------

    // -------------------------------
    // 3️⃣ (1)历史数据拉取调阅器启动,等待拉取任务到来 (2) 启动 worker 维护tasks
    // -------------------------------
    let shutdown = Arc::new(Notify::new());
    service.clone().start_workers(2, shutdown.clone());
    tokio::spawn(scheduler.clone().run(2));
    // -------------------------------
    // 4️⃣ 启动订阅输出观察任务
    // -------------------------------

    let mut sub_rx = scheduler.subscribe();
    tokio::spawn(async move {
        while let Some(batch_enum) = sub_rx.recv().await {
            match batch_enum {
                HistoricalBatchEnum::OHLCV(batch) => {
                    info!(
                        "✅ [{}] Got {} OHLCV records for {}:{} [{} - {}]",
                        batch.period.as_deref().unwrap_or("unknown"), // Option<Arc<str>> -> &str
                        batch.data.len(),
                        batch.exchange,
                        batch.symbol,
                        batch.range.start,
                        batch.range.end
                    );
                    for c in batch.data.iter().take(3) {
                        println!(
                            "    Candle ts={} O={} H={} L={} C={} V={}",
                            c.timestamp(),
                            c.open,
                            c.high,
                            c.low,
                            c.close,
                            c.volume
                        );
                    }
                }
                HistoricalBatchEnum::Tick(batch) => {
                    info!("Tick batch received: {} ticks", batch.data.len());
                    for t in batch.data.iter().take(3) {
                        println!("    Tick ts={} price={} qty={}", t.ts, t.price, t.qty);
                    }
                }
                _ => {}
            }
        }
    });

    // -------------------------------
    // 5️⃣ 初始化任务
    // -------------------------------
    // // 最近数据（过去 2 小时）
    // service.init_recent_tasks(&subscriptions, 2, BackfillDataType::OHLCV).await;
    //
    // // 历史回溯（过去 1 天）
    // service.backfill_historical(&subscriptions, BackfillDataType::OHLCV, 1).await;

    // -------------------------------
    // 6️⃣ 启动后台维护任务
    // -------------------------------

    let svc = service.clone();
    tokio::spawn(async move {
        svc.loop_maintain_tasks_notify(BackfillDataType::OHLCV, &shutdown).await;
    });

    // -------------------------------
    // 7️⃣ 常驻运行直到 Ctrl+C
    // -------------------------------
    info!("Service running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping service...");

    Ok(())
}

/// 从 TOML 文件加载并初始化 SubscriptionMap
pub fn load_subscriptionMaps(app_config: AppConfig) -> Result<Arc<DashMap<(String, String), Subscription>>> {
    let map = Arc::new(DashMap::new());
    for exch_cfg in &app_config.subscriptions {
        for sub in exch_cfg.to_subscriptions() {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            map.insert(key, sub);
        }
    }
    Ok(map)
}

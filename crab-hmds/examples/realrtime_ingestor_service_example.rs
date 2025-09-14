//! examples/realrtime_ingestor_service_example

use crab_hmds::ingestor::ctrservice::control_service::IngestorService;
use crab_hmds::ingestor::ctrservice::{ExchangeConfig, IngestorConfig};
use crab_hmds::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
use crab_hmds::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crab_hmds::ingestor::realtime::subscriber::RealtimeSubscriber;
use crab_hmds::ingestor::realtime::subscriber::binance_subscriber::BinanceSubscriber;
use crab_hmds::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, broadcast};
use tokio::time::{Duration, sleep};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    // -----------------------------
    // Step 1: 初始化配置交易所/币种/周期信息
    // -----------------------------
    let config = IngestorConfig {
        exchanges: vec![ExchangeConfig {
            exchange: "BinanceFuturesUsd".to_string(),
            symbols: vec!["btc".to_string(), "eth".to_string()],
            periods: vec!["1h".to_string(), "5m".to_string()],
        }],
    };

    // -----------------------------
    // Step 2: 初始化历史数据拉取器
    // -----------------------------
    let fetcher = Arc::new(BinanceFetcher::new());
    let backfill_scheduler = BaseBackfillScheduler::new(fetcher.clone(), 2);

    // -----------------------------
    // Step 3: 初始化实时订阅 pipeline
    // -----------------------------
    // 创建 broadcast channel
    let (broadcast_tx, _broadcast_rx) = broadcast::channel(1024);
    let subscriber = Arc::new(BinanceSubscriber::new(broadcast_tx.clone()));

    let mut subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>> = HashMap::new();
    subscribers.insert("BinanceFuturesUsd".to_string(), subscriber.clone());

    // -------------------------------
    // Step 4: 初始化 Pipeline
    // -------------------------------
    let pipeline = Arc::new(MarketDataPipeline::new(subscribers, 1024));

    // -----------------------------
    // Step 5: 初始化 IngestorService
    // -----------------------------
    let shutdown = Arc::new(Notify::new());
    let service = Arc::new(IngestorService::new(
        backfill_scheduler.clone(),
        pipeline.clone(),
        shutdown.clone(),
    ));

    // -----------------------------
    // Step 6: 启动服务
    // -----------------------------
    println!("Starting IngestorService...");
    service.start(Some(config)).await;

    // -----------------------------
    // Step 7: 运行服务一定时间，观察输出
    // -----------------------------
    let run_duration = Duration::from_secs(60);
    println!("IngestorService running for {:?}...", run_duration);
    sleep(run_duration).await;

    // -----------------------------
    // Step 7: 查看健康状态
    // -----------------------------
    let health = service.health_status().await;
    println!("Ingestor Health: {:#?}", health);

    // -----------------------------
    // Step 8: Graceful shutdown
    // -----------------------------
    println!("Stopping IngestorService...");
    service.stop().await;

    println!("IngestorService stopped cleanly.");
    Ok(())

    // let service = Arc::new(IngestorService::with_params(...));
    // service.start(Some(config)).await;
}

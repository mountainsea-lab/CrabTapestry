//! examples/ingestor_service_example

use crab_hmds::ingestor::ctrservice::control_service::IngestorService;
use crab_hmds::ingestor::ctrservice::{ControlMsg, ServiceParams};
use crab_hmds::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
use crab_hmds::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crab_hmds::ingestor::realtime::subscriber::RealtimeSubscriber;
use crab_hmds::ingestor::realtime::subscriber::binance_subscriber::BinanceSubscriber;
use crab_hmds::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crab_hmds::ingestor::scheduler::service::InMemoryBackfillMetaStore;
use crab_hmds::ingestor::scheduler::service::historical_backfill_service::HistoricalBackfillService;
use crab_hmds::load_app_config;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, broadcast};
use tokio::time::sleep;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    ms_tracing::setup_tracing();

    let app_config = load_app_config().expect("系统应用配置信息读取失败");
    let lookback_days = app_config.app.lookback_days;

    // -------------------------------
    // 1️⃣ 初始化历史数据维护服务
    // -------------------------------
    let meta_store = Arc::new(InMemoryBackfillMetaStore::new());
    let fetcher = Arc::new(BinanceFetcher::new());
    let scheduler = BaseBackfillScheduler::new(fetcher.clone(), 3); // retry_limit=3

    let back_fill_service = Arc::new(HistoricalBackfillService::new(
        scheduler.clone(),
        meta_store.clone(),
        4, // default_max_batch_hours
        3, // max_retries
        lookback_days,
    ));

    // -------------------------------
    // Step 2: 初始化 实时数据维护服务pipeline
    // -------------------------------
    // 创建 broadcast channel
    let (broadcast_tx, _broadcast_rx) = broadcast::channel(1024);
    let subscriber = Arc::new(BinanceSubscriber::new(broadcast_tx.clone()));

    let mut subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>> = HashMap::new();
    subscribers.insert("BinanceFuturesUsd".to_string(), subscriber.clone());

    let real_time_service = MarketDataPipeline::new(subscribers, 1024);

    // -------------------------------
    // Step 3: 初始化 ingestor_service
    // -------------------------------
    // Graceful shutdown 通知
    let shutdown = Arc::new(Notify::new());

    // 服务参数
    let params = ServiceParams {
        control_channel_size: 1024,
        internal_channel_size: 64,
        dedup_window_ms: 60_000,
        buffer_cap_ohlcv: 1000,
        buffer_batch_size_ohlcv: 10,
        buffer_cap_tick: 10_000,
        buffer_batch_size_tick: 50,
        buffer_cap_trade: 10_000,
        buffer_batch_size_trade: 50,
    };

    let ingestor_service =
        IngestorService::with_params_and_subscriptions(back_fill_service, real_time_service, shutdown.clone(), params)
            .await;

    // 启动服务任务
    let service_clone = ingestor_service.clone();
    tokio::spawn(async move {
        service_clone.start().await;
    });

    // 模拟发送控制消息
    let control_tx = ingestor_service.control_tx.clone();
    control_tx.send(ControlMsg::Start).await?;

    // 模拟运行一段时间
    sleep(Duration::from_secs(10)).await;

    // 发送 HealthCheck 请求
    control_tx.send(ControlMsg::HealthCheck).await?;

    // 模拟动态新增订阅
    // control_tx.send(ControlMsg::AddSubscriptions(vec![Subscription { ... }])).await?;

    // 模拟停止服务
    // control_tx.send(ControlMsg::Stop).await?;

    // 等待服务优雅停止
    // sleep(Duration::from_secs(2)).await;

    // -----------------------------------------
    // 4️⃣ 模拟一直运行，周期打印 HealthCheck
    // -----------------------------------------
    loop {
        sleep(Duration::from_secs(10)).await;

        // 发送 HealthCheck
        control_tx.send(ControlMsg::HealthCheck).await?;

        // 打印 buffer 长度
        let ohlcv_len = ingestor_service.buffer_ohlcv.len();
        println!("[HealthCheck] buffer sizes: OHLCV={}", ohlcv_len);
    }

    // println!("IngestorService 已停止");
    //
    // Ok(())
}

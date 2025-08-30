use crab_data_event::aggregator::trade_aggregator::TradeAggregatorPool;
use crab_data_event::aggregator::types::BaseBar;
use crab_data_event::ingestion::Ingestor;
use crab_data_event::ingestion::barter_ingestor::BarterIngestor;
use crossbeam::channel::unbounded;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    ms_tracing::setup_tracing();

    let (sender, receiver) = unbounded();
    let ingestor = Arc::new(BarterIngestor::new(sender));

    // Clone the ingestor and start a non-Send subscribe
    // 克隆 ingestor 并启动非 Send 订阅
    let ingestor_clone = ingestor.clone();
    ingestor_clone.start();

    // 创建并启动 TradeAggregatorPool
    let trade_aggregator_pool = Arc::new(TradeAggregatorPool::new());
    let (output_tx, mut output_rx) = mpsc::channel::<BaseBar>(100);

    // 启动 worker 池
    trade_aggregator_pool.start_workers(4, Arc::new(receiver), output_tx);

    // 模拟接收处理完成后的 Bar 输出
    tokio::spawn(async move {
        while let Some(bar) = output_rx.recv().await {
            info!("Generated BaseBar: {:?}", bar);
        }
    });

    // 启动定时清理任务--目前不需要--注释
    // let pool_clone = trade_aggregator_pool.clone();
    // tokio::spawn(async move {
    //     pool_clone.start_cleanup_task(Duration::from_secs(60), Duration::from_secs(30));
    // });

    // 保持运行
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

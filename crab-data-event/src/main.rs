use std::sync::Arc;
use crossbeam::channel::unbounded;
use ms_tracing::tracing_utils::internal::info;
use crab_data_event::ingestion::barter_ingestor::BarterIngestor;
use crab_data_event::ingestion::Ingestor;

#[tokio::main]
async fn main() {
    ms_tracing::setup_tracing();

    let (sender, receiver) = unbounded();
    let ingestor = Arc::new(BarterIngestor::new(sender));

    // Clone the ingestor and start a non-Send subscribe
    // 克隆 ingestor 并启动非 Send 订阅
    let ingestor_clone = ingestor.clone();
    ingestor_clone.start();

    // TODO: Use multiple threads on the consumer side to aggregate real-time trade data into bars
    // 待办：在消费端使用多线程处理 TradeAggregator，将实时 trade 数据聚合为 bar
    let receiver = Arc::new(receiver);
    for _ in 0..4 {
        let rx = receiver.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv() {
                // Log the received event
                // 打印接收到的事件
                info!("Received event: {:?}", event);
            }
        });
    }

}


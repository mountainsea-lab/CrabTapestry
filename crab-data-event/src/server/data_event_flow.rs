use crate::global::get_redis_store;
use crate::ingestion::Ingestor;
use crate::ingestion::barter_ingestor::BarterIngestor;
use crab_infras::aggregator::trade_aggregator::TradeAggregatorPool;
use crab_infras::cache::redis_helper::RedisPubSubHelper;
use crab_infras::cache::{BaseBar, RedisMessage};
use crossbeam::channel::unbounded;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn start_data_event_flow() {
    let (sender, receiver) = unbounded();
    let ingestor = Arc::new(BarterIngestor::new(sender));

    // 启动订阅
    ingestor.clone().start();

    // 启动聚合池
    let aggregator_pool = Arc::new(TradeAggregatorPool::new());
    let (output_tx, mut output_rx) = mpsc::channel::<BaseBar>(100);
    aggregator_pool.start_workers_bridge(4, Arc::new(receiver), output_tx, Arc::new(vec![Arc::from("1m")]));

    // Redis 发布器
    let redis_cache = get_redis_store().expect("get_redis_store failed");
    let bar_puber = RedisPubSubHelper::new_puber(redis_cache);

    // 发布循环
    tokio::spawn(async move {
        while let Some(bar) = output_rx.recv().await {
            let channel = Arc::new("test_channel".to_string());
            let payload = serde_json::to_string(&bar).unwrap();

            let redis_message = RedisMessage {
                message_type: "message".into(),
                channel: channel.clone().to_string(),
                payload,
            };

            let serialized = serde_json::to_string(&redis_message).unwrap();
            info!("[publish  message]: {}", serialized);
            let _ = bar_puber.publish_message(channel, &serialized).await;
        }
    });

    // 如果需要后台定时任务，也可以在这里 spawn
    // let pool_clone = aggregator_pool.clone();
    // tokio::spawn(async move {
    //     pool_clone.start_cleanup_task(Duration::from_secs(60), Duration::from_secs(30));
    // });
}

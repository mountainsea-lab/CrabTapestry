use crab_data_event::aggregator::trade_aggregator::TradeAggregatorPool;
use crab_data_event::global::{get_redis_store, init_global_services};
use crab_data_event::ingestion::Ingestor;
use crab_data_event::ingestion::barter_ingestor::BarterIngestor;
use crab_infras::cache::redis_helper::RedisPubSubHelper;
use crab_infras::cache::{BaseBar, RedisMessage};
use crossbeam::channel::unbounded;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    ms_tracing::setup_tracing();
    // init global comments domain
    init_global_services().await.expect("Failed to initialize tracing service");

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

    // 创建消息发布器
    let redis_cache = get_redis_store().expect("get_redis_store failed");
    let bar_puber = RedisPubSubHelper::new_puber(redis_cache);
    // 模拟接收处理完成后的 Bar 输出
    tokio::spawn(async move {
        while let Some(bar) = output_rx.recv().await {
            // 创建一个频道
            let channel = Arc::new(String::from("test_channel"));

            // 将 BaseBar 转为 JSON 字符串
            let bar_payload = serde_json::to_string(&bar).unwrap();

            // 创建 RedisMessage 对象
            let redis_message = RedisMessage {
                message_type: "message".to_string(),
                channel: channel.clone().to_string(),
                payload: bar_payload, // 存储序列化后的 BaseBar
            };

            // 序列化 RedisMessage
            let serialized_message = serde_json::to_string(&redis_message).unwrap();
            let _publish_res = bar_puber.publish_message(channel, &serialized_message).await;
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

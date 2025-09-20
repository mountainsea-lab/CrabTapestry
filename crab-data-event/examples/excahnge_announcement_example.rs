use crab_infras::external::news::announcement::binance::{
    BinanceAnnouncement, BinanceAnnouncementConnector, BinanceAnnouncementTransformer,
};
use crab_infras::external::news::announcement::okx::{
    OkxAnnouncement, OkxAnnouncementConnector, OkxAnnouncementTransformer,
};
use crab_infras::external::news::announcement_manager::AnnouncementManager;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    // 1️⃣ 创建统一广播管理器，历史缓存容量 100
    let manager = AnnouncementManager::new(100);

    // 2️⃣ 创建 Binance Connector 并订阅
    let binance_connector = BinanceAnnouncementConnector::new("general");
    manager
        .subscribe::<BinanceAnnouncementConnector, BinanceAnnouncementTransformer, BinanceAnnouncement>(
            binance_connector,
        )
        .await;

    // 3️⃣ 创建 OKX Connector 并订阅
    let okx_connector = OkxAnnouncementConnector::new("notice");
    manager
        .subscribe::<OkxAnnouncementConnector, OkxAnnouncementTransformer, OkxAnnouncement>(okx_connector)
        .await;

    // 4️⃣ 前端订阅统一流
    let mut rx = manager.subscribe_frontend();

    // 5️⃣ 消费公告
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            println!(
                "📰 [{}] {} - {} (catalog_id: {})",
                event.exchange, event.topic, event.title, event.catalog_id
            );
        }
    });

    // 6️⃣ 模拟运行，保持主线程不退出
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

use async_trait::async_trait;
use barter_integration::Transformer;
use barter_integration::error::SocketError;
use barter_integration::protocol::websocket::WsMessage;
use tokio::sync::mpsc;

pub mod announcement;

#[derive(Debug, Clone)]
pub struct AnnouncementEvent {
    pub exchange: String,  // binance / okx / bybit ...
    pub topic: String,     // 公告频道
    pub title: String,     // 公告标题
    pub publish_date: u64, // 时间戳
    pub catalog_id: u64,   // 分类 ID (如: 上币 = 48)
}

#[async_trait]
pub trait AnnouncementTransformer: Transformer<Output = AnnouncementEvent, Error = SocketError> + Sized {
    /// 初始化 Transformer（可做订阅消息发送、状态同步等）
    async fn init(ws_sink_tx: mpsc::UnboundedSender<WsMessage>) -> Result<Self, SocketError>;
}

use async_trait::async_trait;
use barter_integration::Transformer;
use barter_integration::error::SocketError;
use barter_integration::protocol::websocket::WsMessage;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc};

pub mod announcement;
pub mod announcement_cache;
pub mod announcement_manager;

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
    /// 初始化 Transformer
    ///
    /// # 参数
    /// - ws_sink_tx: WebSocket Sink，用于发送订阅/心跳消息
    /// - initial_cache: 可选的历史公告缓存
    /// - topic: 对应交易所公告 topic
    async fn init(
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        initial_cache: Option<Vec<AnnouncementEvent>>,
        topic: Option<String>,
    ) -> Result<Self, SocketError>;
}

/// 泛型 Exchange 公告 Connector
#[async_trait]
pub trait ExchangeAnnouncementConnector<T, I>
where
    // T 是 Transformer 类型，Input = I，Output = AnnouncementEvent
    T: AnnouncementTransformer
        + Transformer<
            Input = I,
            Output = AnnouncementEvent,
            Error = SocketError,
            OutputIter = Vec<Result<AnnouncementEvent, SocketError>>,
        > + Send
        + 'static,
    I: Send + 'static,
{
    /// 运行 Connector，将公告发送到广播 channel，并维护历史缓存
    async fn run(
        &self,
        announcement_tx: broadcast::Sender<AnnouncementEvent>,
        cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
    ) -> Result<(), SocketError>;
}

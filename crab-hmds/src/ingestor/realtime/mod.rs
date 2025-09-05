mod aggregator;
mod market_data_pipe_line;

use crate::ingestor::types::PublicTradeEvent;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
// ---------- 实时数据订阅器接口 ----------
// ----------------- RealtimeSubscriber Interface  -----------------

/// 通用实时订阅接口
/// 通用实时订阅接口
#[async_trait]
pub trait RealtimeSubscriber: Send + Sync {
    fn exchange(&self) -> &str;

    /// 批量订阅 symbol，返回 TradeEvent channel
    async fn subscribe_symbols(&self, symbols: &[&str]) -> Result<mpsc::Receiver<PublicTradeEvent>, anyhow::Error>;

    /// 取消订阅部分 symbol
    async fn unsubscribe_symbols(&self, symbols: &[&str]) -> Result<(), anyhow::Error>;

    /// 停止订阅
    fn stop(&mut self) -> Result<(), anyhow::Error>;

    /// 查询状态
    fn status(&self) -> SubscriberStatus;
}

/// Subscriber 状态
#[derive(Debug, Clone)]
pub enum SubscriberStatus {
    Connected,
    Subscribed(Vec<String>),
    Disconnected,
    Error(String),
}

/// 实时订阅配置
pub struct Subscription {
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub periods: Vec<Arc<str>>, // 多周期 ["1m","5m","1h"]
}

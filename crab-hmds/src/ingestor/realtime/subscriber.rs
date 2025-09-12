pub mod binance_subscriber;

use crate::ingestor::realtime::SubscriberStatus;
use crate::ingestor::types::PublicTradeEvent;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 实时数据订阅器接口
/// realtime subscriber interface
#[async_trait]
pub trait RealtimeSubscriber: Send + Sync {
    fn exchange(&self) -> &str;

    /// 批量订阅 symbol，返回 TradeEvent channel
    async fn subscribe_symbols(
        self: Arc<Self>,
        symbols: &[&str],
    ) -> anyhow::Result<mpsc::Receiver<PublicTradeEvent>, anyhow::Error>;

    /// 取消订阅部分 symbol
    async fn unsubscribe_symbols(&self, symbols: &[&str]) -> anyhow::Result<(), anyhow::Error>;

    /// 停止订阅
    fn stop(&mut self) -> anyhow::Result<(), anyhow::Error>;

    /// 查询状态
    fn status(&self) -> SubscriberStatus;
}

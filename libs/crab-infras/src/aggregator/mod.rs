use crate::aggregator::types::TradeCandle;
use futures::SinkExt;
use tokio::sync::{broadcast, mpsc};

pub mod trade_aggregator;
pub mod types;

/// 泛型输出 trait约束
pub trait AggregatorOutput: Send + Sync + Clone + 'static {
    fn from_trade_candle(exchange: &str, symbol: &str, period: &str, timestamp: i64, candle: TradeCandle) -> Self;
}

#[async_trait::async_trait]
pub trait OutputSink: Clone + Send + Sync + 'static {
    type Item: AggregatorOutput;

    async fn send(&self, item: Self::Item);
}

#[async_trait::async_trait]
impl<T> OutputSink for mpsc::Sender<T>
where
    T: AggregatorOutput,
{
    type Item = T;

    async fn send(&self, item: T) {
        let _ = self.send(item).await;
    }
}

#[async_trait::async_trait]
impl<T> OutputSink for broadcast::Sender<T>
where
    T: AggregatorOutput,
{
    type Item = T;

    async fn send(&self, item: T) {
        let _ = self.send(item);
    }
}

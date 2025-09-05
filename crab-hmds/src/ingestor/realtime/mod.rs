mod aggregator;
mod market_data_pipe_line;
mod subscriber;

use std::sync::Arc;

/// Subscriber 状态
#[derive(Debug, Clone)]
pub enum SubscriberStatus {
    Connected,
    Subscribed(Vec<String>),
    Disconnected,
    Error(String),
}

/// 实时订阅配置
#[derive(Debug, Clone)]
pub struct Subscription {
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub periods: Vec<Arc<str>>, // 多周期 ["1m","5m","1h"]
}

pub mod market_data_pipe_line;
pub mod subscriber;

/// Subscriber 状态
#[derive(Debug, Clone, PartialEq)]
pub enum SubscriberStatus {
    Connected,
    Subscribed(Vec<String>),
    Disconnected,
    Error(String),
}

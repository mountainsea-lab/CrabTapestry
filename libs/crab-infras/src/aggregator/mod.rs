use crate::aggregator::types::TradeCandle;

pub mod trade_aggregator;
pub mod types;

/// 泛型输出 trait约束
pub trait AggregatorOutput: From<TradeCandle> + Send + 'static {}

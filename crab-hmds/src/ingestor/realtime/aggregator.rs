mod multi_period_aggregator;
mod single_aggregator;

use crate::ingestor::types::{OHLCVRecord, PublicTradeEvent};

/// 单周期聚合器
pub trait Aggregator: Send {
    fn period(&self) -> &str;
    fn on_event(&mut self, event: PublicTradeEvent) -> Option<OHLCVRecord>;
}

// 多周期聚合区器
pub trait MultiPeriodAggregator: Send {
    fn periods(&self) -> &[&str];
    fn on_event(&mut self, event: PublicTradeEvent) -> Vec<OHLCVRecord>;
}

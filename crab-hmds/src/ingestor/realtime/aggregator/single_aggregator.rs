use crate::ingestor::types::{OHLCVRecord, PublicTradeEvent};
use std::sync::Arc;

/// 单周期聚合器
pub struct SingleAggregator {
    period: Arc<str>, // "1m", "5m", ...
}

impl SingleAggregator {
    pub fn new(period: Arc<str>) -> Self {
        Self { period }
    }

    /// 输入一个 tick/trade，返回周期完成的 OHLCVRecord
    pub fn on_event(&mut self, event: PublicTradeEvent) -> Option<OHLCVRecord> {
        // TODO: 聚合逻辑，判断是否结束一个周期
        None
    }
}

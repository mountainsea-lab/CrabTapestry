use crate::ingestor::realtime::aggregator::single_aggregator::SingleAggregator;
use crate::ingestor::types::{OHLCVRecord, PublicTradeEvent};
use std::collections::HashMap;
use std::sync::Arc;

/// 多周期聚合器
pub struct MultiPeriodAggregator {
    exchange: Arc<str>,
    symbol: Arc<str>,
    aggregators: HashMap<Arc<str>, SingleAggregator>, // period -> SingleAggregator
}

impl MultiPeriodAggregator {
    pub fn new(exchange: Arc<str>, symbol: Arc<str>, periods: Vec<Arc<str>>) -> Self {
        let mut aggs = HashMap::new();
        for period in periods {
            aggs.insert(period.clone(), SingleAggregator::new(period));
        }
        Self { exchange, symbol, aggregators: aggs }
    }

    /// 输入一个 TradeEvent，返回完成周期的 OHLCVRecord 列表
    pub fn on_event(&mut self, event: PublicTradeEvent) -> Vec<OHLCVRecord> {
        let mut completed = Vec::new();

        for agg in self.aggregators.values_mut() {
            if let Some(bar) = agg.on_event(event.clone()) {
                completed.push(bar);
            }
        }

        completed
    }

    /// 返回支持的周期列表
    pub fn periods(&self) -> Vec<Arc<str>> {
        self.aggregators.keys().cloned().collect()
    }
}

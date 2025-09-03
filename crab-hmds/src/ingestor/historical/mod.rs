use crate::ingestor::types::OHLCVRecord;
use crate::ingestor::TimeRange;
use anyhow::Result;
#[async_trait::async_trait]
pub trait HistoricalFetcher: Send + Sync {
    /// 拉取指定时间范围历史数据
    /// pull historical data within the specified time range
    async fn fetch_range(&self, symbol: &str, period: &str, range: TimeRange) -> Result<Vec<OHLCVRecord>>;
}
use crate::ingestor::types::{FetchContext, OHLCVRecord, TickRecord};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;

/// 历史数据拉取接口
#[async_trait]
pub trait HistoricalFetcher: Send + Sync {
    /// 拉取 OHLCV 数据（批量模式）
    /// pull OHLCV data in batch mode
    async fn fetch_ohlcv(&self, ctx: FetchContext<'_>) -> Result<Vec<OHLCVRecord>>;

    /// 拉取 Tick 数据（批量模式）
    /// pull tick data in batch mode
    async fn fetch_ticks(&self, ctx: FetchContext<'_>) -> Result<Vec<TickRecord>>;

    /// 拉取 OHLCV 数据（流式模式，适合大规模回溯）
    /// pull OHLCV data in stream mode, suitable for large-scale backfilling
    async fn fetch_ohlcv_stream(&self, ctx: FetchContext<'_>) -> Result<BoxStream<'static, Result<OHLCVRecord>>>;

    /// 拉取 Tick 数据（流式模式）
    /// pull tick data in stream mode
    async fn fetch_ticks_stream(&self, ctx: FetchContext<'_>) -> Result<BoxStream<'static, Result<TickRecord>>>;
}

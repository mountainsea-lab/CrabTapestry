pub mod fetcher;

use crate::ingestor::types::{FetchContext, HistoricalBatch, OHLCVRecord, TickRecord};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use std::sync::Arc;

/// 历史数据拉取接口
/// Historical data fetcher interface
#[async_trait]
pub trait HistoricalFetcher: Send + Sync {
    /// 拉取 OHLCV 数据（流式模式，适合大规模回溯）
    /// pull OHLCV data in stream mode, suitable for large-scale backfilling
    async fn stream_ohlcv(&self, ctx: Arc<FetchContext>) -> Result<BoxStream<'static, Result<OHLCVRecord>>>;

    /// 拉取 Tick 数据（流式模式）
    /// pull tick data in stream mode
    async fn stream_ticks(&self, ctx: Arc<FetchContext>) -> Result<BoxStream<'static, Result<TickRecord>>>;
}

/// Extension trait: provides batch fetch by collecting stream
#[async_trait]
pub trait HistoricalFetcherExt: HistoricalFetcher {
    /// 拉取 OHLCV 数据（批量模式）
    /// pull OHLCV data in batch mode
    async fn fetch_ohlcv(&self, ctx: Arc<FetchContext>) -> Result<HistoricalBatch<OHLCVRecord>> {
        let mut stream = self.stream_ohlcv(ctx.clone()).await?;
        let mut data = Vec::new();
        while let Some(item) = stream.next().await {
            data.push(item?);
        }
        Ok(HistoricalBatch {
            symbol: ctx.symbol.clone(),
            exchange: ctx.exchange.clone(),
            period: ctx.period.clone(), // ✅ 直接 clone Arc
            range: ctx.range.clone(),
            data,
        })
    }

    /// 拉取 Tick 数据（批量模式）
    /// pull tick data in batch mode
    async fn fetch_ticks(&self, ctx: Arc<FetchContext>) -> Result<HistoricalBatch<TickRecord>> {
        let mut stream = self.stream_ticks(ctx.clone()).await?;
        let mut data = Vec::new();
        while let Some(item) = stream.next().await {
            data.push(item?);
        }
        Ok(HistoricalBatch {
            symbol: ctx.symbol.clone(),
            exchange: ctx.exchange.clone(),
            period: ctx.period.clone(), // ✅ 直接 clone Arc
            range: ctx.range.clone(),
            data,
        })
    }
}

/// Blanket impl so all `HistoricalFetcher` automatically get batch mode.
impl<T: HistoricalFetcher + ?Sized> HistoricalFetcherExt for T {}

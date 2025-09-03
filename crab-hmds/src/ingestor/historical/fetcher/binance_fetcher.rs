use crate::ingestor::historical::HistoricalFetcher;
use crate::ingestor::types::{FetchContext, OHLCVRecord, TickRecord};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, stream, stream::BoxStream};
use std::sync::Arc;

/// Binance 历史数据拉取器
pub struct BinanceFetcher {
    // todo use crab infras
    client: String,
}

impl BinanceFetcher {
    pub fn new() -> Self {
        Self { client: String::from("client_id") }
    }

    /// 内部方法：拉取单个分页 OHLCV
    async fn fetch_ohlcv_page(&self, ctx: &FetchContext<'_>, start_ts: i64, end_ts: i64) -> Result<Vec<OHLCVRecord>> {
        // TODO: 调用 Binance API，例如 Kline Endpoint
        // https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=xxx&endTime=xxx&limit=1000
        // 这里只返回空 Vec 作为示例
        Ok(Vec::new())
    }

    /// 内部方法：拉取单个分页 Tick
    async fn fetch_ticks_page(&self, ctx: &FetchContext<'_>, start_ts: i64, end_ts: i64) -> Result<Vec<TickRecord>> {
        // TODO: 调用 Binance Trade History API
        Ok(Vec::new())
    }
}

#[async_trait]
impl HistoricalFetcher for BinanceFetcher {
    /// 流式拉取 OHLCV
    async fn stream_ohlcv(&self, ctx: Arc<FetchContext<'_>>) -> Result<BoxStream<'static, Result<OHLCVRecord>>> {
        // 分批拉取，每批 1000 条（Binance 限制）
        let chunk_ms = 60 * 60 * 1000; // 1小时为例
        let ranges = ctx.range.split(chunk_ms);
        let client = self.client.clone();
        let ctx_clone = ctx.clone();

        // 创建异步 Stream
        let s = stream::iter(ranges.into_iter())
            .then(move |range| {
                let ctx = ctx_clone.clone();
                let client = client.clone();
                async move {
                    let fetcher = BinanceFetcher { client };
                    fetcher.fetch_ohlcv_page(&ctx, range.start, range.end).await
                }
            })
            .flat_map(|res| match res {
                Ok(vec) => stream::iter(vec.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            });

        Ok(s.boxed())
    }

    /// 流式拉取 Tick
    async fn stream_ticks(&self, ctx: Arc<FetchContext<'_>>) -> Result<BoxStream<'static, Result<TickRecord>>> {
        let chunk_ms = 60 * 60 * 1000; // 1小时为例
        let ranges = ctx.range.split(chunk_ms);
        let client = self.client.clone();
        let ctx_clone = ctx.clone();

        let s = stream::iter(ranges.into_iter())
            .then(move |range| {
                let ctx = ctx_clone.clone();
                let client = client.clone();
                async move {
                    let fetcher = BinanceFetcher { client };
                    fetcher.fetch_ticks_page(&ctx, range.start, range.end).await
                }
            })
            .flat_map(|res| match res {
                Ok(vec) => stream::iter(vec.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            });

        Ok(s.boxed())
    }
}

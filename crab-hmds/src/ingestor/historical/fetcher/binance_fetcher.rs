use crate::ingestor::historical::HistoricalFetcher;
use crate::ingestor::types::{FetchContext, OHLCVRecord, TickRecord};
use anyhow::Result;
use async_trait::async_trait;
use crab_infras::external::binance::DefaultBinanceExchange;
use crab_infras::external::binance::market::KlineSummary;
use futures_util::{StreamExt, stream, stream::BoxStream};
use std::sync::Arc;

/// Binance 历史数据拉取器
/// Binance historical data fetcher
pub struct BinanceFetcher {
    client: Arc<DefaultBinanceExchange<'static>>,
}

impl BinanceFetcher {
    pub fn new() -> Self {
        Self {
            client: Arc::new(DefaultBinanceExchange::default()),
        }
    }

    /// 内部方法：拉取单个分页 OHLCV
    /// internal method: fetch single page of OHLCV
    async fn fetch_ohlcv_page(&self, ctx: &FetchContext, start_ts: i64, end_ts: i64) -> Result<Vec<OHLCVRecord>> {
        let symbol = ctx.symbol.clone();
        let interval = ctx.period.clone().unwrap_or_else(|| Arc::from("1m"));

        // 调用 BinanceExchange 获取 KlineSummary
        let kline_summaries: Vec<KlineSummary> = self
            .client
            .get_klines(
                symbol.as_ref(),
                interval.as_ref(),
                Some(ctx.limit),
                Some(start_ts as u64),
                Some(end_ts as u64),
            )
            .await;

        // 转换 KlineSummary -> OHLCVRecord
        let records: Vec<OHLCVRecord> = kline_summaries
            .into_iter()
            .map(|k| OHLCVRecord {
                ts: k.close_time,
                period_start_ts: Some(k.open_time),
                open: k.open,
                high: k.high,
                low: k.low,
                close: k.close,
                volume: k.volume,
                turnover: Some(k.quote_asset_volume),
                num_trades: Some(k.number_of_trades as u32),
                vwap: if k.volume != 0.0 {
                    Some(k.quote_asset_volume / k.volume)
                } else {
                    None
                },
                symbol: ctx.symbol.clone(),
                exchange: ctx.exchange.clone(),
                period: interval.as_ref().to_string(),
            })
            .collect();

        Ok(records)
    }

    /// 内部方法：拉取单个分页 Tick
    /// internal method: fetch single page of ticks
    async fn fetch_ticks_page(&self, _ctx: &FetchContext, _start_ts: i64, _end_ts: i64) -> Result<Vec<TickRecord>> {
        // TODO: 调用 Binance Trade History API
        Ok(Vec::new())
    }
}

#[async_trait]
impl HistoricalFetcher for BinanceFetcher {
    /// 流式拉取 OHLCV
    /// stream pull ohlcv from exchange
    async fn stream_ohlcv(&self, ctx: Arc<FetchContext>) -> Result<BoxStream<'static, Result<OHLCVRecord>>> {
        let client = Arc::clone(&self.client);

        let s = stream::once({
            let ctx = ctx.clone();
            async move {
                let fetcher = BinanceFetcher { client };
                fetcher.fetch_ohlcv_page(&ctx, ctx.range.start, ctx.range.end).await
            }
        })
        .flat_map(|res| match res {
            Ok(vec) => stream::iter(vec.into_iter().map(Ok)).boxed(),
            Err(e) => stream::iter(vec![Err(e)]).boxed(),
        });

        Ok(s.boxed())
    }

    /// 流式拉取 Tick
    /// stream pull ticks from exchange
    async fn stream_ticks(&self, ctx: Arc<FetchContext>) -> Result<BoxStream<'static, Result<TickRecord>>> {
        let chunk_ms = 60 * 60 * 1000; // 每小时为例
        let ranges = ctx.range.split(chunk_ms);
        let ctx_clone = ctx.clone();
        let client = Arc::clone(&self.client);

        let s = stream::iter(ranges.into_iter())
            .then(move |range| {
                let ctx = ctx_clone.clone();
                let client = Arc::clone(&client);
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

// ---------------------- 测试 ----------------------
// ---------------------- test ----------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestor::historical::HistoricalFetcherExt;
    use anyhow::Result;
    use crab_types::TimeRange;

    #[tokio::test]
    async fn test_fetch_ohlcv_pipeline() -> Result<()> {
        // 构造 FetchContext
        let ctx = Arc::new(FetchContext::new(
            Some(1),
            "binance",
            "BTCUSDT",
            "USDT",
            Some("1h"),
            TimeRange::new(100000, 120000), // 最近 3 小时
            500,                            // 最近 3 小时, // 3 小时
        ));

        let fetcher = BinanceFetcher::new();

        // 批量拉取 OHLCV
        let batch = fetcher.fetch_ohlcv(ctx.clone()).await?;

        // 打印输出，确保字段被使用
        println!("HistoricalBatch: {:#?}", batch);

        // 简单断言，保证数据条数正确
        assert_eq!(batch.data.len(), 3); // 3 个小时 -> 3 条记录

        Ok(())
    }
}

use crate::external::CommonExternalParser;
use crate::external::binance::market::{FetchKlineSummaryRequest, KlineSummary};
use crate::external::binance::meta::{BinanceExchangeInfo, FetchExchangeInfoRequest, Symbol};
use barter_integration::error::SocketError;
use barter_integration::protocol::http::rest::RestRequest;
use barter_integration::protocol::http::rest::client::RestClient;
use barter_integration::protocol::http::{BuildStrategy, HttpParser};
use ms_tracing::tracing_utils::internal::{error, warn};
use reqwest::RequestBuilder;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

mod constant;
pub mod market;
pub mod meta;

const MAX_RETRIES: usize = 3;
const RETRY_BASE_DELAY_MS: u64 = 500; // 初始重试延迟 0.5 秒

pub struct BinanceSigner;
impl BuildStrategy for BinanceSigner {
    fn build<Request>(&self, _request: Request, builder: RequestBuilder) -> Result<reqwest::Request, SocketError>
    where
        Request: RestRequest,
    {
        builder.build().map_err(SocketError::from)
    }
}

pub struct BinanceExchange<'a, Strategy, Parser>
where
    Strategy: BuildStrategy,
    Parser: HttpParser,
{
    rest_client: Arc<RestClient<'a, Strategy, Parser>>,
}

impl<'a, Strategy, Parser> Clone for BinanceExchange<'a, Strategy, Parser>
where
    Strategy: BuildStrategy,
    Parser: HttpParser,
{
    fn clone(&self) -> Self {
        Self {
            rest_client: Arc::clone(&self.rest_client),
        }
    }
}

pub type DefaultBinanceExchange<'a> = BinanceExchange<'a, BinanceSigner, CommonExternalParser>;

impl<'a> Default for DefaultBinanceExchange<'a> {
    fn default() -> Self {
        Self {
            rest_client: Arc::new(RestClient::new(constant::BASE_URL, BinanceSigner, CommonExternalParser)),
        }
    }
}

impl<'a, Strategy, Parser> BinanceExchange<'a, Strategy, Parser>
where
    Strategy: BuildStrategy,
    Parser: HttpParser,
    <Parser as HttpParser>::OutputError: Debug,
{
    pub fn new(strategy: Strategy, parser: Parser) -> Self
    where
        Strategy: BuildStrategy,
        Parser: HttpParser,
    {
        Self {
            rest_client: Arc::new(RestClient::new(constant::BASE_URL, strategy, parser)),
        }
    }

    pub async fn get_exchange_info(&self) -> Option<BinanceExchangeInfo> {
        let fetch_request = FetchExchangeInfoRequest;

        match self.rest_client.execute(fetch_request).await {
            Ok((response, _)) => Some(response.0),
            Err(err) => {
                error!("Failed to fetch exchange info: {:?}", err);
                None
            }
        }
    }

    pub async fn get_symbols(&self) -> Option<Vec<Symbol>> {
        let exchange_info = self.get_exchange_info().await;

        if exchange_info.is_some() {
            exchange_info.map(|exchange_info| exchange_info.symbols)?
        } else {
            None
        }
    }

    pub async fn get_klines<S1, S2, S3, S4, S5>(
        &self,
        symbol: S1,
        interval: S2,
        limit: S3,
        start_time: S4,
        end_time: S5,
    ) -> Vec<KlineSummary>
    where
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<Option<i32>>,
        S4: Into<Option<u64>>,
        S5: Into<Option<u64>>,
    {
        let mut params = BTreeMap::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("interval".into(), interval.into());

        if let Some(v) = limit.into() {
            params.insert("limit".into(), v.to_string());
        }
        if let Some(v) = start_time.into() {
            params.insert("startTime".into(), v.to_string());
        }
        if let Some(v) = end_time.into() {
            params.insert("endTime".into(), v.to_string());
        }

        let request = FetchKlineSummaryRequest { query_params: params };

        // 🧩 预先构造一个简单的重试延迟生成器
        let retry_delays = (0..MAX_RETRIES).map(|i| RETRY_BASE_DELAY_MS * 2u64.pow(i as u32));

        for (attempt, delay) in retry_delays.enumerate() {
            match self.rest_client.execute(request.clone()).await {
                Ok((response, _)) => {
                    //     debug!(
                    //     "Successfully fetched {} klines for attempt {}/{}",
                    //     response.0.len(),
                    //     attempt + 1,
                    //     MAX_RETRIES
                    // );
                    return response.0;
                }
                Err(err) => {
                    let err_str = format!("{:?}", err);
                    let retriable = err_str.contains("Timeout")
                        || err_str.contains("Connection")
                        || err_str.contains("Socket")
                        || err_str.contains("temporarily");

                    warn!("Fetch klines attempt {}/{} failed: {:?}", attempt + 1, MAX_RETRIES, err);

                    if retriable && attempt + 1 < MAX_RETRIES {
                        warn!("Will retry after {} ms...", delay);
                        sleep(Duration::from_millis(delay)).await;
                        continue;
                    } else {
                        error!("Aborting after {} failed attempts.", attempt + 1);
                        break;
                    }
                }
            }
        }

        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ms_tracing::tracing_utils::internal::debug;
    use ms_tracing::{setup_tracing, trace_kv};

    #[tokio::test]
    async fn test_get_exchange_info() {
        setup_tracing();
        let dbe = DefaultBinanceExchange::default();
        let exchange_info = dbe.get_exchange_info().await;
        match exchange_info {
            None => {
                debug!("Empty exchange info");
            }
            Some(exchange_info) => {
                trace_kv!(info,
                     "server_time" => exchange_info.server_time,
                     "timezone" => exchange_info.timezone,
                );
            }
        }
    }

    #[tokio::test]
    async fn test_get_symbols() {
        setup_tracing();
        let dbe = DefaultBinanceExchange::default();

        let symbols = dbe.get_symbols().await;
        match symbols {
            None => {
                debug!("Empty exchange info");
            }
            Some(symbols) => {
                for symbol in &symbols {
                    trace_kv!(info,
                     "symbol" => symbol.symbol,
                     "quote_asset" => symbol.quote_asset,
                     "contract_type" => symbol.contract_type,
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_get_klines() {
        setup_tracing();
        let dbe = DefaultBinanceExchange::default();
        let symbol = "btcusdt";
        let interval = "5m";
        let limit = 1;
        let klines = dbe.get_klines(symbol, interval, limit, None, None).await;

        for kline in &klines {
            trace_kv!(info,
             "open" => kline.open,
             "high" => kline.high,
             "open" => kline.low,
             "close" => kline.close,
             "close_time" => kline.close_time,
            );
        }
    }
}

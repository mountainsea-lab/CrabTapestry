use crate::external::CommonExternalParser;
use crate::external::crab_hmds::hmds::FetchCrabHmdsKlineRequest;
use crate::external::crab_hmds::meta::OhlcvRecord;
use barter_integration::error::SocketError;
use barter_integration::protocol::http::rest::RestRequest;
use barter_integration::protocol::http::rest::client::RestClient;
use barter_integration::protocol::http::{BuildStrategy, HttpParser};
use ms_tracing::tracing_utils::internal::error;
use reqwest::RequestBuilder;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

mod constant;
mod hmds;
mod meta;

pub struct HmdsSigner;
impl BuildStrategy for HmdsSigner {
    fn build<Request>(&self, _request: Request, builder: RequestBuilder) -> Result<reqwest::Request, SocketError>
    where
        Request: RestRequest,
    {
        builder.build().map_err(SocketError::from)
    }
}

pub struct HmdsExchange<'a, Strategy, Parser>
where
    Strategy: BuildStrategy,
    Parser: HttpParser,
{
    rest_client: Arc<RestClient<'a, Strategy, Parser>>,
}

impl<'a, Strategy, Parser> Clone for HmdsExchange<'a, Strategy, Parser>
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

pub type DefaultHmdsExchange<'a> = HmdsExchange<'a, HmdsSigner, CommonExternalParser>;

impl<'a> Default for DefaultHmdsExchange<'a> {
    fn default() -> Self {
        Self {
            rest_client: Arc::new(RestClient::new(constant::BASE_URL, HmdsSigner, CommonExternalParser)),
        }
    }
}

impl<'a, Strategy, Parser> HmdsExchange<'a, Strategy, Parser>
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

    pub async fn get_klines<S1, S2, S3, S4, S5>(
        &self,
        exchange: S1,
        symbol: S1,
        period: S2,
        limit: S3,
        start_time: S4,
        end_time: S5,
    ) -> Vec<OhlcvRecord>
    where
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<Option<i32>>,
        S4: Into<Option<u64>>,
        S5: Into<Option<u64>>,
    {
        let mut parameters: BTreeMap<String, String> = BTreeMap::new();
        parameters.insert("exchange".into(), exchange.into());
        parameters.insert("symbol".into(), symbol.into());
        parameters.insert("period".into(), period.into());

        // Add three optional parameters
        if let Some(lt) = limit.into() {
            parameters.insert("limit".into(), format!("{}", lt));
        }
        if let Some(st) = start_time.into() {
            parameters.insert("start_time".into(), format!("{}", st));
        }
        if let Some(et) = end_time.into() {
            parameters.insert("end_time".into(), format!("{}", et));
        }

        let fetch_klines_request = FetchCrabHmdsKlineRequest { query_params: parameters };
        // info!("Fetching kline summary client execute {:?}", fetch_klines_request);
        match self.rest_client.execute(fetch_klines_request).await {
            Ok((response, _)) => response.0,
            Err(err) => {
                error!("Failed to fetch coin data: {:?}", err);
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ms_tracing::tracing_utils::internal::debug;
    use ms_tracing::{setup_tracing, trace_kv};

    #[tokio::test]
    async fn test_get_klines() {
        setup_tracing();
        let dbe = DefaultHmdsExchange::default();
        let exchange = "BinanceFuturesUsd";
        let symbol = "btcusdt";
        let period = "5m";
        let limit = 2;
        let klines = dbe.get_klines(exchange, symbol, period, limit, None, None).await;

        for kline in &klines {
            trace_kv!(info,
             "open" => kline.open,
             "high" => kline.high,
             "open" => kline.low,
             "close" => kline.close,
             "close_time" => kline.ts,
            );
        }
    }
}

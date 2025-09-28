use crate::external::binance::constant;
use barter_integration::protocol::http::rest::RestRequest;
use ms_tracing::tracing_utils::serde_fun::{ParseError, parse_field};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// 表示一根 K 线（蜡烛图）汇总数据
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KlineSummary {
    /// K 线开盘时间（Unix 时间戳，毫秒）
    pub open_time: i64,

    /// 开盘价
    pub open: f64,

    /// 最高价
    pub high: f64,

    /// 最低价
    pub low: f64,

    /// 收盘价（该 K 线期间最后一笔成交价）
    pub close: f64,

    /// 成交量（以基础资产计）
    pub volume: f64,

    /// K 线收盘时间（Unix 时间戳，毫秒）
    pub close_time: i64,

    /// 成交额（以报价资产计）
    pub quote_asset_volume: f64,

    /// 成交笔数
    pub number_of_trades: i64,

    /// 主动买入的成交量（以基础资产计）
    pub taker_buy_base_asset_volume: f64,

    /// 主动买入的成交额（以报价资产计）
    pub taker_buy_quote_asset_volume: f64,
}

impl TryFrom<&Vec<Value>> for KlineSummary {
    type Error = ParseError;

    fn try_from(row: &Vec<Value>) -> Result<Self, Self::Error> {
        Ok(Self {
            open_time: parse_field(row, 0, "open_time")?,
            open: parse_field(row, 1, "open")?,
            high: parse_field(row, 2, "high")?,
            low: parse_field(row, 3, "low")?,
            close: parse_field(row, 4, "close")?,
            volume: parse_field(row, 5, "volume")?,
            close_time: parse_field(row, 6, "close_time")?,
            quote_asset_volume: parse_field(row, 7, "quote_asset_volume")?,
            number_of_trades: parse_field(row, 8, "number_of_trades")?,
            taker_buy_base_asset_volume: parse_field(row, 9, "taker_buy_base_asset_volume")?,
            taker_buy_quote_asset_volume: parse_field(row, 10, "taker_buy_quote_asset_volume")?,
        })
    }
}
#[derive(Debug)]
pub struct FetchKlineSummaryRequest {
    pub(crate) query_params: BTreeMap<String, String>,
}

impl RestRequest for FetchKlineSummaryRequest {
    type Response = BinanceKlineSummaryResponse;
    type QueryParams = BTreeMap<String, String>;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(constant::KLINES)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.query_params)
    }
}

#[derive(Debug)]
pub struct BinanceKlineSummaryResponse(pub Vec<KlineSummary>);
impl<'de> Deserialize<'de> for BinanceKlineSummaryResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw: Vec<Vec<Value>> = Vec::deserialize(deserializer)?;
        let klines = raw
            .into_iter()
            .map(|row| KlineSummary::try_from(&row).map_err(serde::de::Error::custom))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BinanceKlineSummaryResponse(klines))
    }
}

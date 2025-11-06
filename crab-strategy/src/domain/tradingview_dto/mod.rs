use crab_infras::external::crab_hmds::meta::OhlcvRecord;
use serde::{Deserialize, Serialize};

/// 2️⃣ Datafeed 配置信息
#[derive(Serialize)]
pub struct TradingviewConfig {
    pub(crate) supports_search: bool,
    pub(crate) supports_group_request: bool,
    pub(crate) supported_resolutions: Vec<String>,
    pub(crate) supports_marks: bool,
    pub(crate) supports_timescale_marks: bool,
    pub(crate) supports_time: bool,
}

#[derive(Serialize)]
pub struct SymbolInfoUdf {
    pub(crate) name: String,
    pub(crate) ticker: String,
    pub(crate) description: String,
    pub(crate) session: String,
    pub(crate) exchange: String,
    pub(crate) minmov: i32,
    pub(crate) pricescale: i32,
    pub(crate) has_intraday: bool,
    pub(crate) supported_resolutions: Vec<String>,
    pub(crate) has_no_volume: bool,
    // 可选字段
    pub(crate) type_: String,
    pub(crate) currency_code: String,
}

#[derive(Deserialize)]
pub struct SymbolQuery {
    symbol: String,
}

/// 代表每个搜索结果的币种或交易对信息
#[derive(Serialize)]
pub struct SymbolSearchResult {
    pub(crate) name: String,
    pub(crate) ticker: String,
    pub(crate) description: Option<String>,
    pub(crate) exchange: String,
    pub(crate) type_: String,
    pub(crate) currency_code: String,
}

/// 请求查询参数
#[derive(Deserialize)]
pub struct SearchQuery {
    limit: Option<u32>,       // 限制返回结果数量
    query: String,            // 搜索关键词
    type_: Option<String>,    // 类型筛选，如 `crypto`
    exchange: Option<String>, // 交易所筛选，如 `BINANCE`
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    pub(crate) symbol: String,
    pub(crate) resolution: String,
    pub(crate) from: i64,
    pub(crate) to: i64,
    pub(crate) countback: Option<u32>,
    first_data_request: Option<bool>,
}

/// 将 Vec<OhlcvRecord> 转换为 TradingView UDF 列数组格式 JSON
pub fn bars_to_udf(bars: &[OhlcvRecord]) -> serde_json::Value {
    serde_json::json!({
        // 状态字段，bars 为空返回 "no_data"
        "s": if bars.is_empty() { "no_data" } else { "ok" },

        // TradingView UDF 要求的秒级 Unix 时间戳
        "t": bars.iter().map(|b| b.ts / 1000).collect::<Vec<i64>>(),

        // OHLCV 数据
        "o": bars.iter().map(|b| b.open).collect::<Vec<f64>>(),
        "h": bars.iter().map(|b| b.high).collect::<Vec<f64>>(),
        "l": bars.iter().map(|b| b.low).collect::<Vec<f64>>(),
        "c": bars.iter().map(|b| b.close).collect::<Vec<f64>>(),
        "v": bars.iter().map(|b| b.volume).collect::<Vec<f64>>(),

        // 可选字段：成交额/turnover，若无则用 0
        "w": bars.iter().map(|b| b.vwap.unwrap_or(0.0)).collect::<Vec<f64>>(),
        "v_total": bars.iter().map(|b| b.turnover.unwrap_or(0.0)).collect::<Vec<f64>>(),
    })
}

use crate::global;
use crate::server::response::ErrorResponse;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use warp::{Rejection, Reply, http::StatusCode, reply};

/// TradingView Datafeed 兼容接口

/// 1️⃣ 获取服务器时间
pub async fn get_time() -> Result<impl Reply, Rejection> {
    let now = Utc::now().timestamp();
    Ok(reply::json(&serde_json::json!({ "time": now })))
}

/// 2️⃣ Datafeed 配置信息
#[derive(Serialize)]
pub struct ConfigResponse {
    supports_search: bool,
    supports_group_request: bool,
    supported_resolutions: Vec<String>,
    supports_marks: bool,
    supports_timescale_marks: bool,
    supports_time: bool,
}

pub async fn get_config() -> Result<impl Reply, Rejection> {
    let config = ConfigResponse {
        supports_search: true,
        supports_group_request: false,
        supported_resolutions: vec![
            "1".into(),
            "5".into(),
            "15".into(),
            "60".into(),
            "1D".into(),
            "1W".into(),
        ],
        supports_marks: false,
        supports_timescale_marks: false,
        supports_time: true,
    };
    Ok(reply::json(&config))
}

/// 3️⃣ 查询币种信息 /symbols/{symbol}
#[derive(Serialize)]
pub struct SymbolInfoUdf {
    name: String,
    ticker: String,
    description: String,
    session: String,
    exchange: String,
    minmov: i32,
    pricescale: i32,
    has_intraday: bool,
    supported_resolutions: Vec<String>,
    has_no_volume: bool,
    // 可选字段
    type_: String,
    currency_code: String,
}

#[derive(Deserialize)]
pub struct SymbolQuery {
    symbol: String,
}
/// 查询币种信息
pub async fn get_symbol_info(query: SymbolQuery) -> Result<impl warp::Reply, warp::Rejection> {
    let symbol = query.symbol.to_uppercase();

    let info = SymbolInfoUdf {
        name: symbol.clone(),
        ticker: symbol.clone(),
        description: format!("{} trading pair", symbol),
        session: "24x7".into(),
        exchange: "BINANCE".into(),
        minmov: 1,
        pricescale: 100, // 表示 2 位小数
        has_intraday: true,
        supported_resolutions: vec![
            "1".into(),
            "5".into(),
            "15".into(),
            "60".into(),
            "1D".into(),
            "1W".into(),
        ],
        has_no_volume: false,
        type_: "crypto".into(),
        currency_code: "USDT".into(),
    };

    Ok(warp::reply::json(&info))
}

/// 代表每个搜索结果的币种或交易对信息
#[derive(Serialize)]
pub struct SymbolSearchResult {
    name: String,
    ticker: String,
    description: Option<String>,
    exchange: String,
    type_: String,
    currency_code: String,
}

/// 请求查询参数
#[derive(Deserialize)]
pub struct SearchQuery {
    limit: Option<u32>,       // 限制返回结果数量
    query: String,            // 搜索关键词
    type_: Option<String>,    // 类型筛选，如 `crypto`
    exchange: Option<String>, // 交易所筛选，如 `BINANCE`
}

/// 假设这是数据库或缓存中的币种/交易对数据
fn get_symbol_data(query: &str) -> Vec<SymbolSearchResult> {
    // 这里用模拟数据代替实际的数据查询逻辑
    let symbols = vec![
        SymbolSearchResult {
            name: "SOLUSDT".into(),
            ticker: "SOLUSDT".into(),
            description: Some("Solana / USDT trading pair".into()),
            exchange: "BINANCE".into(),
            type_: "crypto".into(),
            currency_code: "USDT".into(),
        },
        SymbolSearchResult {
            name: "SOLBTC".into(),
            ticker: "SOLBTC".into(),
            description: Some("Solana / BTC trading pair".into()),
            exchange: "BINANCE".into(),
            type_: "crypto".into(),
            currency_code: "BTC".into(),
        },
    ];

    symbols
        .into_iter()
        .filter(|symbol| symbol.name.contains(query)) // 根据查询字符串过滤
        .collect()
}
///  5️⃣ 模糊搜索币种 /search?query=BTC&type=crypto&exchange=BINANCE
/// http://localhost:10099/search?limit=30&query=SOLUSD&type=&exchange=BINANCE
pub async fn search_symbols(query: SearchQuery) -> Result<impl Reply, Rejection> {
    // 获取搜索结果，假设这里是从数据库或缓存获取的数据
    let results = get_symbol_data(&query.query);

    // 根据 limit 限制返回数量
    let limit = query.limit.unwrap_or(30) as usize;
    let limited_results = results.into_iter().take(limit).collect::<Vec<_>>();

    // 返回响应
    Ok(reply::json(&serde_json::json!(&limited_results)))
}

/// 4️⃣ 查询历史 K 线 /history?symbol=BTC/USDT&resolution=1&from=...&to=...
#[derive(Deserialize)]
pub struct HistoryQuery {
    symbol: String,
    resolution: String,
    from: i64,
    to: i64,
    countback: Option<u32>,
    first_data_request: Option<bool>,
}

#[derive(Serialize)]
pub struct Bar {
    time: i64, // Unix 秒时间戳
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

/// 将 Vec<Bar> 转换为 TradingView UDF 列数组格式 JSON
fn bars_to_udf(bars: &[Bar]) -> serde_json::Value {
    serde_json::json!({
        "s": "ok",
        "t": bars.iter().map(|b| b.time).collect::<Vec<i64>>(),
        "o": bars.iter().map(|b| b.open).collect::<Vec<f64>>(),
        "h": bars.iter().map(|b| b.high).collect::<Vec<f64>>(),
        "l": bars.iter().map(|b| b.low).collect::<Vec<f64>>(),
        "c": bars.iter().map(|b| b.close).collect::<Vec<f64>>(),
        "v": bars.iter().map(|b| b.volume).collect::<Vec<f64>>(),
    })
}

pub async fn get_history(query: HistoryQuery) -> Result<impl Reply, Rejection> {
    // 这里你可以替换成从数据库或缓存中加载真实历史 K 线
    // 暂时生成一些模拟数据：
    let mut bars = Vec::new();
    let mut t = query.from;
    while t <= query.to {
        let base = 1000.0 + ((t % 1000) as f64 / 100.0);
        bars.push(Bar {
            time: t,
            open: base,
            high: base + 1.0,
            low: base - 1.0,
            close: base + 0.5,
            volume: 10.0,
        });
        t += match query.resolution.as_str() {
            "1" => 60,
            "5" => 300,
            "15" => 900,
            "60" => 3600,
            "1D" => 86400,
            "1W" => 86400 * 7,
            _ => 60,
        };
    }

    if bars.is_empty() {
        Ok(reply::json(&serde_json::json!({ "s": "no_data" })))
    } else {
        // 直接返回平铺的结构
        let response = bars_to_udf(&bars);
        Ok(reply::json(&response)) // 直接返回 response
    }
}

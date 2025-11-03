use crate::global;
use crate::server::response::ErrorResponse;
use chrono::Utc;
use serde::{Deserialize, Serialize};
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
pub struct SymbolInfo {
    name: String,
    ticker: String,
    session: String,
    timezone: String,
    minmov: i32,
    pricescale: i32,
    has_intraday: bool,
    supported_resolutions: Vec<String>,
    has_no_volume: bool,
}

pub async fn get_symbol_info(symbol: String) -> Result<impl Reply, Rejection> {
    let info = SymbolInfo {
        name: symbol.clone(),
        ticker: symbol.clone(),
        session: "24x7".into(),
        timezone: "Etc/UTC".into(),
        minmov: 1,
        pricescale: 100, // 表示 2 位小数（1/100）
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
    };
    Ok(reply::json(&info))
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
        Ok(reply::json(&serde_json::json!({
            "s": "ok",
            "bars": bars
        })))
    }
}

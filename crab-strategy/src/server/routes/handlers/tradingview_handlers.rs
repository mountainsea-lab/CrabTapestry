use crate::domain::service::tradingview_service::{
    fetch_history_data, get_strategy_symbol_data, get_tradingview_config, get_tradingview_symbols,
};
use crate::domain::tradingview_dto::{HistoryQuery, SearchQuery, SymbolQuery, bars_to_udf};
use chrono::Utc;
use crab_infras::external::crab_hmds::meta::OhlcvRecord;
use warp::{Rejection, Reply, http::StatusCode, reply};

/// TradingView Datafeed 兼容接口

/// 1️⃣ 获取服务器时间
pub async fn get_time() -> Result<impl Reply, Rejection> {
    let now = Utc::now().timestamp();
    Ok(reply::json(&serde_json::json!({ "time": now })))
}

/// 获取配置信息 /config
pub async fn get_config() -> Result<impl Reply, Rejection> {
    // 调用业务逻辑层
    match get_tradingview_config().await {
        Ok(info) => {
            // 成功返回 JSON 响应
            Ok(reply::json(&info))
        }
        Err(err) => {
            eprintln!("❌ Failed to get symbol info: {:?}", err);
            // 返回 HTTP 错误响应
            let error_msg = reply::json(&serde_json::json!({
                "error": "Failed to fetch symbol info",
                "detail": err.to_string()
            }));
            Ok(reply::with_status(error_msg, StatusCode::BAD_REQUEST))
        }
    }
}

/// 查询币种信息 /symbols/{symbol}
pub async fn get_symbol_info(query: SymbolQuery) -> Result<impl Reply, Rejection> {
    // 调用业务逻辑层
    match get_tradingview_symbols(query).await {
        Ok(info) => {
            // 成功返回 JSON 响应
            Ok(reply::json(&info))
        }
        Err(err) => {
            eprintln!("❌ Failed to get symbol info: {:?}", err);
            // 返回 HTTP 错误响应
            let error_msg = reply::json(&serde_json::json!({
                "error": "Failed to fetch symbol info",
                "detail": err.to_string()
            }));
            Ok(reply::with_status(error_msg, StatusCode::BAD_REQUEST))
        }
    }
}

/// 5️⃣ 模糊搜索币种 /search?query=BTC&type=crypto&exchange=BINANCE
pub async fn search_symbols(query: SearchQuery) -> Result<impl Reply, Rejection> {
    // 获取搜索结果
    let results = match get_strategy_symbol_data(&query.query).await {
        Ok(res) => res,
        Err(err) => {
            eprintln!("⚠️ Failed to get symbol info: {:?}", err);
            Vec::new()
        }
    };

    // 过滤 type
    let results = if let Some(ref t) = query.type_ {
        results
            .into_iter()
            .filter(|s| s.type_.eq_ignore_ascii_case(t))
            .collect::<Vec<_>>()
    } else {
        results
    };

    // 过滤 exchange
    let results = if let Some(ref ex) = query.exchange {
        results
            .into_iter()
            .filter(|s| s.exchange.eq_ignore_ascii_case(ex))
            .collect::<Vec<_>>()
    } else {
        results
    };

    // 根据 limit 限制返回数量
    let limit = query.limit.unwrap_or(30) as usize;
    let limited_results = results.into_iter().take(limit).collect::<Vec<_>>();

    // 返回 JSON 响应
    Ok(reply::json(&limited_results))
}

/// 4️⃣ 查询历史 K 线 /history?symbol=BTC/USDT&resolution=1&from=...&to=...
pub async fn get_history(query: HistoryQuery) -> Result<impl Reply, Rejection> {
    // 调用 fetch_history_data 获取真实 OHLCV 数据
    let bars: Vec<OhlcvRecord> = match fetch_history_data(&query).await {
        Ok(bars) => bars,
        Err(err) => {
            eprintln!("Failed to fetch history data: {:?}", err);
            Vec::new()
        }
    };

    // 根据 countback 截取最后 N 根 K 线
    let bars = if let Some(count) = query.countback {
        let len = bars.len();
        if len > count as usize {
            bars[len - count as usize..].to_vec()
        } else {
            bars
        }
    } else {
        bars
    };

    // 如果没有数据，返回 no_data
    if bars.is_empty() {
        Ok(reply::json(&serde_json::json!({ "s": "no_data" })))
    } else {
        // 转换为 TradingView UDF JSON
        let response = bars_to_udf(&bars);
        Ok(reply::json(&response))
    }
}

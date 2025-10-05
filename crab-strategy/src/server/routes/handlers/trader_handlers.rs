use crate::global;
use crate::server::response::ErrorResponse;
use warp::{Rejection, Reply, reply};

/// 开启交易
pub async fn enable_trading() -> &'static str {
    let trader = global::get_crab_trader();

    if trader.enable_trading().await.is_ok() {
        "crab-strategy enable trading success..."
    } else {
        "crab-strategy enable trading failed!"
    }
}
/// 停止交易
pub async fn disable_trading() -> &'static str {
    let trader = global::get_crab_trader();

    if trader.enable_trading().await.is_ok() {
        "crab-strategy disable trading success..."
    } else {
        "crab-strategy disable trading failed!"
    }
}

/// 获取系统运行状态
pub async fn get_status() -> Result<impl Reply, Rejection> {
    // ✅ 获取全局 CrabTrader
    let trader = global::get_crab_trader();

    // ✅ 异步获取状态
    match trader.get_status().await {
        Ok(status) => {
            // 返回 JSON 响应
            Ok(reply::json(&status))
        }
        Err(err) => {
            // 发生错误时返回 HTTP 500
            Err(warp::reject::custom(ErrorResponse { message: err.to_string() }))
        }
    }
}

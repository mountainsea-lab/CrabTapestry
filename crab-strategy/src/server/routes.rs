use crate::server::AppState;
use crate::server::routes::handlers::log_handlers::{query_logs, sse_logs, with_cache, with_tx};
use crate::server::routes::handlers::trader_handlers::{disable_trading, enable_trading, get_status};
use crate::server::routes::handlers::tradingview_handlers::{get_config, get_history, get_symbol_info, get_time};
use ms_tracing::LogQuery;
use warp::{self, Filter, cors};

pub mod handlers;

pub fn routes(state: AppState) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let api = warp::path("api");

    let ping = api.and(warp::path("ping")).map(handlers::ping);
    let version = api.and(warp::path("version")).map(handlers::version);
    let sysinfo = api.and(warp::path("sysinfo")).map(handlers::sysinfo);
    let health = api.and(warp::path("health")).map(handlers::health);
    let logs_sse = api
        .and(
            warp::path("logs").and(
                warp::path("sse") // 实时 SSE 接口
                    .and(warp::get())
                    .and(with_tx(state.tx.clone())),
            ),
        )
        .and_then(sse_logs);
    let logs = api
        .and(
            warp::path("logs").and(
                warp::get() // 历史查询接口
                    .and(warp::query::<LogQuery>())
                    .and(with_cache(state.cache)),
            ),
        )
        .and_then(query_logs);
    //===============trader handlers=================
    let enable_trading = api
        .and(warp::path("trader").and(warp::path("enable").and(warp::get())))
        .and_then(enable_trading);

    let disable_trading = api
        .and(warp::path("trader").and(warp::path("disable").and(warp::get())))
        .and_then(disable_trading);

    let get_status = api
        .and(warp::path("trader").and(warp::path("status").and(warp::get())))
        .and_then(get_status);
    //===============trader handlers=================

    // ====================== TradingView Datafeed（根路径） ======================
    let tradingview_routes = warp::path("time")
        .and(warp::get())
        .and_then(get_time)
        .or(warp::path("config").and(warp::get()).and_then(get_config))
        .or(warp::path("symbols")
            .and(warp::path::param::<String>())
            .and(warp::get())
            .and_then(get_symbol_info))
        .or(warp::path("history")
            .and(warp::get())
            .and(warp::query::<handlers::tradingview_handlers::HistoryQuery>())
            .and_then(get_history));

    // ====================== 合并所有路由 ======================
    warp::path::end()
        .map(handlers::index)
        .or(ping)
        .or(logs_sse)
        .or(logs)
        .or(enable_trading)
        .or(disable_trading)
        .or(get_status)
        .or(version)
        .or(sysinfo)
        .or(health)
        .or(tradingview_routes)
        .with(cors())
}

#[allow(dead_code)]
fn with_state(state: AppState) -> impl Filter<Extract = (AppState,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || state.clone())
}

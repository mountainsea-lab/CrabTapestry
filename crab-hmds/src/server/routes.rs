use super::AppState;
use crate::domain::model::ohlcv_record::OhlcvFilter;
use crate::server::routes::handlers::log_handlers::{query_logs, sse_logs, with_cache, with_tx};
use crate::server::routes::handlers::ohlcv_record_handlers::{query_list, query_page};
use ms_tracing::LogQuery;
use warp::{self, Filter};

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

    //=====================ohlcv================================
    let ohlcv_records = api
        .and(
            warp::path("ohlcv").and(
                warp::get() // 历史查询接口
                    .and(warp::query::<OhlcvFilter>()),
            ),
        )
        .and_then(query_list);

    let ohlcv_page = api
        .and(
            warp::path("ohlcv").and(
                warp::get() // 历史查询接口
                    .and(warp::query::<OhlcvFilter>()),
            ),
        )
        .and_then(query_page);
    //=====================ohlcv================================

    warp::path::end()
        .map(handlers::index)
        .or(ping)
        .or(logs_sse)
        .or(logs)
        .or(ohlcv_records)
        .or(ohlcv_page)
        .or(version)
        .or(sysinfo)
        .or(health)
}

#[allow(dead_code)]
fn with_state(state: AppState) -> impl Filter<Extract = (AppState,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || state.clone())
}

use crate::domain::model::AppError;
use crate::domain::model::ohlcv_record::OhlcvFilter;
use crate::domain::service::{query_ohlcv_list, query_ohlcv_page};
use crate::server::routes::handlers::handle_error;
use warp::{Rejection, Reply};

// http://localhost:10088/api/ohlcv/page?exchange=BinanceFuturesUsd&symbol=ETHUSDT&period=1m
pub async fn query_page(ohlcv_filter: OhlcvFilter) -> Result<impl Reply, Rejection> {
    let result = query_ohlcv_page(ohlcv_filter)
        .await
        .map_err(|e| handle_error(AppError::from(e)))?;
    Ok(warp::reply::json(&result))
}
// http://localhost:10088/api/ohlcv/list?exchange=BinanceFuturesUsd&symbol=ETHUSDT&period=1m
pub async fn query_list(ohlcv_filter: OhlcvFilter) -> Result<impl Reply, Rejection> {
    let result = query_ohlcv_list(ohlcv_filter)
        .await
        .map_err(|e| handle_error(AppError::from(e)))?;
    Ok(warp::reply::json(&result))
}

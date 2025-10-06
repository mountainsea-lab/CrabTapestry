use crate::domain::model::AppError;
use crate::server::response::ErrorResponse;
use warp::Rejection;

pub mod log_handlers;
pub mod ohlcv_record_handlers;

pub fn index() -> &'static str {
    "Welcome to crab_hmds-hmds!"
}

pub fn ping() -> &'static str {
    "ping pong!"
}

pub fn version() -> &'static str {
    "crab_hmds-hmds version 0.1.0"
}

pub fn sysinfo() -> &'static str {
    "sysinfo info: hello , I am a crab_hmds tapestry hmds server"
}

pub fn health() -> &'static str {
    "if you ask: hao are you,oh I am ok"
}

fn handle_error(e: AppError) -> Rejection {
    let error_response = match e {
        AppError::NotFound => ErrorResponse { message: "Not found".to_string() },
        AppError::InvalidInput(ref msg) => ErrorResponse { message: msg.clone() },
        AppError::DatabaseError(_) => ErrorResponse { message: "Database error".to_string() },
        AppError::Internal(ref msg) => ErrorResponse { message: msg.clone() },
    };

    // 返回自定义的 Rejection 错误
    warp::reject::custom(error_response)
}

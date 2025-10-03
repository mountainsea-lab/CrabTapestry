use serde::Serialize;

pub type Response = Result<warp::reply::Json, ErrorResponse>;

// 错误响应结构体
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

impl warp::reject::Reject for ErrorResponse {}

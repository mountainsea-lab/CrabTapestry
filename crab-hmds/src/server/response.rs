use warp::Reply;

pub type CusResponse = Result<warp::reply::Json, warp::Rejection>;
pub type _Response = Result<warp::reply::Json, ErrorResponse>;

#[derive(Debug)]
pub struct ErrorResponse(pub warp::reply::Response);

impl Reply for ErrorResponse {
    fn into_response(self) -> warp::reply::Response {
        self.0
    }
}

impl warp::reject::Reject for ErrorResponse {}

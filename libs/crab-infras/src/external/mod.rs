use barter_integration::error::SocketError;
use barter_integration::protocol::http::HttpParser;
use reqwest::StatusCode;
use thiserror::Error;

pub mod binance;
pub mod crab_hmds;
pub mod news;

/// Parser for third domain responses
#[derive(Debug)]
pub struct CommonExternalParser;

impl HttpParser for CommonExternalParser {
    type ApiError = serde_json::Value;
    type OutputError = ExecutionError;

    fn parse_api_error(&self, status: StatusCode, api_error: Self::ApiError) -> Self::OutputError {
        let error = api_error.to_string();
        ExecutionError::Socket(SocketError::HttpResponse(status, error))
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("SocketError: {0}")]
    Socket(#[from] SocketError),
}

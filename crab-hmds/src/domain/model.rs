use diesel::result::Error as DieselError;
use thiserror::Error;

pub mod market_backfill_meta;
pub mod market_missing_range;
pub mod ohlcv_record;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DieselError),

    #[error("Not found")]
    NotFound,

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// 通用分页响应
#[derive(Debug, Clone, serde::Serialize)]
pub struct PageResult<T> {
    pub data: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

#[derive(Debug, Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct PageQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

impl PageQuery {
    pub fn offset(&self) -> i64 {
        match (self.page, self.page_size) {
            (Some(p), Some(s)) => ((p.saturating_sub(1)) * s) as i64,
            _ => 0,
        }
    }

    pub fn limit(&self) -> i64 {
        self.page_size.unwrap_or(20) as i64
    }
}

pub trait PrimaryKeyExtractor<PK> {
    fn primary_key(&self) -> PK;
}

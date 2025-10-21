// ============================
// Repository Error
// ============================
#[derive(thiserror::Error, Debug)]
pub enum RepositoryError {
    #[error("Entity not found")]
    NotFound,
    #[error("Duplicate entity")]
    Duplicate,
    #[error("Database error: {0}")]
    Database(String),
    #[error("Other error: {0}")]
    Other(String),
}

impl From<anyhow::Error> for RepositoryError {
    fn from(e: anyhow::Error) -> Self {
        RepositoryError::Other(e.to_string())
    }
}

// ============================
// 分页结构
// ============================
#[derive(Debug, Clone)]
pub struct Page<T> {
    pub page: usize,
    pub page_size: usize,
    pub total: usize,
    pub records: Vec<T>,
}

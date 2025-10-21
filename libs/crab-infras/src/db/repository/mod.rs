use crate::db::repository::types::{Page, RepositoryError};
use async_trait::async_trait;

pub mod base_service;
pub mod types;

// ============================
// 拆分的 CRUD trait
// ============================
#[async_trait]
pub trait CreateRepository<T> {
    async fn create(&self, entity: T) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait ReadRepository<T, ID> {
    async fn get_by_id(&self, id: ID) -> Result<T, RepositoryError>;
    async fn get_all(&self) -> Result<Vec<T>, RepositoryError>;
    async fn paginate(&self, page: usize, page_size: usize) -> Result<Page<T>, RepositoryError>;
}

#[async_trait]
pub trait UpdateRepository<T> {
    async fn update(&self, entity: T) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait DeleteRepository<ID> {
    async fn delete(&self, id: ID) -> Result<(), RepositoryError>;
}

// 聚合为 Repository
#[async_trait]
pub trait Repository<T, ID>:
    CreateRepository<T> + ReadRepository<T, ID> + UpdateRepository<T> + DeleteRepository<ID> + Send + Sync
{
}
impl<T, ID, R> Repository<T, ID> for R where
    R: CreateRepository<T> + ReadRepository<T, ID> + UpdateRepository<T> + DeleteRepository<ID> + Send + Sync
{
}

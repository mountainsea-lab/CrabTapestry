use crate::db::repository::Repository;
use crate::db::repository::types::{Page, RepositoryError};
use anyhow::Result;
use std::sync::Arc;

// ============================
// BaseService (带 hooks + DTO)
// ============================
#[derive(Clone)]
pub struct BaseService<T, ID, DTO, R>
where
    R: Repository<T, ID>,
    DTO: From<T> + Clone + Send + Sync + 'static,
{
    repo: Arc<R>,
    _phantom: std::marker::PhantomData<(T, ID, DTO)>,
}

impl<T, ID, DTO, R> BaseService<T, ID, DTO, R>
where
    T: Clone + Send + Sync + 'static,
    ID: Clone + Send + Sync + 'static,
    R: Repository<T, ID> + 'static,
    DTO: From<T> + Clone + Send + Sync + 'static,
{
    pub fn new(repo: Arc<R>) -> Self {
        Self { repo, _phantom: Default::default() }
    }

    // --- Hooks ---
    async fn before_create(&self, _entity: &T) -> Result<()> {
        Ok(())
    }
    async fn after_create(&self, _entity: &T) -> Result<()> {
        Ok(())
    }

    // --- CRUD ---
    pub async fn create(&self, entity: T) -> Result<(), RepositoryError> {
        self.before_create(&entity).await?;
        self.repo.create(entity.clone()).await?;
        self.after_create(&entity).await?;
        Ok(())
    }

    pub async fn get_by_id(&self, id: ID) -> Result<DTO, RepositoryError> {
        let entity = self.repo.get_by_id(id).await?;
        Ok(DTO::from(entity))
    }

    pub async fn paginate(&self, page: usize, page_size: usize) -> Result<Page<DTO>, RepositoryError> {
        let result = self.repo.paginate(page, page_size).await?;
        let records = result.records.into_iter().map(DTO::from).collect();
        Ok(Page {
            page: result.page,
            page_size: result.page_size,
            total: result.total,
            records,
        })
    }
}

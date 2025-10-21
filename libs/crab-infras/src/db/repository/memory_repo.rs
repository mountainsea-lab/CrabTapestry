use crate::db::repository::types::{Page, RepositoryError};
use crate::db::repository::{CreateRepository, DeleteRepository, ReadRepository, UpdateRepository};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// -----------------------------
// 通用内存 Repository 模板实现
// -----------------------------
#[derive(Clone)]
pub struct InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    store: Arc<RwLock<HashMap<ID, T>>>,
    get_id_fn: Arc<dyn Fn(&T) -> ID + Send + Sync>,
}

impl<T, ID> InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new<F>(get_id_fn: F) -> Self
    where
        F: Fn(&T) -> ID + Send + Sync + 'static,
    {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            get_id_fn: Arc::new(get_id_fn),
        }
    }
}

#[async_trait]
impl<T, ID> CreateRepository<T> for InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    async fn create(&self, entity: T) -> Result<(), RepositoryError> {
        let id = (self.get_id_fn)(&entity);
        let mut db = self.store.write().await;
        if db.contains_key(&id) {
            return Err(RepositoryError::Duplicate);
        }
        db.insert(id, entity);
        Ok(())
    }
}

#[async_trait]
impl<T, ID> ReadRepository<T, ID> for InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    async fn get_by_id(&self, id: ID) -> Result<T, RepositoryError> {
        let db = self.store.read().await;
        db.get(&id).cloned().ok_or(RepositoryError::NotFound)
    }

    async fn get_all(&self) -> Result<Vec<T>, RepositoryError> {
        let db = self.store.read().await;
        Ok(db.values().cloned().collect())
    }

    async fn paginate(&self, page: usize, page_size: usize) -> Result<Page<T>, RepositoryError> {
        let db = self.store.read().await;
        let total = db.len();
        let start = (page - 1) * page_size;
        let records = db.values().cloned().skip(start).take(page_size).collect::<Vec<_>>();
        Ok(Page { page, page_size, total, records })
    }
}

#[async_trait]
impl<T, ID> UpdateRepository<T> for InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    async fn update(&self, entity: T) -> Result<(), RepositoryError> {
        let id = (self.get_id_fn)(&entity);
        let mut db = self.store.write().await;
        if !db.contains_key(&id) {
            return Err(RepositoryError::NotFound);
        }
        db.insert(id, entity);
        Ok(())
    }
}

#[async_trait]
impl<T, ID> DeleteRepository<ID> for InMemoryRepository<T, ID>
where
    T: Clone + Send + Sync + 'static,
    ID: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    async fn delete(&self, id: ID) -> Result<(), RepositoryError> {
        let mut db = self.store.write().await;
        db.remove(&id).ok_or(RepositoryError::NotFound)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct User {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_in_memory_repository() -> Result<()> {
        let repo = InMemoryRepository::new(|u: &User| u.id);

        // Create
        let user = User { id: 1, name: "Alice".into() };
        repo.create(user.clone()).await?;
        assert_eq!(repo.get_by_id(1).await?, user);

        // Update
        let mut updated = user.clone();
        updated.name = "Alice Updated".into();
        repo.update(updated.clone()).await?;
        assert_eq!(repo.get_by_id(1).await?, updated);

        // Paginate
        let page = repo.paginate(1, 10).await?;
        assert_eq!(page.total, 1);
        assert_eq!(page.records.len(), 1);

        // Delete
        repo.delete(1).await?;
        assert!(matches!(repo.get_by_id(1).await, Err(RepositoryError::NotFound)));

        Ok(())
    }
}

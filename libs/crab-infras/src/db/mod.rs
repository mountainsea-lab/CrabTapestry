use anyhow::Result;
use once_cell::sync::OnceCell;
use std::sync::Arc;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[derive(Clone)]
pub struct Database {
    inner: Arc<dyn DatabaseBackend + Send + Sync>,
}

#[async_trait::async_trait]
pub trait DatabaseBackend: Send + Sync {
    async fn run_migrations(&self) -> Result<()>;
}

static GLOBAL_DB: OnceCell<Database> = OnceCell::new();

impl Database {
    /// 初始化全局数据库实例
    pub async fn initialize(database_url: &str) -> Result<()> {
        dotenvy::dotenv().ok();

        // 根据 feature 调用不同函数创建 backend
        let backend = build_backend(database_url)?;

        GLOBAL_DB
            .set(Database { inner: backend })
            .map_err(|_| anyhow::anyhow!("Database already initialized"))?;

        Ok(())
    }

    pub fn global() -> &'static Database {
        GLOBAL_DB.get().expect("Database not initialized")
    }

    pub async fn run_migrations(&self) -> Result<()> {
        self.inner.run_migrations().await
    }
}

// ----------------- Feature backend 构建 -----------------

#[cfg(feature = "postgres")]
async fn build_backend(database_url: &str) -> Result<Arc<dyn DatabaseBackend + Send + Sync>> {
    Ok(Arc::new(postgres::PostgresDatabase::new(database_url).await?))
}

#[cfg(feature = "mysql")]
fn build_backend(database_url: &str) -> Result<Arc<dyn DatabaseBackend + Send + Sync>> {
    todo!("Implement MySQL backend")
    // Ok(Arc::new(mysql::MySqlDatabase::new(database_url)?))
}

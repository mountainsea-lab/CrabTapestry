use anyhow::Result;
use dotenvy::dotenv;
use once_cell::sync::OnceCell;
use std::env;
use std::sync::Arc;

#[cfg(not(any(feature = "postgres", feature = "mysql")))]
compile_error!("At least one database backend feature must be enabled.");

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;
pub mod repository;

/// 通用数据库抽象层
#[derive(Clone)]
pub struct Database {
    inner: Arc<dyn DatabaseBackend + Send + Sync>,
}

/// 数据库后台 trait，统一接口
#[async_trait::async_trait]
pub trait DatabaseBackend: Send + Sync {
    /// 执行全量迁移
    /// - `migration_dir`: 可选迁移目录，如果 None，则使用默认迁移路径
    async fn run_migrations(&self, migration_dir: Option<&std::path::Path>) -> Result<()>;

    /// 执行单条 SQL（可选）
    async fn execute_sql(&self, sql: &str) -> Result<()>;
}

/// 全局单例
static GLOBAL_DB: OnceCell<Database> = OnceCell::new();

impl Database {
    /// 初始化全局数据库实例
    pub async fn initialize() -> Result<()> {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        // 创建 backend
        let backend = build_backend(&database_url).await?;

        GLOBAL_DB
            .set(Database { inner: backend })
            .map_err(|_| anyhow::anyhow!("Database already initialized"))?;

        Ok(())
    }

    /// 获取全局数据库实例
    pub fn global() -> &'static Database {
        GLOBAL_DB.get().expect("Database not initialized")
    }

    /// 执行迁移
    pub async fn run_migrations(&self, migration_dir: Option<&std::path::Path>) -> Result<()> {
        self.inner.run_migrations(migration_dir).await
    }

    /// 执行单条 SQL
    pub async fn execute_sql(&self, sql: &str) -> Result<()> {
        self.inner.execute_sql(sql).await
    }
}

// ----------------- Feature backend 构建 -----------------

// async fn build_backend(database_url: &str) -> Result<Arc<dyn DatabaseBackend + Send + Sync>> {
//     #[cfg(feature = "postgres")]
//     {
//         let backend = postgres::PostgresDatabase::new(database_url).await?;
//         Ok(Arc::new(backend));
//     }
//
//     // #[cfg(feature = "mysql")]
//     // {
//     //     // todo!("impl feature")
//     //     let backend = mysql::MySqlDatabase::new(database_url).await?;
//     //     let backend =    make_mysql_pool().await?;
//     //     return Ok(Arc::new(backend));
//     // }
//     //
//     // #[allow(unreachable_code)]
//     // Err(anyhow::anyhow!("No database feature enabled"))
//     // fallback 分支 — 没启用任何数据库 feature
//     Err(anyhow::anyhow!("No database backend feature enabled"))
// }

async fn build_backend(database_url: &str) -> Result<Arc<dyn DatabaseBackend + Send + Sync>> {
    #[cfg(feature = "postgres")]
    {
        let backend = postgres::PostgresDatabase::new(database_url).await?;
        return Ok(Arc::new(backend)); // ✅ 注意这里要加 return
    }

    // #[cfg(feature = "mysql")]
    // {
    //     let backend = mysql::MySqlDatabase::new(database_url).await?;
    //     return Ok(Arc::new(backend)); // ✅ 同样 return
    // }

    // fallback，当没有任何 feature 启用时：
    Err(anyhow::anyhow!("No database backend feature enabled"))
}

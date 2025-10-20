use crate::db::DatabaseBackend;
use anyhow::Result;
use async_trait::async_trait;
use ms_tracing::tracing_utils::internal::info;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::path::{Path, PathBuf};
use std::{fs, str::FromStr, time::Duration};

#[derive(Clone)]
pub struct PostgresDatabase {
    pub pool: PgPool,
}

impl PostgresDatabase {
    pub async fn new(database_url: &str) -> Result<Self> {
        let options = PgConnectOptions::from_str(database_url)?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(3600))
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl DatabaseBackend for PostgresDatabase {
    async fn run_migrations(&self, migration_dir: Option<&Path>) -> Result<()> {
        // 使用默认路径，如果没有传入
        let dir = migration_dir.unwrap_or_else(|| Path::new("./migrations/postgres"));

        let mut entries: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|ext| ext == "sql").unwrap_or(false))
            .collect();

        // 按文件名排序执行，保证迁移顺序
        entries.sort();

        for entry in entries {
            let sql = fs::read_to_string(&entry)?;
            info!("Running migration: {:?}", entry.file_name().unwrap());
            sqlx::query(&sql).execute(&self.pool).await?;
        }

        Ok(())
    }
    async fn execute_sql(&self, sql: &str) -> Result<()> {
        sqlx::query(sql).execute(&self.pool).await?;
        Ok(())
    }
}

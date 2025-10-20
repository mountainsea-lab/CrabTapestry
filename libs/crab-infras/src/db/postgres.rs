use crate::db::DatabaseBackend;
use anyhow::Result;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, time::Duration};

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

#[async_trait::async_trait]
impl DatabaseBackend for PostgresDatabase {
    async fn run_migrations(&self) -> Result<()> {
        tracing::info!("Running Postgres migrations...");
        // 可直接用 sqlx::migrate! 宏
        // sqlx::migrate!("./migrations/postgres").run(&self.pool).await?;
        Ok(())
    }
}

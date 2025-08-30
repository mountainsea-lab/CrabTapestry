use crate::utils::create_redis_pool;
use anyhow::{Context, Result};
use bb8_redis::{RedisConnectionManager, bb8, redis::cmd};
use ms_tracing::tracing_utils::internal::{debug, info, warn};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone)]
pub struct RedisCache {
    pool: bb8::Pool<RedisConnectionManager>,
}

impl RedisCache {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let pool = create_redis_pool(redis_url).await?;
        info!("Connected to Redis KV store at {}", redis_url);
        Ok(Self { pool })
    }

    pub async fn get<T: DeserializeOwned + Send>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.pool.get().await.context("Failed to get Redis connection")?;

        let value: Option<String> = cmd("GET")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .with_context(|| format!("Failed to execute GET for key: {}", key))?;

        match value {
            Some(json_str) => serde_json::from_str(&json_str)
                .with_context(|| format!("Failed to deserialize value for key: {}", key))
                .map(Some),
            None => Ok(None),
        }
    }

    pub async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.pool.get().await.context(format!(
            "Failed to get Redis connection: {:#?}",
            self.pool.state().statistics
        ))?;
        let json_str = serde_json::to_string(value)?;
        let _: () = cmd("SET")
            .arg(key)
            .arg(json_str)
            .query_async(&mut *conn)
            .await
            .with_context(|| format!("Failed to set key: {}", key))?;
        debug!(key, "redis set ok");
        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.pool.get().await.context(format!(
            "Failed to get Redis connection: {:#?}",
            self.pool.state().statistics
        ))?;
        let exists: bool = cmd("EXISTS")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .with_context(|| format!("Failed to query exists for key: {}", key))?;
        debug!(key, exists, "redis exists ok");
        Ok(exists)
    }

    fn make_price_key(&self, mint: &str) -> String {
        format!("solana:price:{}", mint)
    }

    fn make_metadata_key(&self, mint: &str) -> String {
        format!("solana:metadata:{}", mint)
    }

    pub async fn push_kline<T: Serialize>(&self, key: &str, kline: &T) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let json = serde_json::to_string(kline)?;
        let _: () = cmd("RPUSH").arg(&key).arg(json).query_async(&mut *conn).await?;
        Ok(())
    }

    pub async fn pop_all_klines<T: DeserializeOwned>(&self, key: &str) -> Result<Vec<T>> {
        let mut conn = self.pool.get().await?;
        let len: usize = cmd("LLEN").arg(&key).query_async(&mut *conn).await?;

        if len == 0 {
            return Ok(vec![]);
        }

        let raw: Vec<String> = cmd("LRANGE").arg(&key).arg(0).arg(len - 1).query_async(&mut *conn).await?;

        let _: () = cmd("DEL").arg(&key).query_async(&mut *conn).await?;

        raw.into_iter()
            .map(|s| serde_json::from_str(&s).context("Failed to deserialize kline"))
            .collect()
    }

    pub async fn len(&self, key: &str) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let len: usize = cmd("LLEN").arg(&key).query_async(&mut *conn).await?;
        Ok(len)
    }

    /// 通用扫描 Redis 中匹配的所有 key（支持 glob-style 通配符）
    async fn scan_keys_by_pattern(&self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        let mut cursor = 0;
        let mut all_keys = vec![];

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await?;

            all_keys.extend(keys);

            if next_cursor == 0 {
                break;
            }

            cursor = next_cursor;
        }

        Ok(all_keys)
    }

    /// 通用弹出并删除所有匹配 pattern 的 Redis 列表，反序列化为 Vec<T>
    pub async fn pop_all_by_pattern<T: DeserializeOwned>(&self, pattern: &str) -> Result<Vec<T>> {
        let mut conn = self.pool.get().await?;
        let keys = self.scan_keys_by_pattern(pattern).await?;
        let mut result = Vec::new();

        for key in keys {
            let len: usize = cmd("LLEN").arg(&key).query_async(&mut *conn).await.unwrap_or(0);
            if len == 0 {
                continue;
            }

            let raw: Vec<String> = cmd("LRANGE")
                .arg(&key)
                .arg(0)
                .arg(len - 1)
                .query_async(&mut *conn)
                .await
                .unwrap_or_default();

            let _: i64 = cmd("DEL")
                .arg(&key)
                .query_async(&mut *conn)
                .await
                .with_context(|| format!("Failed to delete Redis key: {}", key))?;

            for s in raw {
                match serde_json::from_str::<T>(&s) {
                    Ok(value) => result.push(value),
                    Err(e) => {
                        warn!("Failed to deserialize entry from key {}: {}", key, e);
                    }
                }
            }
        }

        Ok(result)
    }

    /// 统计所有匹配 Redis key pattern 的列表总长度
    pub async fn len_by_pattern(&self, pattern: &str) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let keys = self.scan_keys_by_pattern(pattern).await?;
        let mut total = 0;

        for key in keys {
            let len: usize = cmd("LLEN").arg(&key).query_async(&mut *conn).await.unwrap_or(0);
            total += len;
        }

        Ok(total)
    }
}

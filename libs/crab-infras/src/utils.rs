use crate::cache::redis_cache::RedisCache;
use anyhow::Result;
use bb8_redis::{RedisConnectionManager, bb8};
use crab_common_utils::env::{is_local, must_get_env};
use std::sync::Arc;

pub async fn create_redis_pool(redis_url: &str) -> Result<bb8::Pool<RedisConnectionManager>> {
    let manager = RedisConnectionManager::new(redis_url)?;
    let pool = bb8::Pool::builder()
        .max_size(200)
        .min_idle(Some(20))
        .max_lifetime(Some(std::time::Duration::from_secs(60 * 15))) // 15 minutes
        .idle_timeout(Some(std::time::Duration::from_secs(60 * 5))) // 5 minutes
        .build(manager)
        .await?;
    Ok(pool)
}

pub async fn init_redis_cache() -> Result<Arc<RedisCache>> {
    match is_local() {
        true => {
            // let kv_store = RedisCache::new("redis://localhost:6379").await?;
            let kv_store = RedisCache::new(must_get_env("REDIS_URL").as_str()).await?;
            Ok(Arc::new(kv_store))
        }
        false => {
            let kv_store = RedisCache::new(must_get_env("REDIS_URL").as_str()).await?;
            Ok(Arc::new(kv_store))
        }
    }
}

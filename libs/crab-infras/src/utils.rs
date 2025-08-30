use bb8_redis::{RedisConnectionManager, bb8};
use anyhow::Result;

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

// Use Arc to avoid cloning actual instances and allow shared ownership

use anyhow::Result;
use crab_infras::cache::redis_cache::RedisCache;
use crab_infras::utils::init_redis_cache;
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub static REDIS_CACHE: OnceCell<Arc<RedisCache>> = OnceCell::new();

/// 初始化全局服务
pub async fn init_global_services() -> Result<()> {
    let redis_store = init_redis_cache().await?;
    let _ = set_redis_store(redis_store).await?;
    Ok(())
}

/// 异步设置 Redis 缓存实例
pub async fn set_redis_store(instance: Arc<RedisCache>) -> Result<()> {
    REDIS_CACHE
        .set(instance)
        .map_err(|err| anyhow::anyhow!("Failed to set Redis cache: {:?}", err))?;
    Ok(())
}

/// 获取共享的 Redis 缓存实例
pub fn get_redis_store() -> Result<Arc<RedisCache>> {
    REDIS_CACHE
        .get()
        .cloned() // 将引用转换为所有权
        .ok_or_else(|| {
            anyhow::anyhow!("RedisCache not initialized. Ensure init_global_services has been called before.").into()
        })
}

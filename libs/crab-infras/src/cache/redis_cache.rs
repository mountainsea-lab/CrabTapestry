use crate::utils::create_redis_pool;
use anyhow::{Context, Result};
use bb8_redis::redis::aio::PubSub;
use bb8_redis::{
    RedisConnectionManager, bb8,
    redis::{AsyncCommands, Client, Connection, cmd},
};
use futures::future;
use ms_tracing::tracing_utils::internal::{debug, info, warn};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RedisCache {
    pool: bb8::Pool<RedisConnectionManager>,
    redis_url: String,
}

impl RedisCache {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let pool = create_redis_pool(redis_url).await?;
        info!("Connected to Redis KV store at {}", redis_url);
        Ok(Self { pool, redis_url: redis_url.to_owned() })
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

    // 发布消息到指定频道
    pub async fn publish_message(&self, channel: &str, message: &str) -> Result<()> {
        let mut conn = self.pool.get().await.context("Failed to get Redis connection")?;
        let _: () = conn.publish(channel, message).await.context("Failed to publish message")?;
        debug!(channel, "Message published to Redis channel.");
        Ok(())
    }

    /// 创建独占连接, 绕过 bb8 管理
    #[allow(dead_code)]
    pub(crate) async fn get_subscription_connection(&self) -> Result<Connection> {
        let client = Client::open(self.redis_url.as_str()).context("Failed to create Redis client")?;

        let connection = client.get_connection().context("Failed to create subscription connection")?;

        Ok(connection)
    }

    /// 创建独占PubSub, 绕过 bb8 管理
    async fn init_pubsub(&self) -> Result<Arc<RwLock<PubSub>>> {
        let client = Client::open(self.redis_url.as_str()).context("Failed to create Redis client")?;
        let pubsub = client.get_async_pubsub().await.context("Failed to create PubSub")?;
        Ok(Arc::new(RwLock::new(pubsub)))
    }

    // 订阅单个频道并返回消息通道
    pub async fn subscribe(&self, channel: Arc<String>) -> Result<Arc<RwLock<PubSub>>> {
        let pubsub = self.init_pubsub().await?;

        // 获取写锁
        pubsub
            .write()
            .await
            .subscribe(&channel)
            .await
            .context("Failed to subscribe to channel")?;

        // 直接返回 pubsub 让业务层处理
        info!("Subscribed to channel: {}", channel);

        // 返回 pubsub 对象
        Ok(pubsub)
    }

    // 批量订阅多个频道并返回 pubsub 供业务层处理
    pub async fn subscribe_multiple1(&self, channels: Vec<Arc<String>>) -> Result<Arc<RwLock<PubSub>>> {
        let pubsub = self.init_pubsub().await?;

        // 克隆 pubsub 以便在多个异步任务中共享
        let pubsub_write = pubsub.clone();
        let subscribe_futures: Vec<_> = channels
            .clone()
            .into_iter()
            .map(|channel| {
                let pubsub_write = pubsub_write.clone(); // 只需要克隆一次 pubsub

                // 使用 `async move` 将 `channel` 和 `pubsub_write` 移入闭包
                async move {
                    let mut pubsub_write = pubsub_write.write().await; // 锁住并订阅
                    pubsub_write
                        .subscribe(channel.as_str())
                        .await
                        .context(format!("Failed to subscribe to channel: {}", channel))
                }
            })
            .collect();

        // 等待所有订阅操作并返回结果
        let results = future::join_all(subscribe_futures).await;

        // 处理每个订阅的结果
        for result in results {
            result?; // 解包每个 Result
        }

        info!("Successfully subscribed to channels: {:?}", channels);
        Ok(pubsub)
    }

    // 取消订阅单个频道
    pub async fn unsubscribe(&self, channel: String) -> Result<()> {
        let pubsub = self.init_pubsub().await?;

        pubsub
            .write()
            .await
            .unsubscribe(&channel)
            .await
            .context("Failed to unsubscribe from channel")?;

        info!("Unsubscribed from channel: {}", channel);
        Ok(())
    }

    // 取消订阅多个频道
    pub async fn unsubscribe_multiple(&self, channels: Vec<String>) -> Result<()> {
        let pubsub = self.init_pubsub().await?;

        // 使用并发取消订阅
        let unsubscribe_futures: Vec<_> = channels
            .clone()
            .into_iter()
            .map(|channel| {
                let pubsub = pubsub.clone(); // 克隆 pubsub 用于每个异步任务

                // 使用 `async move` 将 `channel` 和 `pubsub` 移入闭包
                async move {
                    let mut pubsub = pubsub.write().await; // 获取写锁
                    pubsub
                        .unsubscribe(&channel)
                        .await
                        .context(format!("Failed to unsubscribe from channel: {}", channel))
                }
            })
            .collect();

        // 等待所有取消订阅操作并返回结果
        let results = future::join_all(unsubscribe_futures).await;

        // 处理每个取消订阅的结果
        for result in results {
            result?; // 解包每个 Result
        }

        info!("Successfully unsubscribed from channels: {:?}", channels);
        Ok(())
    }

    // 使用模式订阅多个频道
    // pub async fn psubscribe(&self, pattern: Vec<String>) -> Result<mpsc::Receiver<String>> {
    //     let mut conn = self.get_subscription_connection().await?;
    //     let mut pubsub = conn.as_pubsub();
    //
    //     pubsub.psubscribe(pattern)
    //         .await
    //         .context("Failed to pattern subscribe")?;
    //
    //     // 创建一个 mpsc 通道
    //     let (tx, rx) = mpsc::channel(32);
    //
    //     // 异步任务负责将接收到的消息发送到通道
    //     tokio::spawn(async move {
    //         loop {
    //             match pubsub.get_message().await {
    //                 Ok(message) => {
    //                     if let Some(msg) = message.get_payload::<String>() {
    //                         // 发送消息到通道
    //                         if tx.send(msg).await.is_err() {
    //                             break; // 如果接收者关闭通道，退出循环
    //                         }
    //                     }
    //                 }
    //                 Err(e) => {
    //                     eprintln!("Error while receiving message: {}", e);
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //
    //     info!("Subscribed to pattern: {}", pattern);
    //
    //     // 返回接收器
    //     Ok(rx)
    // }
    //

    // // 取消模式订阅
    // pub async fn punsubscribe(&self, pattern: &str) -> Result<()> {
    //     let mut conn = self.get_subscription_connection().await?;
    //     let mut pubsub = conn.as_pubsub();
    //
    //     pubsub.punsubscribe(pattern)
    //         .context("Failed to pattern unsubscribe")?;
    //
    //     info!("Pattern unsubscribed from: {}", pattern);
    //     Ok(())
    // }
    //
    // // 批量取消模式订阅
    // pub async fn punsubscribe_patterns(&self, patterns: Vec<String>) -> Result<()> {
    //     let mut conn = self.get_subscription_connection().await?;
    //     let mut pubsub = conn.as_pubsub();
    //
    //     for pattern in patterns {
    //         pubsub.punsubscribe(pattern)
    //             .await
    //             .context("Failed to pattern unsubscribe")?;
    //     }
    //
    //     info!("Patterns unsubscribed from: {:?}", patterns);
    //     Ok(())
    // }
}

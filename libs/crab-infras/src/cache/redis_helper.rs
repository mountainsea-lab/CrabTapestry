use crate::cache::redis_cache::RedisCache;
use crate::cache::{RedisMessage, parse_redis_message};
use anyhow::Result;
use bb8_redis::redis::aio::PubSub;
use futures::StreamExt;
use ms_tracing::tracing_utils::internal::{debug, error};
use std::option::Option;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{RwLock, broadcast};
use tokio::time::sleep;

// 业务层的订阅/发布辅助操作
pub struct RedisPubSubHelper {
    redis_cache: Arc<RedisCache>,
    sender: Option<broadcast::Sender<RedisMessage>>,
}

impl RedisPubSubHelper {
    pub async fn publish_message(&self, channel: Arc<String>, message: &str) -> Result<()> {
        self.redis_cache.publish_message(channel, message).await
    }
    pub async fn subscribe_channel(&self, channel: Arc<String>) -> Result<Arc<RwLock<PubSub>>> {
        self.redis_cache.subscribe(channel).await
    }
    pub async fn subscribe_multiple(&self, channels: Vec<Arc<String>>) -> Result<Arc<RwLock<PubSub>>> {
        self.redis_cache.subscribe_multiple(channels).await
    }
    pub async fn unsubscribe_channel(&self, channel: Arc<String>) -> Result<()> {
        self.redis_cache.unsubscribe(channel).await
    }

    pub async fn unsubscribe_multiple(&self, pattern: Vec<Arc<String>>) -> Result<()> {
        self.redis_cache.unsubscribe_multiple(pattern).await
    }

    pub fn new_suber(redis_cache: Arc<RedisCache>) -> Self {
        let (sender, _) = broadcast::channel(100); // 设置广播通道
        Self { redis_cache, sender: Some(sender) }
    }
    pub fn new_puber(redis_cache: Arc<RedisCache>) -> Self {
        Self { redis_cache, sender: None } // 只初始化 redis_cache
    }

    // 创建消息接收者
    pub fn create_message_receiver(&self) -> Option<Receiver<RedisMessage>> {
        self.sender.as_ref().map(|sender| sender.subscribe())
    }

    // 用于监听消息并广播给所有接收者
    pub async fn message_listener(&self, channels: Vec<String>) -> Result<()> {
        let pubsub = self
            .redis_cache
            .subscribe_multiple(channels.into_iter().map(Arc::new).collect())
            .await?;

        // 获取写锁
        let mut pubsub_write = pubsub.write().await;

        let mut stream = pubsub_write.on_message(); // 获取流

        loop {
            // 获取下一条消息
            match stream.next().await {
                Some(msg) => {
                    let redis_message = parse_redis_message(&msg)?;

                    // 确保 sender 存在
                    if let Some(sender) = &self.sender {
                        let mut retries = 0;
                        let max_retries = 3;
                        let mut sent = false;

                        // 尝试发送消息，如果失败则重试
                        while retries < max_retries && !sent {
                            match sender.send(redis_message.clone()) {
                                Ok(_) => {
                                    sent = true;
                                    debug!("Successfully broadcast message: {:?}", redis_message);
                                }
                                Err(e) => {
                                    retries += 1;
                                    error!(
                                        "Failed to broadcast message (attempt {}/{}): {} - message: {:?}",
                                        retries, max_retries, e, redis_message
                                    );
                                    // 延时重试
                                    sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }

                        // 如果超过最大重试次数仍然失败，则记录并退出
                        if !sent {
                            error!(
                                "Failed to broadcast message after {} attempts, giving up. Message: {:?}",
                                max_retries, redis_message
                            );
                            break;
                        }
                    } else {
                        // 处理 sender 为 None 的情况
                        error!("Sender is None, unable to broadcast message: {:?}", redis_message);
                        break; // 或者继续，取决于需求
                    }
                }
                None => {
                    // 如果流结束，退出循环
                    break;
                }
            }
        }
        Ok(())
    }

    // 启动监听并返回消息接收器 Arc<Self>
    pub async fn start_message_listener(self: Arc<Self>, channels: Vec<String>) -> Result<Receiver<RedisMessage>> {
        let receiver = match self.create_message_receiver() {
            Some(receiver) => receiver,
            None => return Err(anyhow::anyhow!("Failed to create receiver because sender is None").into()),
        };

        // 启动订阅任务，并在后台异步运行
        let self_clone = Arc::clone(&self);

        tokio::spawn({
            let channels = channels.clone();
            async move {
                if let Err(err) = self_clone.message_listener(channels).await {
                    error!("Error in message listener: {}", err);
                }
            }
        });

        Ok(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BaseBar;
    use std::sync::Arc;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn test_publish_message() {
        ms_tracing::setup_tracing();

        // 创建 RedisCache 实例
        let redis_cache = Arc::new(RedisCache::new("redis://localhost:6379").await.unwrap());
        let pub_helper = RedisPubSubHelper::new_puber(redis_cache);

        // 创建一个频道
        let channel = Arc::new(String::from("test_channel"));

        // 创建 BaseBar 对象
        let base_bar = BaseBar {
            time_period: 60000,
            begin_time: OffsetDateTime::now_utc(),
            end_time: OffsetDateTime::now_utc(),
            open_price: 4000.0,
            high_price: 4050.0,
            low_price: 3950.0,
            close_price: 4005.0,
            volume: 1200.0,
            amount: 4800000.0,
            trades: 100,
        };

        // 将 BaseBar 转为 JSON 字符串
        let bar_payload = serde_json::to_string(&base_bar).unwrap();

        // 创建 RedisMessage 对象
        let redis_message = RedisMessage {
            message_type: "message".to_string(),
            channel: channel.clone().to_string(),
            payload: bar_payload, // 存储序列化后的 BaseBar
        };

        // 序列化 RedisMessage
        let serialized_message = serde_json::to_string(&redis_message).unwrap();

        loop {
            // 发布消息
            let publish_result = pub_helper.publish_message(channel.clone(), &serialized_message).await;

            // 验证发布是否成功
            match publish_result {
                Ok(_) => {
                    // 如果发布成功，则输出调试日志
                    debug!("Message published to Redis channel: {}", channel);
                }
                Err(e) => {
                    panic!("Failed to publish message: {:?}", e);
                }
            }
            // 模拟 10 秒发布一次
            sleep(Duration::from_secs(10)).await;
        }
    }

    #[tokio::test]
    async fn test_subscribe_multiple_channels() {
        ms_tracing::setup_tracing();

        // 创建多个频道
        let channels = vec!["test_channel".to_string()];

        let redis_cache_sub = Arc::new(RedisCache::new("redis://localhost:6379").await.unwrap());
        let sub_helper = Arc::new(RedisPubSubHelper::new_suber(redis_cache_sub));

        // 启动后台任务来监听频道消息
        let mut receiver = match sub_helper.start_message_listener(channels).await {
            Ok(r) => r, // 成功时获取 receiver
            Err(e) => {
                // 处理错误
                error!("Error starting message listener: {:?}", e);
                return; // 或者根据需要退出/继续处理
            }
        };

        tokio::select! {
                // 等待接收到消息
                received_message = receiver.recv() => {
                    let received_message = received_message.unwrap();

                    // 解析接收到的 RedisMessage
                    match received_message {
                        RedisMessage { message_type, channel, payload } => {
                            // 确保消息类型和频道匹配
                            assert_eq!(message_type, "message");
                            assert_eq!(channel, "test_channel");

                            // 反序列化消息中的 payload（BaseBar）
                            let received_bar: BaseBar = serde_json::from_str(&payload).unwrap();

                            // 验证 BaseBar 的字段是否正确
                            assert_eq!(received_bar.time_period, 60000);
                            assert_eq!(received_bar.open_price, 4000.0);
                            assert_eq!(received_bar.high_price, 4050.0);
                            assert_eq!(received_bar.low_price, 3950.0);
                            assert_eq!(received_bar.close_price, 4005.0);
                            assert_eq!(received_bar.volume, 1200.0);
                            assert_eq!(received_bar.amount, 4800000.0);
                            assert_eq!(received_bar.trades, 100);
                        },
                    }
                },
                // 设置超时等待 500 秒
                _ = sleep(Duration::from_secs(500)) => {
                    panic!("Test timed out while waiting for message.");
                }
        }
    }
}

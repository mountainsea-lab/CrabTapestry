use crate::cache::RedisMessage;
use crate::cache::redis_cache::RedisCache;
use anyhow::{Context, Result};
use bb8_redis::redis;
use bb8_redis::redis::{Value, cmd};
use ms_tracing::tracing_utils::internal::{debug, error, info};
use tokio::sync::{broadcast, mpsc};

// 业务层的订阅服务
pub struct RedisPubSubService {
    redis_cache: RedisCache,
    sender: broadcast::Sender<RedisMessage>,
}

impl RedisPubSubService {
    pub async fn subscribe_channel(&self, channel: &str) -> Result<()> {
        self.redis_cache.subscribe(channel).await
    }
    pub async fn subscribe_multiple(&self, channels: Vec<&str>) -> Result<()> {
        self.redis_cache.subscribe_multiple(channels).await
    }
    pub async fn unsubscribe_channel(&self, channel: &str) -> Result<()> {
        self.redis_cache.unsubscribe(channel).await
    }

    pub async fn psubscribe_pattern(&self, pattern: &str) -> Result<()> {
        self.redis_cache.psubscribe(pattern).await
    }

    pub async fn punsubscribe_pattern(&self, pattern: &str) -> Result<()> {
        self.redis_cache.punsubscribe(pattern).await
    }

    pub fn new(redis_cache: RedisCache) -> Self {
        let (sender, _) = broadcast::channel(100); // 设置广播通道
        Self { redis_cache, sender }
    }

    // pub async fn create_message_receiver(&self, channels: Vec<String>) -> anyhow::Result<broadcast::Receiver<RedisMessage>> {
    //     let receiver = self.sender.subscribe();
    //
    //     let self_clone = self.clone();
    //     tokio::spawn(async move {
    //         if let Err(e) = self_clone.message_listener(channels).await {
    //             error!("Message listener error: {}", e);
    //         }
    //     });
    //
    //     Ok(receiver)
    // }
    //
    // async fn message_listener(&self, channels: Vec<String>) -> Result<()> {
    //     let mut conn = self.redis_cache.get_subscription_connection().await?;
    //
    //     for channel in &channels {
    //         cmd("SUBSCRIBE")
    //             .arg(channel)
    //             .exec_async(&mut *conn)
    //             .await
    //             .context(format!("Failed to subscribe to channel: {}", channel))?;
    //     }
    //
    //     info!("Subscribed to channels: {:?}", channels);
    //
    //     loop {
    //         let msg: Value = redis::cmd("READONLY")
    //             .query_async(&mut *conn)
    //             .await
    //             .context("Failed to read message")?;
    //
    //         if let Ok(message) = Self::parse_redis_message(&msg) {
    //             if self.sender.send(message).is_err() {
    //                 info!("No listeners, stopping listener");
    //                 break;
    //             }
    //         }
    //     }
    //
    //     Ok(())
    // }
    //
    // /// 简化的消息解析 - 只关注实际的消息内容
    // fn parse_redis_message(value: &Value) -> Result<RedisMessage, anyhow::Error> {
    //     // 只处理数组类型的消息响应
    //     if let Value::Array(elements) = value {
    //         if elements.len() >= 3 {
    //             // 检查是否是消息类型
    //             let message_type = match elements.first().unwrap() {
    //                 Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                 Value::SimpleString(s) => s.clone(),
    //                 _ => return Err(anyhow::anyhow!("Invalid message type")),
    //             };
    //
    //             // 只处理实际的消息，忽略订阅确认等
    //             if message_type == "message" {
    //                 let channel = match &elements[1] {
    //                     Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                     Value::SimpleString(s) => s.clone(),
    //                     _ => return Err(anyhow::anyhow!("Invalid channel format")),
    //                 };
    //
    //                 let payload = match &elements[2] {
    //                     Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                     Value::SimpleString(s) => s.clone(),
    //                     _ => return Err(anyhow::anyhow!("Invalid payload format")),
    //                 };
    //
    //                 return Ok(RedisMessage { message_type, channel, payload });
    //             }
    //
    //             // 对于模式订阅的消息
    //             if message_type == "pmessage" && elements.len() >= 4 {
    //                 let channel = match &elements[2] {
    //                     Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                     Value::SimpleString(s) => s.clone(),
    //                     _ => return Err(anyhow::anyhow!("Invalid channel format")),
    //                 };
    //
    //                 let payload = match &elements[3] {
    //                     Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                     Value::SimpleString(s) => s.clone(),
    //                     _ => return Err(anyhow::anyhow!("Invalid payload format")),
    //                 };
    //
    //                 return Ok(RedisMessage { message_type, channel, payload });
    //             }
    //         }
    //     }
    //
    //     Err(anyhow::anyhow!("Not a message response"))
    // }
}

// impl RedisPubSubService {
//     pub fn new(redis_cache: RedisCache) -> Self {
//         Self { redis_cache }
//     }
//
//     /// 创建消息接收器 - 返回一个异步通道用于接收消息
//     pub async fn create_message_receiver(&self, channels: Vec<String>) -> anyhow::Result<mpsc::Receiver<RedisMessage>> {
//         let (tx, rx) = mpsc::channel(100);
//         let self_clone = self.clone();
//
//         tokio::spawn(async move {
//             if let Err(e) = self_clone.message_listener(channels, tx).await {
//                 error!("Message listener error: {}", e);
//             }
//         });
//
//         Ok(rx)
//     }
//
//     /// 消息监听器内部实现
//     async fn message_listener(&self, channels: Vec<String>, tx: mpsc::Sender<RedisMessage>) -> anyhow::Result<()> {
//         let mut conn = self.get_subscription_connection().await?;
//
//         // 订阅所有频道
//         for channel in &channels {
//             cmd("SUBSCRIBE")
//                 .arg(channel)
//                 .exec_async(&mut *conn)
//                 .await
//                 .context(format!("Failed to subscribe to channel: {}", channel))?;
//         }
//
//         info!("Subscribed to channels: {:?}", channels);
//
//         // 持续监听消息
//         loop {
//             let msg: Value = redis::cmd("READONLY")
//                 .query_async(&mut *conn)
//                 .await
//                 .context("Failed to read message")?;
//
//             // 使用 if let Ok() 处理 Result
//             if let Ok(message) = Self::parse_redis_message(&msg) {
//                 if tx.send(message).await.is_err() {
//                     info!("Receiver disconnected, stopping listener");
//                     break;
//                 }
//             } else {
//                 // 忽略解析错误
//                 debug!("Skipping non-message response");
//             }
//         }
//
//         Ok(())
//     }
//
//     /// 简化的消息解析 - 只关注实际的消息内容
//     fn parse_redis_message(value: &Value) -> anyhow::Result<RedisMessage, anyhow::Error> {
//         // 只处理数组类型的消息响应
//         if let Value::Array(elements) = value {
//             if elements.len() >= 3 {
//                 // 检查是否是消息类型
//                 let message_type = match elements.first().unwrap() {
//                     Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
//                     Value::SimpleString(s) => s.clone(),
//                     _ => return Err(anyhow::anyhow!("Invalid message type")),
//                 };
//
//                 // 只处理实际的消息，忽略订阅确认等
//                 if message_type == "message" {
//                     let channel = match &elements[1] {
//                         Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
//                         Value::SimpleString(s) => s.clone(),
//                         _ => return Err(anyhow::anyhow!("Invalid channel format")),
//                     };
//
//                     let payload = match &elements[2] {
//                         Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
//                         Value::SimpleString(s) => s.clone(),
//                         _ => return Err(anyhow::anyhow!("Invalid payload format")),
//                     };
//
//                     return Ok(RedisMessage { message_type, channel, payload });
//                 }
//
//                 // 对于模式订阅的消息
//                 if message_type == "pmessage" && elements.len() >= 4 {
//                     let channel = match &elements[2] {
//                         Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
//                         Value::SimpleString(s) => s.clone(),
//                         _ => return Err(anyhow::anyhow!("Invalid channel format")),
//                     };
//
//                     let payload = match &elements[3] {
//                         Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
//                         Value::SimpleString(s) => s.clone(),
//                         _ => return Err(anyhow::anyhow!("Invalid payload format")),
//                     };
//
//                     return Ok(RedisMessage { message_type, channel, payload });
//                 }
//             }
//         }
//
//         Err(anyhow::anyhow!("Not a message response"))
//     }
//     /// 发布应用消息
//     pub async fn publish_app_message(&self, channel: &str, message: &serde_json::Value) -> Result<i64> {
//         let message_str = serde_json::to_string(message)?;
//         self.redis_cache.publish(channel, &message_str).await
//     }
// }
// //
// // /// 更高级的订阅管理器
// // pub struct SubscriptionManager {
// //     redis_cache: RedisCache,
// //     active_subscriptions: Arc<Mutex<HashSet<String>>>,
// // }
// //
// // impl SubscriptionManager {
// //     pub fn new(redis_cache: RedisCache) -> Self {
// //         Self {
// //             redis_cache,
// //             active_subscriptions: Arc::new(Mutex::new(HashSet::new())),
// //         }
// //     }
// //
// //     /// 管理多个订阅
// //     pub async fn manage_subscriptions(&self, channels: &[&str]) -> Result<mpsc::Receiver<AppMessage>> {
// //         let (tx, rx) = mpsc::channel(100);
// //
// //         for channel in channels {
// //             self.redis_cache.subscribe(channel).await?;
// //             self.active_subscriptions.lock().unwrap().insert(channel.to_string());
// //         }
// //
// //         // 启动监听任务...
// //         Ok(rx)
// //     }
// // }

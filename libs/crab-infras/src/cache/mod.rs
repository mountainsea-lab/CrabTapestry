pub mod redis_cache;
pub mod redis_helper;

/// Redis 消息结构体
#[derive(Debug, Clone)]
pub struct RedisMessage {
    pub message_type: String,
    pub channel: String, // 频道名称
    pub payload: String, // 消息内容
}

//===============function==============
// 简化的消息解析 - 只关注实际的消息内容
// pub fn parse_redis_message(value: &Msg) -> anyhow::Result<RedisMessage, anyhow::Error> {
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

// // 使用基础操作
// let redis_cache = RedisCache::new("redis://localhost:6379").await?;
// redis_cache.subscribe("news").await?;
//
// // 使用业务服务
// let pubsub_service = RedisPubSubService::new(redis_cache.clone());
// let mut message_stream = pubsub_service.create_message_stream("news").await?;
//
// // 使用基础操作发布
// redis_cache.publish("news", "Hello").await?;
//
// // 使用业务服务发布
// pubsub_service.publish_app_message("news", &json!({"text": "Hello"})).await?;

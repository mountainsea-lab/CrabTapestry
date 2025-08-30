pub mod redis_cache;
pub mod redis_helper;

/// Redis 消息结构体
#[derive(Debug, Clone)]
pub struct RedisMessage {
    pub message_type: String,
    pub channel: String, // 频道名称
    pub payload: String, // 消息内容
}

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

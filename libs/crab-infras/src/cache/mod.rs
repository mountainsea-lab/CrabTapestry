use anyhow::Result;
use bb8_redis::redis::{Msg, Value};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, serde::rfc3339};

pub mod bar_cache;
pub mod redis_cache;
pub mod redis_helper;

/// Redis 消息结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisMessage {
    pub message_type: String,
    pub channel: String, // 频道名称
    pub payload: String, // 消息内容
}

/// BaseBar 结构体 - 对应 ta4r 的 BaseBar 类
/// BaseBar struct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseBar {
    /// 时间周期（例如 1 天、15 分钟等）
    pub time_period: i64,
    /// Bar 周期的开始时间（UTC, ISO8601 字符串）
    #[serde(with = "rfc3339")]
    pub begin_time: OffsetDateTime,
    /// Bar 周期的结束时间（UTC, ISO8601 字符串）
    #[serde(with = "rfc3339")]
    pub end_time: OffsetDateTime,
    /// OHLC
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    /// 总交易量
    pub volume: f64,
    /// 总交易金额
    pub amount: f64,
    /// 交易次数
    pub trades: u64,
}

//===============function==============
// 简化的消息解析 - 只关注实际的消息内容
/// 解析 Redis Pub/Sub 消息
pub fn parse_redis_message(msg: &Msg) -> Result<RedisMessage> {
    // 解析频道
    let channel = match msg.get_channel() {
        Ok(Value::BulkString(bytes)) => String::from_utf8_lossy(&*bytes).to_string(),
        Ok(Value::SimpleString(s)) => s.clone(),
        Ok(_) => return Err(anyhow::anyhow!("Invalid channel format").into()), // 处理其它类型
        Err(e) => return Err(anyhow::anyhow!("Error getting channel: {}", e).into()), // 处理错误
    };

    // 解析外层消息内容
    let payload = match msg.get_payload() {
        Ok(Value::BulkString(bytes)) => {
            let payload_str = String::from_utf8_lossy(&*bytes).to_string();

            // 反序列化外层 JSON
            let outer_json: Result<serde_json::Value, _> = serde_json::from_str(&payload_str);
            match outer_json {
                Ok(outer_data) => {
                    // 提取内层的 payload 字段（作为字符串）
                    let inner_payload_str = outer_data
                        .get("payload")
                        .and_then(|v| v.as_str()) // 获取 payload 字段并转换为字符串
                        .ok_or_else(|| anyhow::anyhow!("Missing payload field"))?;
                    inner_payload_str.to_string()
                    // 反序列化内层的 payload 字段为 Payload 结构
                    // match serde_json::from_str::<BaseBar>(inner_payload_str) {
                    //     Ok(parsed_payload) => parsed_payload, // 返回解析后的 Payload
                    //     Err(_) => return Err(anyhow::anyhow!("Failed to parse inner payload as JSON").into()),
                    // }
                }
                Err(_) => return Err(anyhow::anyhow!("Failed to parse outer JSON").into()),
            }
        }
        Ok(Value::SimpleString(s)) => s.clone(),
        Ok(_) => return Err(anyhow::anyhow!("Invalid payload format").into()), // 处理其它类型
        Err(e) => return Err(anyhow::anyhow!("Error getting payload: {}", e).into()), // 处理错误
    };

    // 解析消息类型，使用模式匹配
    let message_type = match msg.get_pattern::<Option<Value>>() {
        Ok(Some(_)) => "pmessage".to_string(), // 如果有模式，则是 pmessage
        Ok(None) => "message".to_string(),     // 如果没有模式，则是 message
        Err(e) => return Err(anyhow::anyhow!("Error getting pattern: {}", e).into()), // 处理错误
    };

    Ok(RedisMessage { message_type, channel, payload })
}

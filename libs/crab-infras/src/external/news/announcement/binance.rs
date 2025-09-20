use crate::external::news::{AnnouncementEvent, AnnouncementTransformer, ExchangeAnnouncementConnector};
use async_trait::async_trait;
use barter_integration::Transformer;
use barter_integration::error::SocketError;
use barter_integration::protocol::StreamParser;
use barter_integration::protocol::websocket::{WebSocketParser, WsMessage, connect};
use diesel::internal::derives::multiconnection::chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{Mutex, broadcast, mpsc};
use uuid::Uuid;

/// ---------------------- 公告模型 ----------------------

#[derive(Debug, Deserialize, Clone)]
pub struct BinanceAnnouncementData {
    #[serde(rename = "publishDate")]
    pub publish_date: u64,
    pub title: String,
    #[serde(default)]
    pub catalog_id: u64,
    #[serde(default)]
    pub topic: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "UPPERCASE")]
pub enum BinanceAnnouncement {
    COMMAND {
        code: Option<u32>,
        msg: Option<String>,
    },
    DATA {
        #[serde(deserialize_with = "de_inner_json")]
        data: BinanceAnnouncementData,
    },
}

/// 将字符串 JSON 解为目标类型
fn de_inner_json<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

/// ---------------------- BinanceAnnouncementTransformer ----------------------

pub struct BinanceAnnouncementTransformer {
    seen: HashSet<(u64, String)>,
    exchange: String,
}

impl BinanceAnnouncementTransformer {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            exchange: "binance".to_string(),
        }
    }
}

impl Default for BinanceAnnouncementTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformer for BinanceAnnouncementTransformer {
    type Error = SocketError;
    type Input = BinanceAnnouncement;
    type Output = AnnouncementEvent;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        match input {
            BinanceAnnouncement::DATA { data } => {
                let key = (data.publish_date, data.title.clone());
                if !self.seen.contains(&key) {
                    self.seen.insert(key.clone());
                    return vec![Ok(AnnouncementEvent {
                        exchange: self.exchange.clone(),
                        topic: data.topic,
                        title: data.title,
                        publish_date: data.publish_date,
                        catalog_id: data.catalog_id,
                    })];
                }
                Vec::new()
            }
            BinanceAnnouncement::COMMAND { code, msg } => {
                if let Some(c) = code {
                    if c != 0 {
                        return vec![Err(SocketError::Exchange(format!(
                            "Binance COMMAND error: code={:?}, msg={:?}",
                            code, msg
                        )))];
                    }
                }
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl AnnouncementTransformer for BinanceAnnouncementTransformer {
    async fn init(
        _ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        initial_cache: Option<Vec<AnnouncementEvent>>,
        _topic: Option<String>,
    ) -> Result<Self, SocketError> {
        let mut transformer = Self::new();
        // 将历史缓存标记为已见，避免重复广播
        if let Some(events) = initial_cache {
            for e in events {
                transformer.seen.insert((e.publish_date, e.title));
            }
        }
        Ok(transformer)
    }
}

/// ---------------------- BinanceAnnouncementConnector ----------------------

pub struct BinanceAnnouncementConnector {
    pub topic: String,
    pub history_size: usize,
}

// impl BinanceAnnouncementConnector {
//     pub fn new(topic: &str) -> Self {
//         Self {
//             topic: topic.to_string(),
//             history_size: 100,
//         }
//     }

//     /// 启动公告订阅
//     pub async fn run<T>(
//         &self,
//         announcement_tx: broadcast::Sender<AnnouncementEvent>,
//         cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
//     ) -> Result<(), SocketError>
//     where
//         T: AnnouncementTransformer
//         + Transformer<Input = BinanceAnnouncement, Output = AnnouncementEvent, Error = SocketError, OutputIter = Vec<Result<AnnouncementEvent, SocketError>>>
//         + Send
//         + 'static
//         + Default,
//     {
//         // 构建订阅 URL
//         let url = format!(
//             "wss://api.binance.com/sapi/wss?random={}&recvWindow=30000&timestamp={}",
//             Uuid::new_v4(),
//             chrono::Utc::now().timestamp_millis()
//         );
//
//         // 使用 barter-rs connect
//         let ws_stream = connect(url).await?;
//         let ws_stream = ws_stream;
//
//         // 分割 Sink/Stream
//         let (mut write, mut read) = ws_stream.split();
//
//         // 发送订阅
//         let sub_msg = serde_json::json!({
//             "command": "SUBSCRIBE",
//             "value": self.topic
//         });
//
//         write.send(WsMessage::text(sub_msg.to_string())).await?;
//
//         // 初始化 Transformer
//         let initial_cache = cache.lock().await.clone().into();
//         let mut transformer = T::init(
//             mpsc::unbounded_channel().0,
//             Some(initial_cache),
//             Some(self.topic.clone()),
//         )
//         .await?;
//
//         // 消息循环
//         while let Some(msg) = read.next().await {
//             if let Some(parsed) = WebSocketParser::parse::<BinanceAnnouncement>(msg) {
//                 match parsed {
//                     Ok(announcement_msg) => {
//                         for res in transformer.transform(announcement_msg) {
//                             if let Ok(event) = res {
//                                 // 广播
//                                 let _ = announcement_tx.send(event.clone());
//                                 // 历史缓存
//                                 let mut cache_lock = cache.lock().await;
//                                 if cache_lock.len() >= self.history_size {
//                                     cache_lock.pop_front();
//                                 }
//                                 cache_lock.push_back(event);
//                             }
//                         }
//                     }
//                     Err(e) => eprintln!("❌ 消息解析错误: {:?}", e),
//                 }
//             }
//         }
//
//         Ok(())
//     }
// }

impl BinanceAnnouncementConnector {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            history_size: 100,
        }
    }

    pub async fn run<T, I>(
        &self,
        announcement_tx: broadcast::Sender<AnnouncementEvent>,
        cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
    ) -> Result<(), SocketError>
    where
        T: AnnouncementTransformer
            + Transformer<
                Input = I,
                Output = AnnouncementEvent,
                Error = SocketError,
                OutputIter = Vec<Result<AnnouncementEvent, SocketError>>,
            > + Send
            + 'static,
        I: Send + 'static + DeserializeOwned,
    {
        // 构建订阅 URL
        let url = format!(
            "wss://api.binance.com/sapi/wss?random={}&recvWindow=30000&timestamp={}",
            Uuid::new_v4(),
            Utc::now().timestamp_millis()
        );

        // 建立连接
        let ws_stream = connect(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // 发送订阅消息
        let sub_msg = serde_json::json!({ "command": "SUBSCRIBE", "value": self.topic });
        write.send(WsMessage::text(sub_msg.to_string())).await?;

        // 初始化 Transformer（同步历史缓存）
        let initial_cache: Vec<AnnouncementEvent> = cache.lock().await.clone().into();
        let mut transformer = T::init(
            mpsc::unbounded_channel().0,
            Some(initial_cache),
            Some(self.topic.clone()),
        )
        .await?;

        // 消息循环
        while let Some(msg) = read.next().await {
            if let Some(parsed) = WebSocketParser::parse::<I>(msg) {
                match parsed {
                    Ok(input_msg) => {
                        // 临时 Vec 避免跨 await 捕获非 Send 迭代器
                        let results: Vec<_> = transformer.transform(input_msg);

                        for res in results {
                            if let Ok(event) = res {
                                let _ = announcement_tx.send(event.clone());

                                // 更新历史缓存
                                let mut cache_lock = cache.lock().await;
                                if cache_lock.len() >= self.history_size {
                                    cache_lock.pop_front();
                                }
                                cache_lock.push_back(event);
                            }
                        }
                    }
                    Err(e) => eprintln!("❌ 消息解析错误: {:?}", e),
                }
            }
        }

        Ok(())
    }
}
#[async_trait]
impl<T> ExchangeAnnouncementConnector<T, BinanceAnnouncement> for BinanceAnnouncementConnector
where
    T: AnnouncementTransformer
        + Transformer<
            Input = BinanceAnnouncement,
            Output = AnnouncementEvent,
            Error = SocketError,
            OutputIter = Vec<Result<AnnouncementEvent, SocketError>>,
        > + Default
        + Send
        + 'static,
{
    async fn run(
        &self,
        announcement_tx: broadcast::Sender<AnnouncementEvent>,
        cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
    ) -> Result<(), SocketError> {
        // 调用 BinanceAnnouncementConnector 内部 run 方法
        Self::new(&self.topic)
            .run::<T, BinanceAnnouncement>(announcement_tx, cache)
            .await
    }
}

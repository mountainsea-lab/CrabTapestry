use crate::external::news::{AnnouncementEvent, AnnouncementTransformer, ExchangeAnnouncementConnector};
use async_trait::async_trait;
use barter_integration::Transformer;
use barter_integration::error::SocketError;
use barter_integration::protocol::StreamParser;
use barter_integration::protocol::websocket::{WebSocketParser, WsMessage, connect};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{Mutex, broadcast, mpsc};

/// ---------------------- 公告模型 ----------------------
#[derive(Debug, Deserialize, Clone)]
pub struct OkxAnnouncementData {
    pub title: String,
    pub content: String,
    pub time: u64,
    #[serde(default)]
    pub topic: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
pub enum OkxAnnouncement {
    Notice {
        arg: Option<String>,
        data: Vec<OkxAnnouncementData>,
    },
}

/// ---------------------- OKX Transformer ----------------------
pub struct OkxAnnouncementTransformer {
    seen: HashSet<(u64, String)>,
    exchange: String,
}

impl OkxAnnouncementTransformer {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            exchange: "okx".to_string(),
        }
    }
}

impl Default for OkxAnnouncementTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformer for OkxAnnouncementTransformer {
    type Error = SocketError;
    type Input = OkxAnnouncement;
    type Output = AnnouncementEvent;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        match input {
            OkxAnnouncement::Notice { data, .. } => data
                .into_iter()
                .filter_map(|d| {
                    let key = (d.time, d.title.clone());
                    if !self.seen.contains(&key) {
                        self.seen.insert(key.clone());
                        Some(Ok(AnnouncementEvent {
                            exchange: self.exchange.clone(),
                            topic: d.topic,
                            title: d.title,
                            publish_date: d.time,
                            catalog_id: 0,
                        }))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}

#[async_trait]
impl AnnouncementTransformer for OkxAnnouncementTransformer {
    async fn init(
        _ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        initial_cache: Option<Vec<AnnouncementEvent>>,
        _topic: Option<String>,
    ) -> Result<Self, SocketError> {
        let mut transformer = Self::new();
        if let Some(events) = initial_cache {
            for e in events {
                transformer.seen.insert((e.publish_date, e.title));
            }
        }
        Ok(transformer)
    }
}

/// ---------------------- OKX Connector ----------------------
pub struct OkxAnnouncementConnector {
    pub topic: String,
    pub history_size: usize,
}

impl OkxAnnouncementConnector {
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
        let url = "wss://ws.okx.com:8443/ws/v5/public";

        let ws_stream = connect(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // 订阅 notice 公告
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{"channel": "notice"}]
        });
        write.send(WsMessage::text(sub_msg.to_string())).await?;

        let initial_cache: Vec<AnnouncementEvent> = cache.lock().await.clone().into();
        let mut transformer = T::init(
            mpsc::unbounded_channel().0,
            Some(initial_cache),
            Some(self.topic.clone()),
        )
        .await?;

        while let Some(msg) = read.next().await {
            if let Some(parsed) = WebSocketParser::parse::<I>(msg) {
                match parsed {
                    Ok(input_msg) => {
                        let results: Vec<_> = transformer.transform(input_msg);
                        for res in results {
                            if let Ok(event) = res {
                                let _ = announcement_tx.send(event.clone());

                                let mut cache_lock = cache.lock().await;
                                if cache_lock.len() >= self.history_size {
                                    cache_lock.pop_front();
                                }
                                cache_lock.push_back(event);
                            }
                        }
                    }
                    Err(e) => eprintln!("❌ OKX 消息解析错误: {:?}", e),
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T> ExchangeAnnouncementConnector<T, OkxAnnouncement> for OkxAnnouncementConnector
where
    T: AnnouncementTransformer
        + Transformer<
            Input = OkxAnnouncement,
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
        Self::new(&self.topic).run::<T, OkxAnnouncement>(announcement_tx, cache).await
    }
}

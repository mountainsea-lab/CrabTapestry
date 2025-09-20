use crate::external::news::{AnnouncementEvent, AnnouncementTransformer, ExchangeAnnouncementConnector};
use barter_integration::Transformer;
use barter_integration::error::SocketError;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};

pub struct AnnouncementManager {
    broadcaster: broadcast::Sender<AnnouncementEvent>,
    history_cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
}

impl AnnouncementManager {
    pub fn new(history_size: usize) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            broadcaster: tx,
            history_cache: Arc::new(Mutex::new(VecDeque::with_capacity(history_size))),
        }
    }

    /// 泛型订阅任意交易所 Connector
    pub async fn subscribe<C, T, I>(&self, connector: C)
    where
        C: ExchangeAnnouncementConnector<T, I> + Send + 'static,
        T: AnnouncementTransformer
            + Transformer<
                Input = I,
                Output = AnnouncementEvent,
                Error = SocketError,
                OutputIter = Vec<Result<AnnouncementEvent, SocketError>>,
            > + Send
            + Default
            + 'static,
        I: Send + 'static,
    {
        let tx = self.broadcaster.clone();
        let cache = self.history_cache.clone();

        tokio::spawn(async move {
            if let Err(e) = connector.run(tx, cache).await {
                eprintln!("❌ Connector error: {:?}", e);
            }
        });
    }

    /// 前端订阅
    pub fn subscribe_frontend(&self) -> broadcast::Receiver<AnnouncementEvent> {
        self.broadcaster.subscribe()
    }

    /// 历史公告
    pub async fn get_history(&self) -> Vec<AnnouncementEvent> {
        let cache = self.history_cache.lock().await;
        cache.iter().cloned().collect()
    }
}

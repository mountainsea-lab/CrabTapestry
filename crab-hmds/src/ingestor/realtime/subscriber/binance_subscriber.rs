use crate::ingestor::realtime::SubscriberStatus;
use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use async_trait::async_trait;
use tokio::task::JoinHandle;

/// Binance 实时订阅器
/// Binance realtime subscriber
pub struct BinanceSubscriber {
    exchange: String,
    ws_url: String,
    status: Arc<Mutex<SubscriberStatus>>,
    running_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl BinanceSubscriber {
    pub fn new() -> Self {
        Self {
            exchange: "binance".into(),
            ws_url: "wss://stream.binance.com:9443/stream?streams=".into(),
            status: Arc::new(Mutex::new(SubscriberStatus::Disconnected)),
            running_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn format_streams(symbols: &[&str]) -> String {
        // Binance WS trade stream: <symbol>@trade
        symbols
            .iter()
            .map(|s| format!("{}@trade", s.to_lowercase()))
            .collect::<Vec<_>>()
            .join("/")
    }
}

#[async_trait]
impl RealtimeSubscriber for BinanceSubscriber {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    async fn subscribe_symbols(&self, symbols: &[&str]) -> Result<Receiver<PublicTradeEvent>> {
        let stream_param = Self::format_streams(symbols);
        let url = format!("{}{}", self.ws_url, stream_param);

        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let (tx, rx) = mpsc::channel(1024);

        *self.status.lock().unwrap() = SubscriberStatus::Connected;

        // spawn a task to read WS messages
        let tx_clone = tx.clone();
        let symbols_set: HashSet<String> = symbols.iter().map(|s| s.to_uppercase()).collect();
        let task = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                if let Ok(Message::Text(txt)) = msg {
                    // 解析 JSON
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&txt) {
                        if let Some(data) = parsed.get("data") {
                            if let Ok(trade) = serde_json::from_value::<BinanceRawTrade>(data.clone()) {
                                if symbols_set.contains(&trade.s) {
                                    let event = PublicTradeEvent {
                                        ts: trade.T as i64,
                                        exchange: Arc::from("binance"),
                                        symbol: Arc::from(trade.s.clone()),
                                        price: trade.p.parse().unwrap_or(0.0),
                                        qty: trade.q.parse().unwrap_or(0.0),
                                        side: Some(if trade.m { "sell" } else { "buy" }.into()),
                                    };
                                    let _ = tx_clone.send(event).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        self.running_tasks.lock().unwrap().push(task);

        Ok(rx)
    }

    async fn unsubscribe_symbols(&self, symbols: &[&str]) -> Result<()> {
        // TODO: 可实现动态取消订阅，通过 WS 发送 unsubscribe 消息
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        // TODO: 可以优雅停止所有任务
        *self.status.lock().unwrap() = SubscriberStatus::Disconnected;
        Ok(())
    }

    fn status(&self) -> SubscriberStatus {
        self.status.lock().unwrap().clone()
    }
}

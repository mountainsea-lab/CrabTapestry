/// Pipeline 统一管理多交易所多币种多周期
pub struct MarketDataPipeline {
    subscribers: HashMap<String, Arc<dyn RealtimeSubscriber>>,
    aggregators: Arc<Mutex<HashMap<(String, String, String), MultiPeriodAggregator>>>,
    pub ohlcv_tx: mpsc::Sender<OHLCVRecord>,
    pub ohlcv_rx: Mutex<mpsc::Receiver<OHLCVRecord>>,
}

impl MarketDataPipeline {
    pub fn new(subscribers: HashMap<String, Arc<dyn RealtimeSubscriber>>) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        Self {
            subscribers,
            aggregators: Arc::new(Mutex::new(HashMap::new())),
            ohlcv_tx: tx,
            ohlcv_rx: Mutex::new(rx),
        }
    }

    /// 启动订阅和聚合
    pub async fn subscribe(&self, subs: Vec<Subscription>) -> Result<(), anyhow::Error> {
        for sub in subs {
            let subscriber = self
                .subscribers
                .get(sub.exchange.as_ref())
                .ok_or_else(|| anyhow::anyhow!("No subscriber for {}", sub.exchange))?;

            // 启动订阅 tick
            let mut trade_rx = subscriber.subscribe_symbols(&[sub.symbol.as_ref()]).await?;

            // 初始化聚合器
            let mut agg = MultiPeriodAggregator::new(sub.exchange.clone(), sub.symbol.clone(), sub.periods.clone());
            self.aggregators.lock().unwrap().insert(
                (sub.exchange.to_string(), sub.symbol.to_string(), "multi".to_string()),
                agg,
            );

            let ohlcv_tx = self.ohlcv_tx.clone();
            let aggregators = self.aggregators.clone();
            let exchange = sub.exchange.clone();
            let symbol = sub.symbol.clone();

            // 每个 symbol 启动独立任务
            tokio::spawn(async move {
                while let Some(trade) = trade_rx.recv().await {
                    let mut aggs = aggregators.lock().unwrap();
                    if let Some(multi_agg) =
                        aggs.get_mut(&(exchange.to_string(), symbol.to_string(), "multi".to_string()))
                    {
                        let bars = multi_agg.on_event(trade);
                        for bar in bars {
                            let _ = ohlcv_tx.send(bar).await;
                        }
                    }
                }
            });
        }
        Ok(())
    }

    /// 获取统一 OHLCV channel
    pub fn receiver(&self) -> mpsc::Receiver<OHLCVRecord> {
        self.ohlcv_rx.lock().unwrap().clone()
    }
}

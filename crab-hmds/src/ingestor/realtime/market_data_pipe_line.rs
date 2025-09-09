use crate::ingestor::realtime::aggregator::multi_period_aggregator::MultiPeriodAggregator;
use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use crate::ingestor::realtime::Subscription;
use crate::ingestor::types::OHLCVRecord;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::warn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

/// 管理每个 symbol 的订阅任务
/// manage each symbol's subscription task
struct SymbolTask {
    handle: JoinHandle<()>,
    cancel_tx: watch::Sender<bool>, // 支持多次发送取消信号
}

/// Pipeline 主体：负责实时订阅、聚合和广播
/// Pipeline core: responsible for real-time subscription, aggregation and broadcasting
pub struct MarketDataPipeline {
    /// exchange -> subscriber
    subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>>,

    /// (exchange, symbol, "multi") -> MultiPeriodAggregator
    aggregators: Arc<DashMap<(String, String, String), MultiPeriodAggregator>>,

    /// broadcast 输出给多个消费者
    ohlcv_tx: broadcast::Sender<OHLCVRecord>,

    /// 已订阅交易对 (exchange, symbol) -> Subscription
    subscribed: Arc<DashMap<(String, String), Subscription>>,

    /// symbol 的任务句柄
    tasks: Arc<DashMap<(String, String), SymbolTask>>,

    /// 每个交易所已订阅币种集合
    status: Arc<DashMap<String, Arc<DashMap<String, ()>>>>,
}

impl MarketDataPipeline {
    /// 创建 Pipeline
    /// create a new pipeline
    pub fn new(
        subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>>,
        broadcast_capacity: usize,
    ) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            subscribers,
            aggregators: Arc::new(DashMap::new()),
            ohlcv_tx: tx,
            subscribed: Arc::new(DashMap::new()),
            tasks: Arc::new(DashMap::new()),
            status: Arc::new(DashMap::new()),
        }
    }

    /// 订阅单个 symbol
    pub async fn subscribe_symbol(&self, sub: Subscription) -> anyhow::Result<()> {
        let exchange_s = sub.exchange.to_string();
        let symbol_s = sub.symbol.to_string();
        let key = (exchange_s.clone(), symbol_s.clone());

        // 已经订阅则直接返回
        if self.subscribed.contains_key(&key) {
            return Ok(());
        }

        // 获取 subscriber
        let subscriber = self
            .subscribers
            .get(&exchange_s)
            .ok_or_else(|| anyhow::anyhow!("No subscriber for {}", exchange_s))?
            .clone();

        // 保存订阅
        self.subscribed.insert(key.clone(), sub.clone());

        // 更新交易所状态集合
        let exch_status = self
            .status
            .entry(exchange_s.clone())
            .or_insert_with(|| Arc::new(DashMap::new()));
        exch_status.insert(symbol_s.clone(), ());

        // 初始化聚合器
        let key_multi = (exchange_s.clone(), symbol_s.clone(), "multi".to_string());
        self.aggregators.entry(key_multi.clone()).or_insert_with(|| {
            MultiPeriodAggregator::new(sub.exchange.clone(), sub.symbol.clone(), sub.periods.clone())
        });

        // TradeEvent 流
        let mut trade_rx = subscriber.subscribe_symbols(&[sub.symbol.as_ref()]).await?;

        let aggregators = self.aggregators.clone();
        let ohlcv_tx = self.ohlcv_tx.clone();
        let exchange_clone = exchange_s.clone();
        let symbol_clone = symbol_s.clone();

        // 安全取消通道
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        // 启动任务
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_trade = trade_rx.recv() => {
                        if let Some(trade) = maybe_trade {
                             warn!("stream public trade event {:?}", trade);
                            if let Some(mut agg) = aggregators.get_mut(&key_multi) {
                                // 安全聚合，不 panic
                                let bars = agg.on_event(trade);
                                for bar in bars {
                                    let _ = ohlcv_tx.send(bar);
                                }
                            }
                        } else {
                            // stream closed naturally
                            break;
                        }
                    }
                    _ = cancel_rx.changed() => {
                        if *cancel_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            warn!("stream closed for {}/{}", exchange_clone, symbol_clone);
        });

        self.tasks.insert(key, SymbolTask { handle, cancel_tx });

        Ok(())
    }

    /// 取消订阅单个 symbol
    pub async fn unsubscribe_symbol(&self, exchange: &str, symbol: &str) -> anyhow::Result<()> {
        let key = (exchange.to_string(), symbol.to_string());

        self.subscribed.remove(&key);
        self.aggregators
            .remove(&(exchange.to_string(), symbol.to_string(), "multi".to_string()));

        // 取消任务
        if let Some((_, task)) = self.tasks.remove(&key) {
            let _ = task.cancel_tx.send(true);
            let _ = task.handle.await;
        }

        // 更新交易所状态
        if let Some(exch_status) = self.status.get(exchange) {
            exch_status.remove(symbol);
        }

        // 调用 subscriber 取消（可选）
        if let Some(subscriber) = self.subscribers.get(exchange) {
            let _ = subscriber.unsubscribe_symbols(&[symbol]).await;
        }

        Ok(())
    }

    /// 批量订阅多个 symbol（同一个 exchange，高性能模板）
    pub async fn subscribe_many(
        &self,
        exchange: Arc<str>,
        symbols: Vec<Arc<str>>,
        periods: Vec<Arc<str>>,
    ) -> anyhow::Result<()> {
        let exchange_s = exchange.to_string();

        // 获取 subscriber
        let subscriber = self
            .subscribers
            .get(&exchange_s)
            .ok_or_else(|| anyhow::anyhow!("No subscriber for {}", exchange_s))?
            .clone();

        // 获取或创建交易所状态集合
        let exch_status = self
            .status
            .entry(exchange_s.clone())
            .or_insert_with(|| Arc::new(DashMap::new()));

        // 筛选未订阅的 symbol
        let symbols_to_subscribe: Vec<_> = symbols
            .into_iter()
            .filter(|sym| {
                let key = (exchange_s.clone(), sym.to_string());
                !self.subscribed.contains_key(&key)
            })
            .collect();

        if symbols_to_subscribe.is_empty() {
            return Ok(());
        }

        // 为每个 symbol 单独订阅，避免 clone Receiver
        for sym in symbols_to_subscribe {
            let key = (exchange_s.clone(), sym.to_string());

            // 保存订阅信息
            let sub = Subscription {
                exchange: exchange.clone(),
                symbol: sym.clone(),
                periods: periods.clone(),
            };
            self.subscribed.insert(key.clone(), sub.clone());
            exch_status.insert(sym.to_string(), ());

            // 初始化聚合器
            let key_multi = (exchange_s.clone(), sym.to_string(), "multi".to_string());
            self.aggregators.entry(key_multi.clone()).or_insert_with(|| {
                MultiPeriodAggregator::new(sub.exchange.clone(), sub.symbol.clone(), sub.periods.clone())
            });

            // 每个 symbol 独立 TradeEvent 流
            let mut trade_rx = subscriber.clone().subscribe_symbols(&[sub.symbol.as_ref()]).await?;

            // 克隆 Arc 用于任务
            let aggregators = self.aggregators.clone();
            let ohlcv_tx = self.ohlcv_tx.clone();
            let exchange_clone = exchange_s.clone();
            let symbol_clone = sym.clone();

            // 安全取消通道
            let (cancel_tx, mut cancel_rx) = watch::channel(false);

            // 启动任务
            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        maybe_trade = trade_rx.recv() => {
                            if let Some(trade) = maybe_trade {
                                if let Some(mut agg) = aggregators.get_mut(&key_multi) {
                                    let bars = agg.on_event(trade);
                                    for bar in bars {
                                        let _ = ohlcv_tx.send(bar);
                                    }
                                }
                            } else {
                                break; // 底层流自然关闭
                            }
                        }
                        _ = cancel_rx.changed() => {
                            if *cancel_rx.borrow() {
                                break; // 主动取消
                            }
                        }
                    }
                }
                warn!(
                    "stream closed for {}/{}: {}",
                    exchange_clone,
                    symbol_clone,
                    if *cancel_rx.borrow() { "canceled" } else { "natural" }
                );
            });

            // 保存任务
            self.tasks.insert(key, SymbolTask { handle, cancel_tx });
        }

        Ok(())
    }

    /// 批量取消多个 symbol（同一个 exchange，高性能模板）
    pub async fn unsubscribe_many(&self, exchange: &str, symbols: &[&str]) -> anyhow::Result<()> {
        let exchange_s = exchange.to_string();

        // 获取交易所状态集合
        let exch_status_opt = self.status.get(&exchange_s);

        // 并发取消每个 symbol 的任务
        let mut handles = Vec::with_capacity(symbols.len());
        for &sym in symbols {
            let key = (exchange_s.clone(), sym.to_string());

            // 移除订阅信息和聚合器
            self.subscribed.remove(&key);
            self.aggregators
                .remove(&(exchange_s.clone(), sym.to_string(), "multi".to_string()));

            // 取消 symbol 任务
            if let Some((_, task)) = self.tasks.remove(&key) {
                let h = tokio::spawn(async move {
                    let _ = task.cancel_tx.send(true);
                    let _ = task.handle.await;
                });
                handles.push(h);
            }

            // 更新交易所状态集合
            if let Some(exch_status) = exch_status_opt.as_ref() {
                exch_status.remove(sym);
            }
        }

        // 等待所有取消任务完成
        for h in handles {
            let _ = h.await;
        }

        // 调用 subscriber 批量取消（可选）
        if let Some(subscriber) = self.subscribers.get(&exchange_s) {
            let _ = subscriber.unsubscribe_symbols(symbols).await;
        }

        Ok(())
    }

    /// 获取 broadcast Receiver
    pub fn subscribe_receiver(&self) -> broadcast::Receiver<OHLCVRecord> {
        self.ohlcv_tx.subscribe()
    }

    /// 查询交易所状态
    pub fn get_subscriber_status(&self, exchange: &str) -> Option<Vec<String>> {
        self.status
            .get(exchange)
            .map(|map| map.iter().map(|kv| kv.key().clone()).collect())
    }

    // 获取订阅总数
    pub fn subscribed_count(&self) -> usize {
        self.subscribed.len()
    }
}

//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::Arc;
//
//
//     #[tokio::test]
//     async fn test_subscribe_symbol() {
//         let mut pipeline = MarketDataPipeline::new(
//             vec![("exchange1".to_string(), Arc::new(MockSubscriber::new(10)))].into_iter().collect(),
//             10,
//         );
//
//         let subscription = Subscription {
//             exchange: Arc::new("exchange1".into()),
//             symbol: Arc::new("symbol-1".into()),
//             periods: vec![Arc::new("1m".into())],
//         };
//
//         // Test subscription
//         assert!(pipeline.subscribe_symbol(subscription.clone()).await.is_ok());
//
//         // Verify subscription status
//         let status = pipeline.get_subscriber_status("exchange1");
//         assert!(status.is_some());
//         assert_eq!(status.unwrap().len(), 1);
//
//         // Verify subscribed count
//         assert_eq!(pipeline.subscribed_count(), 1);
//     }
//
//     #[tokio::test]
//     async fn test_unsubscribe_symbol() {
//         let mut pipeline = MarketDataPipeline::new(
//             vec![("exchange1".to_string(), Arc::new(MockSubscriber::new(10)))].into_iter().collect(),
//             10,
//         );
//
//         let subscription = Subscription {
//             exchange: "exchange1".to_string(),
//             symbol: "symbol-1".to_string(),
//             periods: vec!["1m".to_string()],
//         };
//
//         // Subscribe first
//         pipeline.subscribe_symbol(subscription.clone()).await.unwrap();
//
//         // Unsubscribe
//         assert!(pipeline.unsubscribe_symbol("exchange1", "symbol-1").await.is_ok());
//
//         // Verify the status is removed
//         let status = pipeline.get_subscriber_status("exchange1");
//         assert!(status.is_some());
//         assert_eq!(status.unwrap().len(), 0);
//
//         // Verify subscribed count
//         assert_eq!(pipeline.subscribed_count(), 0);
//     }
//
//     #[tokio::test]
//     async fn test_subscribe_many_symbols() {
//         let mut pipeline = MarketDataPipeline::new(
//             vec![("exchange1".to_string(), Arc::new(MockSubscriber::new(10)))].into_iter().collect(),
//             10,
//         );
//
//         let exchange = "exchange1".to_string();
//         let symbols = vec![
//             "symbol-1".to_string(),
//             "symbol-2".to_string(),
//             "symbol-3".to_string(),
//         ];
//         let periods = vec!["1m".to_string()];
//
//         // Subscribe multiple symbols
//         pipeline
//             .subscribe_many(Arc::from(exchange.clone()), symbols.into_iter().map(Arc::from).collect(), periods)
//             .await
//             .unwrap();
//
//         // Verify subscription
//         let status = pipeline.get_subscriber_status(&exchange);
//         assert!(status.is_some());
//         assert_eq!(status.unwrap().len(), 3);
//
//         // Verify subscribed count
//         assert_eq!(pipeline.subscribed_count(), 3);
//     }
//
//     #[tokio::test]
//     async fn test_unsubscribe_many_symbols() {
//         let mut pipeline = MarketDataPipeline::new(
//             vec![("exchange1".to_string(), Arc::new(MockSubscriber::new(10)))].into_iter().collect(),
//             10,
//         );
//
//         let exchange = "exchange1".to_string();
//         let symbols = vec![
//             "symbol-1".to_string(),
//             "symbol-2".to_string(),
//             "symbol-3".to_string(),
//         ];
//         let periods = vec!["1m".to_string()];
//
//         // Subscribe multiple symbols
//         pipeline
//             .subscribe_many(Arc::from(exchange.clone()), symbols.clone().into_iter().map(Arc::from).collect(), periods)
//             .await
//             .unwrap();
//
//         // Unsubscribe multiple symbols
//         pipeline
//             .unsubscribe_many(&exchange, &symbols.iter().map(AsRef::as_ref).collect::<Vec<_>>())
//             .await
//             .unwrap();
//
//         // Verify status is empty
//         let status = pipeline.get_subscriber_status(&exchange);
//         assert!(status.is_some());
//         assert_eq!(status.unwrap().len(), 0);
//
//         // Verify subscribed count is zero
//         assert_eq!(pipeline.subscribed_count(), 0);
//     }
// }
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};
    use crate::ingestor::realtime::subscriber::binance_subscriber::BinanceSubscriber;

    /// 通用测试工具函数
    /// exchange: 交易所名称
    /// symbols: 待订阅币种列表
    /// periods: K线周期列表
    /// expected_count: 每个币种期望接收的 OHLCV 数量
    pub async fn run_pipeline_test(
        exchange: &str,
        symbols: &[&str],
        periods: &[&str],
        expected_count: usize,
    ) -> anyhow::Result<Vec<OHLCVRecord>> {
        // -------------------------------
        // Step 1: 初始化 Subscriber
        // -------------------------------
        // 创建 broadcast channel
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1024);
        let subscriber = Arc::new(BinanceSubscriber::new(broadcast_tx.clone()));

        let mut subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>> = HashMap::new();
        subscribers.insert(exchange.to_string(), subscriber.clone());

        // -------------------------------
        // Step 2: 初始化 Pipeline
        // -------------------------------
        let pipeline = MarketDataPipeline::new(subscribers, 1024);

        // -------------------------------
        // Step 3: 创建接收端
        // -------------------------------
        let mut ohlcv_rx = pipeline.subscribe_receiver();

        // -------------------------------
        // Step 4: 批量订阅
        // -------------------------------
        let symbols_arc: Vec<Arc<str>> = symbols.iter().map(|s| Arc::from(*s)).collect();
        let periods_arc: Vec<Arc<str>> = periods.iter().map(|p| Arc::from(*p)).collect();

        pipeline
            .subscribe_many(Arc::from(exchange), symbols_arc.clone(), periods_arc.clone())
            .await?;

        // 等待 subscriber 建立 WebSocket 连接
        tokio::time::sleep(Duration::from_secs(600)).await;

        // -------------------------------
        // Step 5: 等待接收 OHLCV 数据
        // -------------------------------
        let mut received_bars = Vec::new();
        let start = tokio::time::Instant::now();
        let max_wait = Duration::from_secs(800); // 超时保护

        while received_bars.len() < expected_count * symbols.len() && start.elapsed() < max_wait {
            tokio::select! {
                Ok(bar) = ohlcv_rx.recv() => {
                    println!("Received OHLCV: {} {} {}", bar.exchange, bar.symbol, bar.open);
                    received_bars.push(bar);
                }
                _ = sleep(Duration::from_millis(500)) => {}
            }
        }

        if received_bars.is_empty() {
            anyhow::bail!("No OHLCV bars received for exchange {}", exchange);
        }

        // -------------------------------
        // Step 6: 批量取消订阅
        // -------------------------------
        let symbols_str: Vec<&str> = symbols.to_vec();
        pipeline.unsubscribe_many(exchange, &symbols_str).await?;

        Ok(received_bars)
    }

    #[tokio::test]
    async fn test_pipeline_with_tool() {
        let exchange = "binance";
        let symbols = &["BTCUSDT", "ETHUSDT"];
        let periods = &["1m", "5m"];
        let expected_count = 3;

        let bars = run_pipeline_test(exchange, symbols, periods, expected_count)
            .await
            .expect("Pipeline test failed");

        println!("Total OHLCV bars received: {}", bars.len());
        assert!(bars.len() >= expected_count * symbols.len());
    }
}

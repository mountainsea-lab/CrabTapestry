use crate::ingestor::realtime::aggregator::multi_period_aggregator::MultiPeriodAggregator;
use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use crate::ingestor::realtime::{SubscriberStatus, Subscription};
use crate::ingestor::types::OHLCVRecord;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{info, warn};
use std::collections::HashMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, oneshot, watch};
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

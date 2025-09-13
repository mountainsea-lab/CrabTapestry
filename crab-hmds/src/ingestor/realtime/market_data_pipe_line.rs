use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use crate::ingestor::types::OHLCVRecord;
use crab_common_utils::time_utils::parse_period_to_millis;
use crab_infras::aggregator::trade_aggregator::TradeAggregatorPool;
use crab_infras::config::sub_config::Subscription;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;

/// Pipeline 主体：负责实时订阅、聚合和广播
/// Pipeline core: responsible for real-time subscription, aggregation and broadcasting
pub struct MarketDataPipeline {
    /// exchange -> subscriber
    subscribers: HashMap<String, Arc<dyn RealtimeSubscriber + Send + Sync>>,

    /// (exchange, symbol, "multi") -> MultiPeriodAggregator
    aggregators: Arc<TradeAggregatorPool>,

    /// broadcast 输出给多个消费者
    ohlcv_tx: Sender<OHLCVRecord>,

    /// 已订阅交易对 (exchange, symbol) -> Subscription
    subscribed: Arc<DashMap<(String, String), Subscription>>,

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
            aggregators: Arc::new(TradeAggregatorPool::new()),
            ohlcv_tx: tx,
            subscribed: Arc::new(DashMap::new()),
            status: Arc::new(DashMap::new()),
        }
    }

    /// 批量订阅多个 symbol
    /// 批量订阅多个 symbol
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

        // 获取或创建交易所状态
        let exch_status = self
            .status
            .entry(exchange_s.clone())
            .or_insert_with(|| Arc::new(DashMap::new()));

        // 筛选未订阅的 symbols
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

        // 保存订阅信息
        for sym in &symbols_to_subscribe {
            let key = (exchange_s.clone(), sym.to_string());
            let sub = Subscription {
                exchange: exchange.clone(),
                symbol: sym.clone(),
                periods: periods.clone(),
            };
            self.subscribed.insert(key.clone(), sub.clone());
            exch_status.insert(sym.to_string(), ());
        }

        // 批量订阅 trade 流
        let trade_rx = subscriber
            .subscribe_symbols(&symbols_to_subscribe.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .await?;

        // 克隆必要对象
        let ohlcv_tx = self.ohlcv_tx.clone();
        let aggregator_pool = self.aggregators.clone();
        let subscribed_map = Arc::clone(&self.subscribed);

        // 启动泛型 worker，支持 mpsc 或 broadcast 输出
        aggregator_pool.start_workers_generic::<Sender<OHLCVRecord>>(
            4,              // worker 数量
            None,           // crossbeam_rx 可选
            Some(trade_rx), // tokio_rx
            ohlcv_tx,       // 输出 sink
            subscribed_map, // 已订阅信息
        );

        Ok(())
    }

    /// 批量取消多个 symbol（同一个 exchange）
    pub async fn unsubscribe_many(&self, exchange: &str, symbols: &[&str]) -> anyhow::Result<()> {
        let exchange_s = exchange.to_string();

        // 获取交易所状态集合
        let exch_status_opt = self.status.get(&exchange_s);

        // 移除订阅信息和聚合器
        for &sym in symbols {
            let key = (exchange_s.clone(), sym.to_string());

            // 获取周期
            if let Some(sub) = self.subscribed.get(&key) {
                for period in &sub.periods {
                    let period_ms = parse_period_to_millis(period).unwrap_or(60_000) as u64;
                    let agg_key = (exchange_s.clone(), sym.to_string(), period_ms);
                    self.aggregators.remove(&agg_key);
                }
            }

            // 删除订阅信息和交易所状态
            self.subscribed.remove(&key);
            if let Some(exch_status) = exch_status_opt.as_ref() {
                exch_status.remove(sym);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestor::realtime::subscriber::binance_subscriber::BinanceSubscriber;
    use crab_common_utils::time_utils::parse_period_to_millis;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

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

        // 等待 subscriber 建立 WebSocket 连接 + 周期聚合
        let max_period_sec: u64 = periods
            .iter()
            .filter_map(|p| parse_period_to_millis(p)) // 返回 Option<u64>，失败的过滤掉
            .max() // 找到最大的
            .unwrap_or(60000) as u64; // 如果全都解析失败，默认 60 秒

        tokio::time::sleep(Duration::from_secs(max_period_sec + 10)).await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pipeline_with_tool() {
        ms_tracing::setup_tracing();

        let exchange = "BinanceFuturesUsd";
        let symbols = &["btc", "eth", "sol"];
        let periods = &["1m", "5m"];
        let expected_count = 3;

        let bars = run_pipeline_test(exchange, symbols, periods, expected_count)
            .await
            .expect("Pipeline test failed");

        println!("Total OHLCV bars received: {}", bars.len());
        assert!(bars.len() >= expected_count * symbols.len());
    }
}

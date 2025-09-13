use crate::aggregator::types::{PublicTradeEvent, Subscription, TradeCandle};
use crate::aggregator::{AggregatorOutput, OutputSink};
use crate::cache::BaseBar;
use crab_common_utils::time_utils::parse_period_to_millis;
use crossbeam::channel::Receiver;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task;
use trade_aggregation::{
    Aggregator, AlignedTimeRule, GenericAggregator, MillisecondPeriod, TimestampResolution, Trade,
};

pub type AggregatorKey = (String, String, u64); // (exchange, symbol, timeframe)

/// 定义聚合器
/// Define the aggregator
pub struct AggregatorWithCounter {
    pub aggregator: GenericAggregator<TradeCandle, AlignedTimeRule, Trade>,
    pub candle_count: usize,
    pub last_update: Instant, // 上次使用时间，用于清理
}

#[derive(Clone)]
pub struct TradeAggregatorPool {
    aggregators: Arc<DashMap<AggregatorKey, Arc<RwLock<AggregatorWithCounter>>>>,
}

impl TradeAggregatorPool {
    pub fn new() -> Self {
        Self { aggregators: Arc::new(DashMap::new()) }
    }

    /// 获取或创建聚合器（多线程安全）
    /// Get or create an aggregator (multi-threaded safe)
    pub fn get_or_create_aggregator(
        &self,
        exchange: &str,
        symbol: &str,
        time_period: u64,
    ) -> Arc<RwLock<AggregatorWithCounter>> {
        let key = (exchange.to_string(), symbol.to_string(), time_period);

        let entry = self.aggregators.entry(key).or_insert_with(|| {
            Arc::new(RwLock::new(AggregatorWithCounter {
                aggregator: GenericAggregator::new(
                    AlignedTimeRule::new(
                        MillisecondPeriod::from_non_zero(time_period),
                        TimestampResolution::Millisecond,
                    ),
                    false,
                ),
                candle_count: 0,
                last_update: Instant::now(),
            }))
        });

        // 克隆 Arc 给 worker 使用
        Arc::clone(entry.value())
    }

    /// 启动定时清理任务，避免 retain 阻塞 worker
    /// Start the scheduled cleaning task to prevent the retain from blocking the worker
    pub fn start_cleanup_task(self: Arc<Self>, stale_duration: Duration, check_interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(check_interval);

            loop {
                ticker.tick().await;
                let now = Instant::now();
                let mut removed = 0;
                let mut to_remove_keys = Vec::new();

                // 异步读取聚合器
                for entry in self.aggregators.iter() {
                    let guard = entry.value().read().await; // tokio::RwLock 异步锁
                    if now.duration_since(guard.last_update) > stale_duration {
                        to_remove_keys.push(entry.key().clone());
                    }
                }

                // batch delete
                for key in to_remove_keys {
                    if self.aggregators.remove(&key).is_some() {
                        removed += 1;
                    }
                }

                if removed > 0 {
                    if removed > 10 {
                        info!("Cleaned up {} stale aggregators", removed);
                    }
                }
            }
        });
    }

    /// 弹性异步 worker，支持 crossbeam 或 tokio 输入，泛型输出 BaseBar 或 TradeCandle
    pub fn start_workers_generic<S>(
        self: Arc<Self>,
        worker_num: usize,
        crossbeam_rx: Option<Arc<Receiver<PublicTradeEvent>>>, // crossbeam 可选
        tokio_rx: Option<mpsc::Receiver<PublicTradeEvent>>,    // tokio 可选
        output_tx: S,                                          // 泛型输出
        subscribed: Arc<DashMap<(String, String), Subscription>>,
    ) where
        S: OutputSink,
        S::Item: AggregatorOutput,
    {
        // 1️⃣ 桥接 crossbeam -> tokio channel
        let bridge_rx = if let Some(cb_rx) = crossbeam_rx {
            let (bridge_tx, bridge_rx) = mpsc::channel::<PublicTradeEvent>(1024);
            let cb_rx_clone = cb_rx.clone();
            std::thread::spawn(move || {
                while let Ok(event) = cb_rx_clone.recv() {
                    let _ = bridge_tx.blocking_send(event);
                }
            });
            bridge_rx
        } else if let Some(tokio_rx) = tokio_rx {
            tokio_rx
        } else {
            panic!("Either crossbeam_rx or tokio_rx must be provided");
        };

        let bridge_rx = Arc::new(Mutex::new(bridge_rx));

        // 2️⃣ 弹性异步 worker
        for _worker_id in 0..worker_num {
            let subscribed = Arc::clone(&subscribed.clone());
            let pool = self.clone();
            let tx = output_tx.clone();
            let rx = bridge_rx.clone();

            task::spawn(async move {
                loop {
                    let maybe_event = {
                        let mut rx_lock = rx.lock().await;
                        rx_lock.recv().await
                    };

                    let event = match maybe_event {
                        Some(ev) => ev,
                        None => break, // channel 关闭退出
                    };

                    // 遍历周期
                    if let Some(sub) = subscribed.get(&(event.exchange.clone(), event.symbol.clone())) {
                        for period in sub.periods.iter() {
                            let period_sec = parse_period_to_millis(period).unwrap_or(60 * 1000);

                            // 获取或创建聚合器
                            let aggregator =
                                pool.get_or_create_aggregator(&event.exchange, &event.symbol, period_sec as u64);

                            // 异步写锁
                            let mut guard = aggregator.write().await;
                            guard.last_update = Instant::now();

                            let trade: Trade = (&event).into();
                            if let Some(trade_candle) = guard.aggregator.update(&trade) {
                                if guard.candle_count >= 1 {
                                    // info!("agg trade candle {:?}", trade_candle);
                                    let exchange = event.exchange.clone();
                                    let symbol = event.symbol.clone();
                                    let timestamp = trade.timestamp;

                                    let item =
                                        S::Item::from_trade_candle(&exchange, &symbol, period, timestamp, trade_candle);
                                    let _ = tx.send(item).await;
                                }
                                guard.candle_count += 1;
                            }
                        }
                    }
                }

                // info!("[Worker-{worker_id}] exiting");
            });
        }
    }
}

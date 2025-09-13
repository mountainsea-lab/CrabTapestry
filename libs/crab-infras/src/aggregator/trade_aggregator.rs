use crate::aggregator::types::{PublicTradeEvent, TradeCandle};
use crate::cache::BaseBar;
use crossbeam::channel::Receiver;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{error, info};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use trade_aggregation::{
    Aggregator, AlignedTimeRule, GenericAggregator, MillisecondPeriod, TimestampResolution, Trade,
};

pub type AggregatorKey = (String, String, u64); // (exchange, symbol, timeframe)

/// 定义聚合器
/// Define the aggregator
struct AggregatorWithCounter {
    aggregator: GenericAggregator<TradeCandle, AlignedTimeRule, Trade>,
    candle_count: usize,
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
    fn get_or_create_aggregator(
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

                for entry in self.aggregators.iter() {
                    if let Ok(guard) = entry.value().read() {
                        if now.duration_since(guard.last_update) > stale_duration {
                            to_remove_keys.push(entry.key().clone());
                        }
                    }
                }

                // batch delete
                for key in to_remove_keys {
                    if self.aggregators.remove(&key).is_some() {
                        removed += 1;
                    }
                }

                if removed > 0 {
                    // 优化日志输出，只在移除数量较大时打印
                    // Optimize log output and print only when the number of removals is large
                    if removed > 10 {
                        info!("Cleaned up {} stale aggregators", removed);
                    }
                }
            }
        });
    }

    /// 启动 worker 池
    /// start worker pool
    pub fn start_workers(
        self: Arc<Self>,
        worker_num: usize,
        receiver: Arc<Receiver<PublicTradeEvent>>, // 使用 Arc 包裹 Receiver
        output_tx: mpsc::Sender<BaseBar>,
    ) {
        for id in 0..worker_num {
            let rx = Arc::clone(&receiver); // 共享 Arc
            let pool = self.clone();
            let tx = output_tx.clone();

            // start a thread execute
            std::thread::spawn(move || {
                while let Ok(event) = rx.recv() {
                    // 阻塞式 recv
                    let aggregator = pool.get_or_create_aggregator(
                        &event.exchange,
                        &event.symbol,
                        event.time_period, // Use the event's own time period
                    );

                    let mut guard = aggregator.write().unwrap(); // 获取写锁
                    guard.last_update = Instant::now();

                    let trade: Trade = (&event).into();
                    // debug!("[Worker-{id}] Failed to update aggregator for trade: {trade:?}");
                    if let Some(trade_candle) = guard.aggregator.update(&trade) {
                        if guard.candle_count >= 1 {
                            // convert TradeCandle to BaseBar then send
                            let base_bar: BaseBar = trade_candle.into(); // 使用 From trait 转换
                            if let Err(e) = tx.blocking_send(base_bar) {
                                error!("[Worker-{id}] send error: {e}");
                            }
                        }
                        guard.candle_count += 1;
                    } else {
                        // debug!("[Worker-{id}] Failed to update aggregator for trade: {event:?}");
                    }
                }
            });
        }
    }
}

use crate::aggregator::types::{PublicTradeEvent, TradeCandle};
use crate::cache::BaseBar;
use crab_common_utils::time_utils::parse_period_to_secs;
use crossbeam::channel::Receiver;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{error, info};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
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

    /// 弹性异步 worker，输出 TradeCandle
    pub fn start_workers_final(
        self: Arc<Self>,
        periods: Arc<Vec<Arc<str>>>,
        mut input_rx: mpsc::Receiver<PublicTradeEvent>,
        output_tx: mpsc::Sender<TradeCandle>,
    ) {
        let pool = self.clone();

        // 单独 task 监听 input_rx
        task::spawn(async move {
            while let Some(event) = input_rx.recv().await {
                let pool = pool.clone();
                let tx = output_tx.clone();
                let periods = periods.clone();
                // 弹性 spawn 处理每个 trade
                task::spawn(async move {
                    pool.process_trade_event(periods, event, &tx).await;
                });
            }
        });
    }

    /// 处理单个 trade 事件
    async fn process_trade_event(
        &self,
        periods: Arc<Vec<Arc<str>>>,
        event: PublicTradeEvent,
        output_tx: &mpsc::Sender<TradeCandle>,
    ) {
        // 遍历事件对应的所有周期
        for period_sec in periods.iter().map(|p| parse_period_to_secs(p).unwrap_or(60)) {
            // 获取或创建聚合器
            let aggregator = self.get_or_create_aggregator(
                &event.exchange,
                &event.symbol,
                period_sec, // Use the event's own time period
            );

            let mut guard = aggregator.write().unwrap();
            guard.last_update = Instant::now();

            let trade: Trade = (&event).into();

            if let Some(trade_candle) = guard.aggregator.update(&trade) {
                // 初始第一根可能数据不全 丢弃掉
                if guard.candle_count >= 1 {
                    // 输出 TradeCandle
                    let _ = output_tx.send(trade_candle);
                }
                guard.candle_count += 1;
            }
        }
    }
}

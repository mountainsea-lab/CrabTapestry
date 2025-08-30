use crate::aggregator::types::{BaseBar, TradeCandle};
use crate::ingestion::types::PublicTradeEvent;
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

/// 封装 aggregator 以及 candle count
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
    pub fn start_cleanup_task(self: Arc<Self>, stale_duration: Duration, check_interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(check_interval);
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let mut removed = 0;
                let mut to_remove_keys = Vec::new();

                // 遍历 DashMap shard 安全
                for entry in self.aggregators.iter() {
                    if let Ok(guard) = entry.value().read() {
                        if now.duration_since(guard.last_update) > stale_duration {
                            // 标记为待删除
                            to_remove_keys.push(entry.key().clone());
                        }
                    }
                }

                // 批量删除
                for key in to_remove_keys {
                    if self.aggregators.remove(&key).is_some() {
                        removed += 1;
                    }
                }

                if removed > 0 {
                    // 优化日志输出，只在移除数量较大时打印
                    if removed > 10 {
                        info!("Cleaned up {} stale aggregators", removed);
                    }
                }
            }
        });
    }

    /// 启动 worker 池
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

            // 启动一个线程来处理事件
            std::thread::spawn(move || {
                while let Ok(event) = rx.recv() {
                    // 阻塞式 recv
                    let aggregator = pool.get_or_create_aggregator(
                        &event.exchange,
                        &event.symbol,
                        event.time_period, // 使用事件自身周期
                    );

                    let mut guard = aggregator.write().unwrap(); // 获取写锁
                    guard.last_update = Instant::now();

                    let trade: Trade = (&event).into();
                    // debug!("[Worker-{id}] Failed to update aggregator for trade: {trade:?}");
                    // 在 worker 代码中使用
                    if let Some(trade_candle) = guard.aggregator.update(&trade) {
                        if guard.candle_count >= 1 {
                            // 将 TradeCandle 转换为 BaseBar 并发送
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

use crate::aggregator::types::{PublicTradeEvent, TradeCandle};
use crate::cache::BaseBar;
use crab_common_utils::time_utils::parse_period_to_secs;
use crossbeam::channel::Receiver;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{error, info};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, Mutex};
use tokio::task;
use trade_aggregation::{
    Aggregator, AlignedTimeRule, GenericAggregator, MillisecondPeriod, TimestampResolution, Trade,
};
use crate::aggregator::AggregatorOutput;

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

    // /// 启动定时清理任务，避免 retain 阻塞 worker
    // /// Start the scheduled cleaning task to prevent the retain from blocking the worker
    // pub fn start_cleanup_task(self: Arc<Self>, stale_duration: Duration, check_interval: Duration) {
    //     tokio::spawn(async move {
    //         let mut ticker = tokio::time::interval(check_interval);
    //         loop {
    //             ticker.tick().await;
    //             let now = Instant::now();
    //             let mut removed = 0;
    //             let mut to_remove_keys = Vec::new();
    //
    //             for entry in self.aggregators.iter() {
    //                 if let Ok(guard) = entry.value().read() {
    //                     if now.duration_since(guard.last_update) > stale_duration {
    //                         to_remove_keys.push(entry.key().clone());
    //                     }
    //                 }
    //             }
    //
    //             // batch delete
    //             for key in to_remove_keys {
    //                 if self.aggregators.remove(&key).is_some() {
    //                     removed += 1;
    //                 }
    //             }
    //
    //             if removed > 0 {
    //                 // 优化日志输出，只在移除数量较大时打印
    //                 // Optimize log output and print only when the number of removals is large
    //                 if removed > 10 {
    //                     info!("Cleaned up {} stale aggregators", removed);
    //                 }
    //             }
    //         }
    //     });
    // }

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

    /// 启动 worker 池
    /// start worker pool
    // pub fn start_workers(
    //     self: Arc<Self>,
    //     worker_num: usize,
    //     receiver: Arc<Receiver<PublicTradeEvent>>, // 使用 Arc 包裹 Receiver
    //     output_tx: mpsc::Sender<BaseBar>,
    // ) {
    //     for id in 0..worker_num {
    //         let rx = Arc::clone(&receiver); // 共享 Arc
    //         let pool = self.clone();
    //         let tx = output_tx.clone();
    //
    //         // start a thread execute
    //         std::thread::spawn(move || {
    //             while let Ok(event) = rx.recv() {
    //                 // 阻塞式 recv
    //                 let aggregator = pool.get_or_create_aggregator(
    //                     &event.exchange,
    //                     &event.symbol,
    //                     event.time_period, // Use the event's own time period
    //                 );
    //
    //                 let mut guard = aggregator.write().await; // 获取写锁
    //                 guard.last_update = Instant::now();
    //
    //                 let trade: Trade = (&event).into();
    //                 // debug!("[Worker-{id}] Failed to update aggregator for trade: {trade:?}");
    //                 if let Some(trade_candle) = guard.aggregator.update(&trade) {
    //                     if guard.candle_count >= 1 {
    //                         // convert TradeCandle to BaseBar then send
    //                         let base_bar: BaseBar = trade_candle.into(); // 使用 From trait 转换
    //                         if let Err(e) = tx.blocking_send(base_bar) {
    //                             error!("[Worker-{id}] send error: {e}");
    //                         }
    //                     }
    //                     guard.candle_count += 1;
    //                 } else {
    //                     // debug!("[Worker-{id}] Failed to update aggregator for trade: {event:?}");
    //                 }
    //             }
    //         });
    //     }
    // }

    // pub fn start_workers_bridge(
    //     self: Arc<Self>,
    //     worker_num: usize,
    //     crossbeam_rx: Arc<Receiver<PublicTradeEvent>>,
    //     output_tx: mpsc::Sender<BaseBar>, // 或 TradeCandle
    //     periods: Arc<Vec<Arc<str>>>,      // 多周期聚合
    // ) {
    //     // 1️⃣ 创建 tokio channel 桥接 crossbeam
    //     let (bridge_tx, bridge_rx) = mpsc::channel::<PublicTradeEvent>(1024);
    //     let crossbeam_rx_clone = crossbeam_rx.clone();
    //
    //     // 桥接线程：阻塞接收 crossbeam 消息，发送到 tokio channel
    //     std::thread::spawn(move || {
    //         while let Ok(event) = crossbeam_rx_clone.recv() {
    //             let _ = bridge_tx.blocking_send(event);
    //         }
    //     });
    //
    //     let bridge_rx = Arc::new(tokio::sync::Mutex::new(bridge_rx));
    //
    //     // 2️⃣ 启动 worker_num 个 async worker
    //     for worker_id in 0..worker_num {
    //         let pool = self.clone();
    //         let tx = output_tx.clone();
    //         let periods = periods.clone();
    //         let rx = bridge_rx.clone();
    //
    //         task::spawn(async move {
    //             loop {
    //                 let maybe_event = {
    //                     // 锁住 tokio mpsc Receiver，安全取消息
    //                     let mut rx_lock = rx.lock().await;
    //                     rx_lock.recv().await
    //                 };
    //
    //                 let event = match maybe_event {
    //                     Some(ev) => ev,
    //                     None => break, // channel 已关闭，退出 worker
    //                 };
    //
    //                 // 遍历所有周期
    //                 for period in periods.iter() {
    //                     let period_sec = parse_period_to_secs(period).unwrap_or(60);
    //
    //                     // 获取或创建聚合器
    //                     let aggregator = pool.get_or_create_aggregator(&event.exchange, &event.symbol, period_sec);
    //
    //                     let mut guard = aggregator.write().await;
    //                     guard.last_update = Instant::now();
    //
    //                     let trade: Trade = (&event).into();
    //                     if let Some(trade_candle) = guard.aggregator.update(&trade) {
    //                         if guard.candle_count >= 1 {
    //                             // 输出 BaseBar 或 TradeCandle
    //                             let base_bar: BaseBar = trade_candle.clone().into();
    //                             let _ = tx.send(base_bar).await;
    //                         }
    //                         guard.candle_count += 1;
    //                     }
    //                 }
    //             }
    //
    //             info!("[Worker-{worker_id}] exiting");
    //         });
    //     }
    // }

    // /// 弹性异步 worker，输出 TradeCandle
    // pub fn start_workers_final(
    //     self: Arc<Self>,
    //     periods: Arc<Vec<Arc<str>>>,
    //     mut input_rx: mpsc::Receiver<PublicTradeEvent>,
    //     output_tx: mpsc::Sender<TradeCandle>,
    // ) {
    //     let pool = self.clone();
    //
    //     // 单独任务监听 input_rx
    //     task::spawn(async move {
    //         while let Some(event) = input_rx.recv().await {
    //             let pool = pool.clone();
    //             let tx = output_tx.clone();
    //             let periods = periods.clone();
    //             let event = Arc::new(event); // Arc 包装避免 move
    //
    //             // 弹性 spawn 处理每个 trade
    //             task::spawn(async move {
    //                 pool.process_trade_event(periods, Arc::clone(&event), &tx).await;
    //             });
    //         }
    //     });
    // }
    //
    // /// 处理单个 trade 事件
    // async fn process_trade_event(
    //     &self,
    //     periods: Arc<Vec<Arc<str>>>,
    //     event: Arc<PublicTradeEvent>,
    //     output_tx: &mpsc::Sender<TradeCandle>,
    // ) {
    //     for period_sec in periods.iter().map(|p| parse_period_to_secs(p).unwrap_or(60)) {
    //         // 获取或创建聚合器
    //         let aggregator = self.get_or_create_aggregator(&event.exchange, &event.symbol, period_sec);
    //
    //         // 异步写锁
    //         let mut guard = aggregator.write().await;
    //         guard.last_update = Instant::now();
    //
    //         let trade: Trade = (&*event).into();
    //
    //         if let Some(trade_candle) = guard.aggregator.update(&trade) {
    //             guard.candle_count += 1;
    //
    //             // 输出 TradeCandle
    //             let _ = output_tx.send(trade_candle).await;
    //         }
    //     }
    // }

    /// 弹性异步 worker，支持 crossbeam 或 tokio 输入，泛型输出 BaseBar 或 TradeCandle
    pub fn start_workers_generic<O>(
        self: Arc<Self>,
        worker_num: usize,
        crossbeam_rx: Option<Arc<Receiver<PublicTradeEvent>>>, // crossbeam 可选
        tokio_rx: Option<mpsc::Receiver<PublicTradeEvent>>,    // tokio 可选
        output_tx: mpsc::Sender<O>,                            // 泛型输出
        periods: Arc<Vec<Arc<str>>>,                          // 多周期聚合
    )
    where
        O: AggregatorOutput,
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
        for worker_id in 0..worker_num {
            let pool = self.clone();
            let tx = output_tx.clone();
            let periods = periods.clone();
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
                    for period in periods.iter() {
                        let period_sec = parse_period_to_secs(period).unwrap_or(60);

                        // 获取或创建聚合器
                        let aggregator = pool.get_or_create_aggregator(
                            &event.exchange,
                            &event.symbol,
                            period_sec,
                        );

                        // 异步写锁
                        let mut guard = aggregator.write().await;
                        guard.last_update = Instant::now();

                        let trade: Trade = (&event).into();
                        if let Some(trade_candle) = guard.aggregator.update(&trade) {
                            guard.candle_count += 1;

                            // 输出泛型
                            let _ = tx.send(O::from(trade_candle.clone())).await;
                        }
                    }
                }

                info!("[Worker-{worker_id}] exiting");
            });
        }
    }
}

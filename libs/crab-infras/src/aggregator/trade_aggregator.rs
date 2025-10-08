use crate::aggregator::types::{PublicTradeEvent, TradeCandle};
use crate::aggregator::{AggregatorOutput, OutputSink};
use crate::config::sub_config::Subscription;
use crab_common_utils::time_utils::{milliseconds_to_offsetdatetime, parse_period_to_millis};
use crab_types::bar_cache::bar_key::BarKey;
use crab_types::time_frame::TimeFrame;
use crossbeam::channel::Receiver;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::info;
use parking_lot::RwLock;
use rust_decimal::prelude::FromPrimitive;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use ta4r::bar::base_bar::BaseBar;
use ta4r::num::decimal_num::DecimalNum;
use tokio::sync::{Mutex, mpsc};
use tokio::task;
use trade_aggregation::{
    Aggregator, AlignedTimeRule, CandleComponent, GenericAggregator, MillisecondPeriod, TimestampResolution, Trade,
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
                    let guard = entry.value().read(); // tokio::RwLock 异步锁
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
                            let mut guard = aggregator.write();
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
                                    let _ = tx.send(item);
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

    /// 聚合单笔交易，并返回生成的 TradeCandle 转换后的 BaseBar
    pub fn aggregate_trade(
        &self,
        event: &PublicTradeEvent,
        periods: &[i64], // 毫秒单位
    ) -> Vec<(BarKey, BaseBar<DecimalNum>)> {
        let mut results = Vec::with_capacity(periods.len());

        for &period in periods {
            let aggregator = self.get_or_create_aggregator(&event.exchange, &event.symbol, period as u64);
            let mut guard = aggregator.write();
            guard.last_update = Instant::now();

            // 转换为 trade_aggregation::Trade
            let trade: Trade = event.into();

            if let Some(trade_candle) = guard.aggregator.update(&trade) {
                guard.candle_count += 1;
                // 直接同步转换为 BaseBar
                let exchange = event.exchange.clone();
                let symbol = event.symbol.clone();
                // info!("trade_candle {:?}", trade_candle);
                if let Some(period_str) = TimeFrame::millis_to_str(period) {
                    let bar_key = BarKey::new(&exchange, &symbol, period_str);
                    results.push((bar_key, Self::candle_to_basebar(&trade_candle, period)));
                }
            }
        }
        results
    }

    /// trade_candle -> BaseBar 转换
    fn candle_to_basebar(candle: &TradeCandle, period: i64) -> BaseBar<DecimalNum> {
        let open = DecimalNum::from_f64(candle.open.value()).unwrap_or(DecimalNum::new(0));
        let high = DecimalNum::from_f64(candle.high.value()).unwrap_or(DecimalNum::new(0));
        let low = DecimalNum::from_f64(candle.low.value()).unwrap_or(DecimalNum::new(0));
        let close = DecimalNum::from_f64(candle.close.value()).unwrap_or(DecimalNum::new(0));
        let volume = DecimalNum::from_f64(candle.volume.value()).unwrap_or(DecimalNum::new(0));
        let amount = DecimalNum::from_f64(candle.close.value() * candle.volume.value()).unwrap_or(DecimalNum::new(0));

        BaseBar::<DecimalNum> {
            time_period: time::Duration::milliseconds(period),
            begin_time: milliseconds_to_offsetdatetime(candle.time_range.open_time),
            end_time: milliseconds_to_offsetdatetime(candle.time_range.close_time),
            open_price: Some(open),
            high_price: Some(high),
            low_price: Some(low),
            close_price: Some(close),
            volume,
            amount: Some(amount),
            trades: candle.num_trades.value() as u64,
        }
    }

    /// 移除指定聚合器
    /// key = (exchange, symbol, period)
    pub fn remove(&self, key: &AggregatorKey) -> Option<Arc<RwLock<AggregatorWithCounter>>> {
        self.aggregators.remove(key).map(|(_, v)| v)
    }

    /// 可选：批量移除
    pub fn remove_many(&self, keys: &[AggregatorKey]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.remove(key).is_some() {
                removed += 1;
            }
        }
        removed
    }
}

// 🌟 自定义 Debug
impl fmt::Debug for TradeAggregatorPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 收集 keys（只打印前几个以避免太长）
        let keys: Vec<_> = self.aggregators.iter().take(5).map(|entry| entry.key().clone()).collect();

        write!(
            f,
            "TradeAggregatorPool {{ total_aggregators: {}, keys: {:?}{} }}",
            self.aggregators.len(),
            keys,
            if self.aggregators.len() > keys.len() {
                ", ..."
            } else {
                ""
            }
        )
    }
}

use crate::ingestor::buffer::data_buffer::{CapacityStrategy, DataBuffer};
use crate::ingestor::ctrservice::{
    BufferStats, ControlMsg, DedupStats, IngestorConfig, IngestorHealth, InternalMsg, MarketDataType, ServiceState,
};
use crate::ingestor::dedup::Deduplicatable;
use crate::ingestor::dedup::deduplicator::{DedupMode, Deduplicator};
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::realtime::Subscription;
use crate::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crate::ingestor::scheduler::HistoricalBatchEnum;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::types::{OHLCVRecord, TickRecord, TradeRecord};
use ms_tracing::tracing_utils::internal::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;

/// IngestorService 核心结构
pub struct IngestorService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 生命周期状态
    pub state: Arc<RwLock<ServiceState>>,

    /// 控制消息通道
    pub control_tx: mpsc::Sender<ControlMsg>,
    pub control_rx: Arc<RwLock<mpsc::Receiver<ControlMsg>>>,

    /// 内部错误/信息上报通道
    pub internal_tx: mpsc::Sender<InternalMsg>,
    pub internal_rx: Arc<RwLock<mpsc::Receiver<InternalMsg>>>,

    /// 异步子任务句柄
    pub handles: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// 历史回补调度器
    pub backfill: Arc<BaseBackfillScheduler<F>>,

    /// 实时订阅管理
    pub pipeline: Arc<MarketDataPipeline>,

    /// 去重器
    pub dedup_ohlcv: Arc<Deduplicator<OHLCVRecord>>,
    pub dedup_tick: Arc<Deduplicator<TickRecord>>,
    pub dedup_trade: Arc<Deduplicator<TradeRecord>>,

    /// 异步缓冲
    pub buffer_ohlcv: Arc<DataBuffer<OHLCVRecord>>,
    pub buffer_tick: Arc<DataBuffer<TickRecord>>,
    pub buffer_trade: Arc<DataBuffer<TradeRecord>>,

    /// Graceful shutdown 通知
    pub shutdown: Arc<Notify>,
}

impl<F> IngestorService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 创建服务实例
    pub fn new(
        backfill: Arc<BaseBackfillScheduler<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(1024);
        let (internal_tx, internal_rx) = mpsc::channel(64);

        let dedup_ohlcv = Arc::new(Deduplicator::<OHLCVRecord>::new(60_000));
        let dedup_tick = Arc::new(Deduplicator::<TickRecord>::new(60_000));
        let dedup_trade = Arc::new(Deduplicator::<TradeRecord>::new(60_000));

        let buffer_ohlcv = DataBuffer::new(Some(1000), 10, CapacityStrategy::DropOldest);
        let buffer_tick = DataBuffer::new(Some(10_000), 50, CapacityStrategy::Block);
        let buffer_trade = DataBuffer::new(Some(10_000), 50, CapacityStrategy::Block);

        Self {
            state: Arc::new(RwLock::new(ServiceState::Initialized)),
            control_tx,
            control_rx: Arc::new(RwLock::new(control_rx)),
            internal_tx,
            internal_rx: Arc::new(RwLock::new(internal_rx)),
            handles: Arc::new(RwLock::new(Vec::new())),
            dedup_ohlcv,
            dedup_tick,
            dedup_trade,
            buffer_ohlcv,
            buffer_tick,
            buffer_trade,
            shutdown,
            backfill,
            pipeline,
        }
    }

    /// 启动完整服务
    pub async fn start_full(&self, config: Option<IngestorConfig>) {
        *self.state.write().await = ServiceState::Running;

        // 启动 Control Task
        let control_task = self.spawn_control_task();
        self.handles.write().await.push(control_task);

        // 启动 Backfill Task
        let backfill_task = self.spawn_backfill_task();
        self.handles.write().await.push(backfill_task);

        // 启动 Realtime Task
        let realtime_task = self.spawn_realtime_task();
        self.handles.write().await.push(realtime_task);

        // 启动 Buffer 批量消费 Task
        let buffer_task = self.spawn_buffer_consumer_task();
        self.handles.write().await.push(buffer_task);

        // 初始化订阅配置
        if let Some(cfg) = config {
            self.initialize_subscriptions(&cfg).await;
        }
    }

    /// 停止服务（graceful shutdown）
    pub async fn stop(&self) {
        self.shutdown.notify_waiters();

        let mut handles = self.handles.write().await;
        for handle in handles.drain(..) {
            let _ = handle.await;
        }

        *self.state.write().await = ServiceState::Stopped;
    }

    /// 初始化订阅配置（按交易所批量订阅）
    pub async fn initialize_subscriptions(&self, config: &IngestorConfig) {
        for exch in &config.exchanges {
            // 批量发送一条 ControlMsg::SubscribeMany
            if let Err(e) = self
                .control_tx
                .send(ControlMsg::SubscribeMany {
                    exchange: exch.exchange.clone().into(),
                    symbols: exch.symbols.iter().cloned().map(Into::into).collect(),
                    periods: exch.periods.iter().cloned().map(Into::into).collect(),
                })
                .await
            {
                let _ = self
                    .internal_tx
                    .send(InternalMsg::Error(format!(
                        "Failed to enqueue subscription for {}: {}",
                        exch.exchange, e
                    )))
                    .await;
            }
        }
    }

    /// Control Task: 处理 Subscribe / Unsubscribe / HealthCheck
    fn spawn_control_task(&self) -> JoinHandle<()> {
        let rx = self.control_rx.clone(); // 不需要 mut，因为我们在循环内重新获取
        let pipeline = self.pipeline.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            loop {
                // 在每次迭代中重新获取锁，避免长时间阻塞其他任务
                let msg = {
                    let mut control_rx_guard = rx.write().await;
                    tokio::select! {
                        _ = shutdown.notified() => {
                            break;
                        }
                        msg = control_rx_guard.recv() => msg
                    }
                };

                // 处理消息（此时已释放锁）
                if let Some(msg) = msg {
                    match msg {
                        ControlMsg::Subscribe { exchange, symbol, periods } => {
                            let subscription = Subscription { exchange, symbol, periods };
                            if let Err(e) = pipeline.subscribe_symbol(subscription).await {
                                let _ = internal_tx.send(InternalMsg::Error(format!("Subscribe error: {}", e))).await;
                            }
                        }
                        ControlMsg::SubscribeMany { exchange, symbols, periods } => {
                            if let Err(e) = pipeline.subscribe_many(exchange, symbols, periods).await {
                                let _ = internal_tx
                                    .send(InternalMsg::Error(format!("SubscribeMany error: {}", e)))
                                    .await;
                            }
                        }
                        ControlMsg::Unsubscribe { exchange, symbol } => {
                            let _ = pipeline.unsubscribe_symbol(exchange.as_ref(), symbol.as_ref());
                        }
                        ControlMsg::UnsubscribeMany { exchange, symbols } => {
                            let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_ref()).collect();
                            let _ = pipeline.unsubscribe_many(exchange.as_ref(), &symbol_refs);
                        }
                        ControlMsg::HealthCheck => {
                            // TODO: 可扩展发送状态到监控系统
                        }
                        ControlMsg::Start | ControlMsg::Stop => {
                            // 已在外部处理，这里不做
                        }
                    }
                } else {
                    // 通道关闭
                    break;
                }
            }
        })
    }

    /// Backfill Task: 批量消费历史数据: 拉取 + 批量去重 + 入 Buffer
    pub fn spawn_backfill_task(&self) -> JoinHandle<()> {
        let backfill = self.backfill.clone();
        let dedup_ohlcv = self.dedup_ohlcv.clone();
        let dedup_tick = self.dedup_tick.clone();
        let dedup_trade = self.dedup_trade.clone();
        let buffer_ohlcv = self.buffer_ohlcv.clone();
        let buffer_tick = self.buffer_tick.clone();
        let buffer_trade = self.buffer_trade.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            let mut rx = backfill.output_rx();
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        info!("Backfill task shutdown triggered");
                        break;
                    },
                    maybe_batch = rx.recv() => {
                    let batch = match maybe_batch {
                        Ok(b) => b,
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("Backfill output channel closed, exiting task");
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Lagging behind by {} messages, skipping", n);
                            continue;
                        }
                    };

                        if let Err(e) = async {
                            match batch {
                                HistoricalBatchEnum::OHLCV(hb) => {
                                    let deduped = dedup_ohlcv.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        let _ = buffer_ohlcv.push_batch(deduped).await;
                                        info!("Backfill OHLCV batch pushed: symbol={}, period={}, size={}",
                                            hb.symbol, hb.period.as_deref().unwrap_or("n/a"), hb.data.len());
                                    } else {
                                        debug!("OHLCV batch fully deduplicated: symbol={}, period={}", hb.symbol, hb.period.as_deref().unwrap_or("n/a"));
                                    }
                                },
                                HistoricalBatchEnum::Tick(hb) => {
                                    let deduped = dedup_tick.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        let _ = buffer_tick.push_batch(deduped).await;
                                        info!("Backfill Tick batch pushed: symbol={}, size={}", hb.symbol, hb.data.len());
                                    } else {
                                        debug!("Tick batch fully deduplicated: symbol={}", hb.symbol);
                                    }
                                },
                                HistoricalBatchEnum::Trade(hb) => {
                                    let deduped = dedup_trade.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        let _ = buffer_trade.push_batch(deduped).await;
                                        info!("Backfill Trade batch pushed: symbol={}, size={}", hb.symbol, hb.data.len());
                                    } else {
                                        debug!("Trade batch fully deduplicated: symbol={}", hb.symbol);
                                    }
                                },
                            }
                            Ok::<(), anyhow::Error>(())
                        }.await {
                            let _ = internal_tx.send(InternalMsg::Error(format!("Backfill task error: {}", e))).await;
                            error!("Backfill task error: {}", e);
                        }
                    }
                }
            }
            info!("Backfill task exited cleanly");
        })
    }

    /// Realtime Task: 流式消费实时数据 -> 去重 -> Buffer
    /// Realtime Task: 目前仅处理 OHLCV 实时数据
    pub fn spawn_realtime_task(&self) -> JoinHandle<()> {
        let pipeline = self.pipeline.clone();
        let dedup_ohlcv = self.dedup_ohlcv.clone();
        let buffer_ohlcv = self.buffer_ohlcv.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            let mut rx = pipeline.subscribe_receiver(); // broadcast::Receiver<OHLCVRecord>

            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    maybe_bar = rx.recv() => {
                        let bar = match maybe_bar {
                            Ok(b) => b,
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Realtime OHLCV pipeline lagged, skipped {} messages", n);
                                continue;
                            }
                        };

                        // 异步处理去重和 buffer 写入
                        let deduped = dedup_ohlcv.deduplicate(vec![bar.clone()], DedupMode::Realtime);
                        if !deduped.is_empty() {
                            let buffer = buffer_ohlcv.clone();
                            let internal_tx = internal_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = buffer.push_batch(deduped).await {
                                    let _ = internal_tx.send(
                                        InternalMsg::Error(format!("Realtime OHLCV buffer push failed: {}", e))
                                    ).await;
                                }
                            });
                        }
                    }
                }
            }

            info!("Realtime OHLCV ingestion task stopped");
        })
    }

    // /// Buffer 批量消费 Task，分别处理 OHLCV/Tick/Trade
    // pub fn spawn_buffer_consumer_task(&self) -> JoinHandle<()> {
    //     let buffer_ohlcv = self.buffer_ohlcv.clone();
    //     let buffer_tick = self.buffer_tick.clone();
    //     let buffer_trade = self.buffer_trade.clone();
    //     let shutdown = self.shutdown.clone();
    //     let internal_tx = self.internal_tx.clone();
    //
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = shutdown.notified() => break,
    //                 _ = buffer_ohlcv.notify.notified() => {
    //                     let mut batch = Vec::new();
    //                     while let Some(item) = buffer_ohlcv.pop() {
    //                         batch.push(item);
    //                         if batch.len() >= buffer_ohlcv.batch_notify_size {
    //                             break;
    //                         }
    //                     }
    //
    //                     if !batch.is_empty() {
    //                         if let Err(e) = async {
    //                             println!("Processing OHLCV batch of size {}", batch.len());
    //                             Ok::<(), anyhow::Error>(())
    //                         }.await {
    //                             let _ = internal_tx.send(
    //                                 InternalMsg::Error(format!("OHLCV buffer processing error: {}", e))
    //                             ).await;
    //                         }
    //                     }
    //                 },
    //                 _ = buffer_tick.notify.notified() => {
    //                     let mut batch = Vec::new();
    //                     while let Some(item) = buffer_tick.pop() {
    //                         batch.push(item);
    //                         if batch.len() >= buffer_tick.batch_notify_size {
    //                             break;
    //                         }
    //                     }
    //
    //                     if !batch.is_empty() {
    //                         if let Err(e) = async {
    //                             println!("Processing Tick batch of size {}", batch.len());
    //                             Ok::<(), anyhow::Error>(())
    //                         }.await {
    //                             let _ = internal_tx.send(
    //                                 InternalMsg::Error(format!("Tick buffer processing error: {}", e))
    //                             ).await;
    //                         }
    //                     }
    //                 },
    //                 _ = buffer_trade.notify.notified() => {
    //                     let mut batch = Vec::new();
    //                     while let Some(item) = buffer_trade.pop() {
    //                         batch.push(item);
    //                         if batch.len() >= buffer_trade.batch_notify_size {
    //                             break;
    //                         }
    //                     }
    //
    //                     if !batch.is_empty() {
    //                         if let Err(e) = async {
    //                             println!("Processing Trade batch of size {}", batch.len());
    //                             Ok::<(), anyhow::Error>(())
    //                         }.await {
    //                             let _ = internal_tx.send(
    //                                 InternalMsg::Error(format!("Trade buffer processing error: {}", e))
    //                             ).await;
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //
    //         info!("Buffer consumer task stopped");
    //     })
    // }
    /// Buffer 批量消费 Task，分别处理 OHLCV/Tick/Trade
    pub fn spawn_buffer_consumer_task(&self) -> JoinHandle<()> {
        let buffer_ohlcv = self.buffer_ohlcv.clone();
        let buffer_tick = self.buffer_tick.clone();
        let buffer_trade = self.buffer_trade.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = buffer_ohlcv.notify.notified() => {
                        if let Err(e) = Self::process_buffer_batch::<OHLCVRecord>(&buffer_ohlcv, "OHLCV", &internal_tx).await {
                            let _ = internal_tx.send(InternalMsg::Error(format!("OHLCV buffer processing error: {}", e))).await;
                        }
                    },
                    _ = buffer_tick.notify.notified() => {
                        if let Err(e) = Self::process_buffer_batch::<TickRecord>(&buffer_tick, "Tick", &internal_tx).await {
                            let _ = internal_tx.send(InternalMsg::Error(format!("Tick buffer processing error: {}", e))).await;
                        }
                    },
                    _ = buffer_trade.notify.notified() => {
                        if let Err(e) = Self::process_buffer_batch::<TradeRecord>(&buffer_trade, "Trade", &internal_tx).await {
                            let _ = internal_tx.send(InternalMsg::Error(format!("Trade buffer processing error: {}", e))).await;
                        }
                    }
                }
            }

            info!("Buffer consumer task stopped");
        })
    }

    /// 公共批量处理逻辑，改为使用 `pop_batch` 批量获取数据
    async fn process_buffer_batch<T>(
        buffer: &Arc<DataBuffer<T>>,
        buffer_name: &str,
        internal_tx: &mpsc::Sender<InternalMsg>,
    ) -> Result<(), anyhow::Error>
    where
        T: Deduplicatable + Send + Sync + Clone + 'static,
    {
        // 使用 `pop_batch` 获取一批数据
        let batch = buffer.pop_batch().await;

        if batch.is_empty() {
            // 如果没有数据，则发送一个状态更新消息
            let _ = internal_tx
                .send(InternalMsg::Info(format!(
                    "No data to process in {} buffer",
                    buffer_name
                )))
                .await;
            return Ok(()); // 没有数据，直接返回
        }

        // 处理批量数据
        println!("Processing {} batch of size {}", buffer_name, batch.len());

        // 模拟处理，成功后可以发送一个成功处理的通知
        if let Err(e) = async {
            // 在这里执行一些具体的数据处理，比如写入数据库，调用外部API等
            Ok::<(), anyhow::Error>(()) // 如果没有异常，表示处理成功
        }
        .await
        {
            // 如果处理失败，发送错误消息
            let _ = internal_tx
                .send(InternalMsg::Error(format!(
                    "Error processing {} batch: {}",
                    buffer_name, e
                )))
                .await;
            return Err(e);
        }

        // 处理成功，发送一个成功的通知
        let _ = internal_tx
            .send(InternalMsg::Info(format!(
                "Processed {} batch of size {}",
                buffer_name,
                batch.len()
            )))
            .await;

        Ok(())
    }

    /// 健康状态接口
    pub async fn health_status(&self) -> IngestorHealth {
        let state = { self.state.read().await.clone() };
        let backfill_pending = self.backfill.pending_tasks().await;
        let realtime_subscriptions = self.pipeline.subscribed_count();

        let mut buffer_stats = std::collections::HashMap::new();
        buffer_stats.insert(MarketDataType::OHLCV, BufferStats { len: self.buffer_ohlcv.len() });
        buffer_stats.insert(MarketDataType::Tick, BufferStats { len: self.buffer_tick.len() });
        buffer_stats.insert(MarketDataType::Trade, BufferStats { len: self.buffer_trade.len() });

        let mut dedup_stats = std::collections::HashMap::new();
        dedup_stats.insert(
            MarketDataType::OHLCV,
            DedupStats {
                historical: self.dedup_ohlcv.historical_size(),
                realtime: self.dedup_ohlcv.realtime_size(),
            },
        );
        dedup_stats.insert(
            MarketDataType::Tick,
            DedupStats {
                historical: self.dedup_tick.historical_size(),
                realtime: self.dedup_tick.realtime_size(),
            },
        );
        dedup_stats.insert(
            MarketDataType::Trade,
            DedupStats {
                historical: self.dedup_trade.historical_size(),
                realtime: self.dedup_trade.realtime_size(),
            },
        );

        IngestorHealth {
            state,
            backfill_pending,
            realtime_subscriptions,
            buffer_stats,
            dedup_stats,
        }
    }

    /// 错误监听接口
    pub async fn monitor_errors(&self) {
        let mut rx = self.internal_rx.write().await;
        while let Some(msg) = rx.recv().await {
            match msg {
                InternalMsg::Error(e) => {
                    println!("[ERROR] {}", e);
                    *self.state.write().await = ServiceState::Error(e);
                }
                InternalMsg::Info(info) => {
                    println!("[INFO] {}", info);
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use crate::ingestor::ctrservice::ExchangeConfig;
    use crate::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
    use crate::ingestor::types::{FetchContext, HistoricalSource};

    // 测试 IngestorService 启动与停止功能
    #[tokio::test]
    async fn test_ingestor_service_start_stop() {
        // 1. 设置测试配置
        let config = IngestorConfig {
            exchanges: vec![
                ExchangeConfig {
                    exchange: "binance".to_string(),
                    symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
                    periods: vec!["1m".to_string(), "5m".to_string()],
                },
            ],
        };

        // 2. 创建 Shutdown 通知
        let shutdown = Arc::new(Notify::new());

        // 构造 FetchContext
        let ctx = FetchContext::new_with_past(
            HistoricalSource {
                name: "Binance API".to_string(),
                exchange: "binance".to_string(),
                last_success_ts: 0,
                last_fetch_ts: 0,
                batch_size: 0,
                supports_tick: false,
                supports_trade: false,
                supports_ohlcv: false,
            },
            "binance",
            "BTCUSDT",
            Some("1h"),
            Some(3), // 最近 3 小时
            None,    // 最近 3 小时, // 3 小时
        );

        let fetcher = Arc::new(BinanceFetcher::new());
        // 3. 创建 BackfillScheduler
        let backfill_scheduler = BaseBackfillScheduler::new(fetcher,3);

        // 4. 创建 MarketDataPipeline
        let market_data_pipeline = Arc::new(MarketDataPipeline::new());

        // 5. 创建 IngestorService 实例
        let ingestor_service = IngestorService::<BinanceFetcher>::new(
            backfill_scheduler,
            market_data_pipeline,
            shutdown.clone(),
        );

        // 6. 启动服务
        ingestor_service.start_full(Some(config.clone())).await;

        // 检查是否服务启动，通常可以检查相关状态
        // 这里假设 IngestorService 提供了一个 `is_running()` 方法
        assert!(ingestor_service.is_running());

        // 7. 停止服务
        ingestor_service.stop().await;

        // 再次检查服务是否已停止
        assert!(!ingestor_service.is_running());

        // 8. 其他逻辑验证（如配置是否正确传递）
        let current_config = ingestor_service.get_config().await;
        assert_eq!(current_config, config);
    }

    // 测试无效配置的处理
    #[tokio::test]
    async fn test_ingestor_service_invalid_config() {
        // 传入空配置或无效配置
        let config = IngestorConfig {
            exchanges: vec![], // 传入空的 exchanges 列表
        };

        let shutdown = Arc::new(Notify::new());
        let backfill_scheduler = Arc::new(BaseBackfillScheduler::<MockFetcher>::new());
        let market_data_pipeline = Arc::new(MarketDataPipeline::new());

        let ingestor_service = IngestorService::<BinanceFetcher>::new(
            backfill_scheduler,
            market_data_pipeline,
            shutdown.clone(),
        );

        // 启动服务，验证是否正确处理无效配置
        let result = ingestor_service.start_full(Some(config)).await;

        // 期望返回错误，表示配置无效
        assert!(result.is_err());
    }

    // 测试服务的 Graceful Shutdown 功能
    #[tokio::test]
    async fn test_ingestor_service_shutdown() {
        let shutdown = Arc::new(Notify::new());
        let backfill_scheduler = BaseBackfillScheduler::<MockFetcher>::new());
        let market_data_pipeline = Arc::new(MarketDataPipeline::new());

        let ingestor_service = IngestorService::<BinanceFetcher>::new(
            backfill_scheduler,
            market_data_pipeline,
            shutdown.clone(),
        );

        // 启动服务
        ingestor_service.start_full(None).await;

        // 发送 shutdown 信号
        shutdown.notify_one();

        // 检查服务是否收到关闭信号并正确处理
        // 这里我们假设服务会在 shutdown 后停止，或者提供相应的状态检查方法
        assert!(ingestor_service.is_shutdown());
    }
}

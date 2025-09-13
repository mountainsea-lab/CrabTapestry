use crate::ingestor::buffer::data_buffer::{CapacityStrategy, DataBuffer};
use crate::ingestor::ctrservice::{
    BufferStats, ControlMsg, DedupStats, IngestorConfig, IngestorHealth, InternalMsg, MarketDataType, ServiceParams,
    ServiceState,
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
use std::time::Duration;
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

    /// 用参数初始化服务实例
    pub fn with_params(
        backfill: Arc<BaseBackfillScheduler<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
        params: ServiceParams,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(params.control_channel_size);
        let (internal_tx, internal_rx) = mpsc::channel(params.internal_channel_size);

        let dedup_ohlcv = Arc::new(Deduplicator::<OHLCVRecord>::new(params.dedup_window_ms));
        let dedup_tick = Arc::new(Deduplicator::<TickRecord>::new(params.dedup_window_ms));
        let dedup_trade = Arc::new(Deduplicator::<TradeRecord>::new(params.dedup_window_ms));

        let buffer_ohlcv = DataBuffer::new(
            Some(params.buffer_cap_ohlcv),
            params.buffer_batch_size_ohlcv,
            CapacityStrategy::DropOldest,
        );
        let buffer_tick = DataBuffer::new(
            Some(params.buffer_cap_tick),
            params.buffer_batch_size_tick,
            CapacityStrategy::Block,
        );
        let buffer_trade = DataBuffer::new(
            Some(params.buffer_cap_trade),
            params.buffer_batch_size_trade,
            CapacityStrategy::Block,
        );

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

    pub async fn with_params_and_subscriptions(
        backfill: Arc<BaseBackfillScheduler<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
        params: ServiceParams,
        config: &IngestorConfig,
    ) -> Arc<Self> {
        let service = Arc::new(Self::with_params(backfill, pipeline, shutdown, params));
        service.initialize_subscriptions(config).await;
        service
    }

    /// 停止服务（graceful shutdown）
    pub async fn stop(self: &Arc<Self>) {
        self.shutdown.notify_waiters();

        let mut handles = self.handles.write().await;
        for handle in handles.drain(..) {
            let _ = handle.await;
        }

        *self.state.write().await = ServiceState::Stopped;
    }

    /// 初始化订阅配置（按交易所批量订阅）
    pub async fn initialize_subscriptions(self: &Arc<Self>, config: &IngestorConfig) {
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
    fn spawn_control_task(self: &Arc<Self>) -> JoinHandle<()> {
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
                        ControlMsg::SubscribeMany { exchange, symbols, periods } => {
                            if let Err(e) = pipeline.subscribe_many(exchange, symbols, periods).await {
                                let _ = internal_tx
                                    .send(InternalMsg::Error(format!("SubscribeMany error: {}", e)))
                                    .await;
                            }
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
    pub fn spawn_backfill_task(
        self: &Arc<Self>,
        timeout_secs: u64,   // 可配置超时
        log_interval: usize, // 超时日志打印间隔
    ) -> JoinHandle<()> {
        let service = Arc::clone(self); // Arc<Self>，保证跨 await 点安全

        tokio::spawn(async move {
            let mut rx = service.backfill.subscribe();

            info!("Backfill task started");

            let mut timeout_counter = 0usize;

            loop {
                tokio::select! {
                    // 优先处理 shutdown
                    _ = service.shutdown.notified() => {
                        info!("Backfill task shutdown triggered");
                        break;
                    },

                    // 使用 OutputSubscriber 的 recv_timeout 处理批次
                    maybe_batch = rx.recv_timeout(Duration::from_secs(timeout_secs)) => {
                        match maybe_batch {
                            Some(batch) => {
                                // 收到批次，重置超时计数
                                timeout_counter = 0;

                                if let Err(e) = Self::handle_batch(batch, &service).await {
                                    let _ = service.internal_tx.send(
                                        InternalMsg::Error(format!("Backfill task error: {}", e))
                                    ).await;
                                    error!("Backfill task error: {}", e);
                                }
                            }
                            None => {
                                // 超时未收到批次
                                timeout_counter += 1;
                                if timeout_counter % log_interval == 0 {
                                    debug!(
                                        "Backfill recv_timeout reached {}s without message ({} times)",
                                        timeout_secs,
                                        timeout_counter
                                    );
                                }
                            }
                        }
                    }
                }
            }

            info!("Backfill task exited cleanly");
        })
    }

    /// 批次处理函数封装，直接访问 service 内部字段
    async fn handle_batch(batch: HistoricalBatchEnum, service: &Arc<Self>) -> anyhow::Result<()> {
        match batch {
            HistoricalBatchEnum::OHLCV(hb) => {
                let deduped = service.dedup_ohlcv.deduplicate(hb.data.clone(), DedupMode::Historical);
                if !deduped.is_empty() {
                    service.buffer_ohlcv.push_batch(deduped).await?;
                    info!(
                        "Backfill OHLCV batch pushed: symbol={}, period={}, size={}",
                        hb.symbol,
                        hb.period.as_deref().unwrap_or("n/a"),
                        hb.data.len()
                    );
                } else {
                    debug!(
                        "OHLCV batch fully deduplicated: symbol={}, period={}",
                        hb.symbol,
                        hb.period.as_deref().unwrap_or("n/a")
                    );
                }
            }

            HistoricalBatchEnum::Tick(hb) => {
                let deduped = service.dedup_tick.deduplicate(hb.data.clone(), DedupMode::Historical);
                if !deduped.is_empty() {
                    service.buffer_tick.push_batch(deduped).await?;
                    info!(
                        "Backfill Tick batch pushed: symbol={}, size={}",
                        hb.symbol,
                        hb.data.len()
                    );
                } else {
                    debug!("Tick batch fully deduplicated: symbol={}", hb.symbol);
                }
            }

            HistoricalBatchEnum::Trade(hb) => {
                let deduped = service.dedup_trade.deduplicate(hb.data.clone(), DedupMode::Historical);
                if !deduped.is_empty() {
                    service.buffer_trade.push_batch(deduped).await?;
                    info!(
                        "Backfill Trade batch pushed: symbol={}, size={}",
                        hb.symbol,
                        hb.data.len()
                    );
                } else {
                    debug!("Trade batch fully deduplicated: symbol={}", hb.symbol);
                }
            }
        }

        Ok(())
    }

    /// Realtime Task: 流式消费实时(OHLCV)数据 -> 去重 -> Buffer
    pub fn spawn_realtime_task(self: &Arc<Self>) -> JoinHandle<()> {
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

    /// Buffer 批量消费 Task，分别处理 OHLCV/Tick/Trade
    pub fn spawn_buffer_consumer_task(self: &Arc<Self>) -> JoinHandle<()> {
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
        info!("Processing {} batch of size {}", buffer_name, batch.len());

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
    pub async fn health_status(self: &Arc<Self>) -> IngestorHealth {
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
    pub async fn monitor_errors(self: &Arc<Self>) {
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

    /// 启动所有子任务 + 错误监听
    pub async fn start(self: &Arc<Self>, config: Option<IngestorConfig>) {
        *self.state.write().await = ServiceState::Running;

        // 启动后台任务
        let mut handles = Vec::new();
        handles.push(self.spawn_control_task());
        handles.push(self.spawn_backfill_task(10, 3));
        handles.push(self.spawn_realtime_task());
        handles.push(self.spawn_buffer_consumer_task());
        handles.push(self.spawn_error_monitor_task()); // 错误监听

        *self.handles.write().await = handles;

        // 初始化订阅配置
        if let Some(cfg) = config {
            self.initialize_subscriptions(&cfg).await;
        }
    }

    /// 错误监听任务，随服务启动
    fn spawn_error_monitor_task(self: &Arc<Self>) -> JoinHandle<()> {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut rx = service.internal_rx.write().await;
            while let Some(msg) = rx.recv().await {
                match msg {
                    InternalMsg::Error(e) => {
                        error!("[IngestorService] {}", e);
                        *service.state.write().await = ServiceState::Error(e);
                    }
                    InternalMsg::Info(info) => {
                        debug!("[IngestorService] {}", info);
                    }
                }
            }
        })
    }
}

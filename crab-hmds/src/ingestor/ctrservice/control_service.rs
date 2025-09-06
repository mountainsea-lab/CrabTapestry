use crate::ingestor::buffer::data_buffer::DataBuffer;
use crate::ingestor::ctrservice::{ControlMsg, IngestorConfig, IngestorHealth, InternalMsg, MarketData, ServiceState};
use crate::ingestor::dedup::Deduplicatable;
use crate::ingestor::dedup::deduplicator::{DedupMode, Deduplicator};
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::realtime::Subscription;
use crate::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crate::ingestor::scheduler::HistoricalBatchEnum;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::types::HistoricalBatch;
use futures_util::StreamExt;
use ms_tracing::tracing_utils::internal::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;

/// IngestorService 核心结构
pub struct IngestorService<F, D, B>
where
    F: HistoricalFetcherExt + 'static,
    D: Deduplicatable + Send + Sync + Clone + 'static,
    B: Send + Sync + 'static,
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
    pub dedup: Arc<Deduplicator<D>>,

    /// 高性能异步缓冲
    pub buffer: Arc<DataBuffer<B>>,

    /// Graceful shutdown 通知
    pub shutdown: Arc<Notify>,
}

impl<F, D, B> IngestorService<F, D, B>
where
    F: HistoricalFetcherExt + 'static,
    D: Deduplicatable + Send + Sync + Clone + 'static,
    B: Send + Sync + 'static,
{
    /// 创建服务实例
    pub fn new(
        backfill: Arc<BaseBackfillScheduler<F>>,
        pipeline: Arc<MarketDataPipeline>,
        dedup: Arc<Deduplicator<D>>,
        buffer: Arc<DataBuffer<B>>,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(1024);
        let (internal_tx, internal_rx) = mpsc::channel(64);

        Self {
            state: Arc::new(RwLock::new(ServiceState::Initialized)),
            control_tx,
            control_rx: Arc::new(RwLock::new(control_rx)),
            internal_tx,
            internal_rx: Arc::new(RwLock::new(internal_rx)),
            handles: Arc::new(RwLock::new(Vec::new())),
            backfill,
            pipeline,
            dedup,
            buffer,
            shutdown: Arc::new(Notify::new()),
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
        let mut rx = self.control_rx.clone();
        let pipeline = self.pipeline.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    Some(msg) = rx.write().await.recv() => {
                        match msg {
                            // 单个订阅
                            ControlMsg::Subscribe { exchange, symbol, periods } => {
                                    let subscription = Subscription{exchange,symbol,periods}
                                    if let Err(e) = pipeline.subscribe_symbol(subscription) {
                                        let _ = internal_tx
                                            .send(InternalMsg::Error(format!("Subscribe error: {}", e)))
                                            .await;
                                    }
                            },
                            // 批量订阅
                            ControlMsg::SubscribeMany { exchange, symbols, periods } => {
                                if let Err(e) = pipeline.subscribe_many(
                                    exchange,
                                    symbols,
                                    periods,
                                ) {
                                    let _ = internal_tx
                                        .send(InternalMsg::Error(format!("SubscribeMany error: {}", e)))
                                        .await;
                                }
                            },
                            // 单个退订
                            ControlMsg::Unsubscribe { exchange, symbol } => {
                                let _ = pipeline.unsubscribe_symbol(
                                    exchange.as_ref(),
                                    symbol.as_ref(),
                                );
                            },

                            // 批量退订（保持对称性）
                            ControlMsg::UnsubscribeMany { exchange, symbols } => {
                                let _ = pipeline.unsubscribe_many(
                                    exchange.as_ref(),
                                    symbols.as_ref(),
                                );
                            },

                            ControlMsg::HealthCheck => {
                                // TODO: 可扩展发送状态到监控系统
                            },

                            ControlMsg::Start | ControlMsg::Stop => {
                                // 已在外部处理，这里不做
                            }
                        }
                    }
                }
            }
        })
    }

    /// Backfill Task: 批量消费历史数据: 拉取 + 批量去重 + 入 Buffer
    fn spawn_backfill_task(&self) -> JoinHandle<()> {
        let backfill = self.backfill.clone();
        let dedup = self.dedup.clone();
        let buffer = self.buffer.clone();
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
                            Some(b) => b,
                            None => {
                                info!("Backfill output channel closed, exiting task");
                                break;
                            }
                        };

                        // 异步处理批量去重 + buffer 写入
                        if let Err(e) = async {
                            match &batch {
                                HistoricalBatchEnum::OHLCV(hb) => {
                                    let deduped = dedup.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        buffer.push_batch(deduped).await?;
                                        info!("Backfill OHLCV batch pushed: symbol={}, period={}, size={}",
                                            hb.symbol, hb.period.as_deref().unwrap_or("n/a"), hb.data.len());
                                    } else {
                                        debug!("OHLCV batch fully deduplicated: symbol={}, period={}", hb.symbol, hb.period.as_deref().unwrap_or("n/a"));
                                    }
                                },
                                HistoricalBatchEnum::Tick(hb) => {
                                    let deduped = dedup.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        buffer.push_batch(deduped).await?;
                                        info!("Backfill Tick batch pushed: symbol={}, size={}", hb.symbol, hb.data.len());
                                    } else {
                                        debug!("Tick batch fully deduplicated: symbol={}", hb.symbol);
                                    }
                                },
                                HistoricalBatchEnum::Trade(hb) => {
                                    let deduped = dedup.deduplicate(hb.data.clone(), DedupMode::Historical);
                                    if !deduped.is_empty() {
                                        buffer.push_batch(deduped).await?;
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
    fn spawn_realtime_task(&self) -> JoinHandle<()> {
        let pipeline = self.pipeline.clone();
        let dedup = self.dedup.clone();
        let buffer = self.buffer.clone();
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
                                warn!("Realtime pipeline lagged, skipped {} messages", n);
                                continue;
                            }
                        };

                        // 异步处理去重和 buffer 写入
                        let dedup = dedup.clone();
                        let buffer = buffer.clone();
                        let internal_tx = internal_tx.clone();
                        let md = MarketData::Realtime(bar.clone());

                        tokio::spawn(async move {
                            // 流式去重
                            let stream = futures_util::stream::once(async move { md.clone() });
                            let deduped_stream = dedup.deduplicate_stream(stream, DedupMode::Realtime);

                            deduped_stream
                                .for_each_concurrent(None, |record| {
                                    let buffer = buffer.clone();
                                    let internal_tx = internal_tx.clone();
                                    async move {
                                        if let Err(e) = buffer.push(record.clone()).await {
                                            let _ = internal_tx.send(
                                                InternalMsg::Error(format!("Realtime buffer push failed: {}", e))
                                            ).await;
                                        }
                                    }
                                })
                                .await;
                        });
                    }
                }
            }

            info!("Realtime ingestion task stopped");
        })
    }

    /// Buffer 批量消费 Task
    fn spawn_buffer_consumer_task(&self) -> JoinHandle<()> {
        let buffer = self.buffer.clone();
        let shutdown = self.shutdown.clone();
        let internal_tx = self.internal_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = buffer.notify.notified() => {
                        let mut batch = Vec::new();
                        while let Some(item) = buffer.pop() {
                            batch.push(item);
                            if batch.len() >= buffer.batch_notify_size {
                                break;
                            }
                        }

                        if !batch.is_empty() {
                            if let Err(e) = async {
                                println!("Processing batch of size {}", batch.len());
                                Ok::<(), anyhow::Error>(())
                            }.await {
                                let _ = internal_tx.send(InternalMsg::Error(format!("Buffer processing error: {}", e))).await;
                            }
                        }
                    }
                }
            }
        })
    }

    /// 健康状态接口
    pub async fn health_status(&self) -> IngestorHealth {
        // 获取内部 ServiceState 的 clone
        let state = {
            let guard = self.state.read().await;
            guard.clone()
        };
        let backfill_pending = self.backfill.pending_tasks();
        let realtime_subscriptions = self.pipeline.subscribed_count();
        let buffer_len = self.buffer.len();

        // 增加 Deduplicator 缓存统计
        let dedup_historical = self.dedup.historical_size();
        let dedup_realtime = self.dedup.realtime_size();

        IngestorHealth {
            state,
            backfill_pending,
            realtime_subscriptions,
            buffer_len,
            dedup_historical,
            dedup_realtime,
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

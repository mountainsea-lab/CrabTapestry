use crate::domain::model::ohlcv_record::to_new_records_with_hash;
use crate::domain::service::save_ohlcv_records_batch;
use crate::ingestor::buffer::data_buffer::{CapacityStrategy, DataBuffer};
use crate::ingestor::ctrservice::{ControlMsg, InternalMsg, ServiceParams, ServiceState};
use crate::ingestor::dedup::Deduplicatable;
use crate::ingestor::dedup::deduplicator::{DedupMode, Deduplicator};
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crate::ingestor::scheduler::service::historical_backfill_service::HistoricalBackfillService;
use crate::ingestor::scheduler::{BackfillDataType, HistoricalBatchEnum};
use crate::ingestor::types::{OHLCVRecord, TickRecord, TradeRecord};
use crate::load_subscriptions;
use crab_infras::config::sub_config::{Subscription, SubscriptionMap};
use ms_tracing::tracing_utils::internal::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// IngestorService 核心结构
pub struct IngestorService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 数据订阅
    pub subscriptions: SubscriptionMap,

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
    pub backfill: Arc<HistoricalBackfillService<F>>,

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
        backfill: Arc<HistoricalBackfillService<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
    ) -> Self {
        let subscriptions: SubscriptionMap = Arc::new(Default::default());
        let (control_tx, control_rx) = mpsc::channel(1024);
        let (internal_tx, internal_rx) = mpsc::channel(64);

        let dedup_ohlcv = Arc::new(Deduplicator::<OHLCVRecord>::new(60_000));
        let dedup_tick = Arc::new(Deduplicator::<TickRecord>::new(60_000));
        let dedup_trade = Arc::new(Deduplicator::<TradeRecord>::new(60_000));

        let buffer_ohlcv = DataBuffer::new(Some(1000), 10, CapacityStrategy::DropOldest);
        let buffer_tick = DataBuffer::new(Some(10_000), 50, CapacityStrategy::Block);
        let buffer_trade = DataBuffer::new(Some(10_000), 50, CapacityStrategy::Block);

        Self {
            subscriptions,
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
        backfill: Arc<HistoricalBackfillService<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
        params: ServiceParams,
    ) -> Self {
        let subscriptions: SubscriptionMap = Arc::new(Default::default());

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
            subscriptions,
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
        backfill: Arc<HistoricalBackfillService<F>>,
        pipeline: Arc<MarketDataPipeline>,
        shutdown: Arc<Notify>,
        params: ServiceParams,
    ) -> Arc<Self> {
        let service = Arc::new(Self::with_params(backfill, pipeline, shutdown, params));
        service
    }

    /// 总入口启动所有任务
    pub async fn start(self: Arc<Self>) {
        // ← 初始化订阅信息
        self.initialize_subscriptions().await;

        // 启动消费者任务
        for h in self.clone().spawn_consumer_tasks() {
            self.handles.write().await.push(h);
        }

        // 启动控制任务
        let ctrl_handle = self.clone().spawn_control_task();
        self.handles.write().await.push(ctrl_handle);

        // 启动 error 监控
        let err_handle = self.spawn_error_monitor_task();
        self.handles.write().await.push(err_handle);

        *self.state.write().await = ServiceState::Running;
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

    /// Control Task: 处理 Subscribe / Unsubscribe / HealthCheck
    pub fn spawn_control_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut rx = self.control_rx.write().await;
            while let Some(msg) = rx.recv().await {
                match msg {
                    ControlMsg::Start => {
                        // ✅ 启动 Realtime + Backfill 服务
                        self.start_market_data_pipeline().await;
                        self.start_historical_backfill_service().await;
                    }
                    ControlMsg::Stop => {
                        self.shutdown.notify_waiters();
                        break;
                    }
                    ControlMsg::HealthCheck => {
                        /* TODO: 可扩展发送状态到监控系统 暂时只做简单输出 */
                        let state = self.state.read().await;
                        let ohlcv_len = self.buffer_ohlcv.len();
                        let tick_len = self.buffer_tick.len();
                        let trade_len = self.buffer_trade.len();

                        info!(
                            "HealthCheck: state={:?}, buffer_sizes: OHLCV={}, Tick={}, Trade={}",
                            *state, ohlcv_len, tick_len, trade_len
                        );
                    }
                    ControlMsg::AddSubscriptions(subs) => self.add_subscriptions(subs).await,
                    ControlMsg::RemoveSubscriptions(keys) => self.remove_subscriptions(keys).await,
                }
            }
        })
    }

    /// 启动实时数据 MarketDataPipeline 服务
    ///
    /// - 建立与交易所的实时订阅（ws 或 api stream）
    /// - 将消息推送到内部 realtime channel
    /// - 异常时发送 InternalMsg::Error
    async fn start_market_data_pipeline(&self) {
        let pipeline = self.pipeline.clone();
        let internal_tx = self.internal_tx.clone();
        let subscriptions = self.subscriptions.clone();

        // 按交易所分组
        let mut exch_map: HashMap<String, Vec<Arc<str>>> = HashMap::new();
        let mut periods_map: HashMap<String, Vec<Arc<str>>> = HashMap::new();

        for entry in subscriptions.iter() {
            let ((exch, _sym), sub) = entry.pair();
            exch_map.entry(exch.clone()).or_default().push(sub.symbol.clone());
            periods_map.entry(exch.clone()).or_insert_with(|| sub.periods.clone());
        }

        for (exchange, symbols) in exch_map {
            let periods = periods_map.get(&exchange).cloned().unwrap_or_else(|| vec![Arc::from("1m")]); // 默认周期 1m

            if let Err(e) = pipeline.subscribe_many(Arc::from(exchange.as_str()), symbols, periods).await {
                let _ = internal_tx
                    .send(InternalMsg::Error(format!("SubscribeMany error: {}", e)))
                    .await;
            }
        }
    }

    /// 启动历史数据 HistoricalBackfillService 服务
    ///
    /// - 异常时发送 InternalMsg::Error
    async fn start_historical_backfill_service(&self) {
        let service = self.backfill.clone();
        let subscriptions = self.subscriptions.clone();
        let shutdown = self.shutdown.clone();

        // 1️⃣ 启动 worker 消费任务
        let _shutdown_tx = service.clone().start_workers(4, shutdown); // worker 数量可配置

        let shutdown1 = self.shutdown.clone();
        // 2️⃣ 启动 scheduler
        let scheduler = service.scheduler.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown1.notified() => {
                    info!("Scheduler received shutdown1, stopping");
                }
                result = scheduler.run(2) => {   // ✅ 注意这里没有 .await
                    match result {
                        Ok(_) => info!("Scheduler stopped"),
                        Err(e) => error!("Scheduler failed: {:?}", e),
                    }
                }
            }
        });
        let shutdown2 = self.shutdown.clone();
        // 3️⃣ 启动 maintain loop（缺口扫描 + 最近补齐）
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown2.notified() => {
                    info!("Backfill maintain loop received shutdown2, stopping");
                }
                _ = service.loop_maintain_tasks_notify(
                    &subscriptions,
                    BackfillDataType::OHLCV,
                    &shutdown2
                ) => {
                    info!("Backfill maintain loop exited normally");
                }
            }
        });
    }

    /// 数据消费总控：统一启动 Realtime / Backfill / BufferConsumer 三个任务
    pub fn spawn_consumer_tasks(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        // 启动实时数据消费
        handles.push(self.clone().spawn_realtime_task());

        // 启动历史数据消费
        handles.push(self.clone().spawn_backfill_task(10, 20));

        // 启动 Buffer 消费（落库 / 指标计算）
        handles.push(self.clone().spawn_buffer_consumer_task());

        handles
    }

    /// 启动实时数据消费任务（逐条直通，无 dedup）
    pub fn spawn_realtime_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut rx = self.pipeline.subscribe_receiver();

            loop {
                tokio::select! {
                    _ = self.shutdown.notified() => {
                        info!("Realtime OHLCV ingestion shutdown");
                        break;
                    }

                    maybe_bar = rx.recv() => {
                        match maybe_bar {
                            Ok(bar) => {
                                // 直接入 buffer，不去重，低延迟
                                if let Err(e) = self.buffer_ohlcv.push(bar).await {
                                    let _ = self.internal_tx.send(
                                        InternalMsg::Error(format!("Realtime buffer push failed: {}", e))
                                    ).await;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Realtime pipeline lagged, skipped {} messages", n);
                            }
                        }
                    }
                }
            }

            info!("Realtime OHLCV ingestion stopped");
        })
    }

    /// 启动历史数据消费任务
    pub fn spawn_backfill_task(self: Arc<Self>, timeout_secs: u64, log_interval: usize) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut rx = self.backfill.subscribe();
            info!("Backfill task started");
            let mut timeout_counter = 0;

            loop {
                tokio::select! {
                    _ = self.shutdown.notified() => {
                        info!("Backfill task shutdown triggered");
                        break;
                    }

                    maybe_batch = rx.recv_timeout(Duration::from_secs(timeout_secs)) => {
                        match maybe_batch {
                            Some(batch) => {
                                timeout_counter = 0;
                                // 直接入 buffer，无去重，批量推送
                                match batch {
                                    HistoricalBatchEnum::OHLCV(hb) => {
                                        if !hb.data.is_empty() {
                                            if let Err(e) = self.buffer_ohlcv.push_batch(hb.data).await {
                                                let _ = self.internal_tx.send(
                                                    InternalMsg::Error(format!("Backfill OHLCV push failed: {}", e))
                                                ).await;
                                            }
                                        }
                                    }
                                    HistoricalBatchEnum::Tick(hb) => {
                                        if !hb.data.is_empty() {
                                            if let Err(e) = self.buffer_tick.push_batch(hb.data).await {
                                                let _ = self.internal_tx.send(
                                                    InternalMsg::Error(format!("Backfill Tick push failed: {}", e))
                                                ).await;
                                            }
                                        }
                                    }
                                    HistoricalBatchEnum::Trade(hb) => {
                                        if !hb.data.is_empty() {
                                            if let Err(e) = self.buffer_trade.push_batch(hb.data).await {
                                                let _ = self.internal_tx.send(
                                                    InternalMsg::Error(format!("Backfill Trade push failed: {}", e))
                                                ).await;
                                            }
                                        }
                                    }
                                }
                            }
                            None => {
                                timeout_counter += 1;
                                if timeout_counter % log_interval == 0 {
                                    debug!("Backfill recv_timeout {}s reached {} times", timeout_secs, timeout_counter);
                                }
                            }
                        }
                    }
                }
            }

            info!("Backfill task stopped");
        })
    }

    /// 启动 Buffer Consumer 任务（落库 + 指标计算）
    pub fn spawn_buffer_consumer_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Buffer Consumer task started");
            const FLUSH_INTERVAL: Duration = Duration::from_millis(500); // 时间窗口

            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);

            loop {
                tokio::select! {
                    // 关闭信号
                    _ = self.shutdown.notified() => {
                        info!("Buffer Consumer shutdown triggered");

                        // flush 剩余数据
                        let batch = self.buffer_ohlcv.pop_batch().await;
                        if !batch.is_empty() {
                            if let Err(e) = self.handle_batch(batch).await {
                                error!("Final flush failed: {}", e);
                            }
                        }
                        break;
                    }

                    // 时间窗口 flush
                    _ = ticker.tick() => {
                        let batch = self.buffer_ohlcv.pop_batch().await;
                        if !batch.is_empty() {
                            if let Err(e) = self.handle_batch(batch).await {
                                error!("Timed flush failed: {}", e);
                            }
                        }
                    }
                }
            }

            info!("Buffer Consumer task stopped");
        })
    }

    /// 批量处理函数：去重 + 落库 + 指标计算
    async fn handle_batch(&self, batch: Vec<Arc<OHLCVRecord>>) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let start = Instant::now();

        // 去重，deduplicate 需要改成支持 Arc<T>
        let deduped = self.dedup_ohlcv.deduplicate_arc(batch, DedupMode::Unified);
        if deduped.is_empty() {
            return Ok(());
        }

        // 打印每条记录用于验证
        for record in &deduped {
            let r = record.as_ref();
            info!(
                "OHLCVRecord - symbol: {}, period: {}, timestamp: {}, open: {}, high: {}, low: {}, close: {}, volume: {}",
                r.symbol,
                r.period,
                r.timestamp(),
                r.open,
                r.high,
                r.low,
                r.close,
                r.volume
            );
        }

        let duration = start.elapsed();
        self.buffer_ohlcv.metrics.record_batch(deduped.len(), duration, deduped.len());

        // ✅ 落库
        Self::save_with_log(&deduped).await?;

        Ok(())
    }

    /// 落库 + 日志逻辑封装
    async fn save_with_log(batch: &[Arc<OHLCVRecord>]) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // 转换为插入模型
        let new_records = to_new_records_with_hash(batch).await;

        // 落库并记录日志
        match save_ohlcv_records_batch(&new_records).await {
            Ok(_) => {
                info!(
                    "✅ Saved {} OHLCV records. first_ts={:?}, last_ts={:?}, symbol={}",
                    new_records.len(),
                    new_records.first().map(|r| r.ts),
                    new_records.last().map(|r| r.ts),
                    new_records.first().map(|r| r.symbol.clone()).unwrap_or_default(),
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "❌ Failed to save {} OHLCV records. first_record={:?}, error={}",
                    new_records.len(),
                    new_records.first(),
                    e
                );
                Err(e.into())
            }
        }
    }

    /// 异步初始化订阅配置（仅加载到服务状态，不启动任务）
    pub async fn initialize_subscriptions(self: &Arc<Self>) {
        match load_subscriptions() {
            Ok(subscriptions) => {
                info!("Loaded {} subscriptions", subscriptions.len());

                // 清空并批量插入
                self.subscriptions.clear();
                // 遍历并克隆元素插入 DashMap
                for r in subscriptions.iter() {
                    let key = r.key().clone();
                    let sub = r.value().clone();
                    self.subscriptions.insert(key, sub);
                }

                // self.notify_subscriptions_changed().await;
            }
            Err(e) => error!("Failed to load subscriptions: {}", e),
        }
    }

    /// 动态新增订阅
    pub async fn add_subscriptions(&self, subs: Vec<Subscription>) {
        for sub in subs {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            self.subscriptions.insert(key, sub);
        }
        self.notify_subscriptions_changed().await;
    }

    /// 动态移除订阅
    pub async fn remove_subscriptions(&self, keys: Vec<(Arc<str>, Arc<str>)>) {
        for key in keys {
            let key = (key.0.to_string(), key.1.to_string());
            self.subscriptions.remove(&key);
        }
        self.notify_subscriptions_changed().await;
    }

    /// 统一通知逻辑 通知订阅配置已更新
    pub async fn notify_subscriptions_changed(&self) {
        todo!("通知订阅配置已更新");
        // 这里可以触发 Backfill 或 Pipeline 的 notify
        // self.backfill.notify.notify_waiters();
        // self.pipeline.notify.notify_waiters();
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

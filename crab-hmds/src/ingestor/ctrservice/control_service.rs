use crate::ingestor::buffer::data_buffer::{CapacityStrategy, DataBuffer};
use crate::ingestor::ctrservice::{
    BufferStats, ControlMsg, DedupStats, IngestorHealth, InternalMsg, MarketDataType, ServiceParams,
    ServiceState,
};
use crate::ingestor::dedup::Deduplicatable;
use crate::ingestor::dedup::deduplicator::{DedupMode, Deduplicator};
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::realtime::market_data_pipe_line::MarketDataPipeline;
use crate::ingestor::scheduler::HistoricalBatchEnum;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::types::{OHLCVRecord, TickRecord, TradeRecord};
use ms_tracing::tracing_utils::internal::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use crab_infras::config::sub_config::{load_subscriptions_map, Subscription, SubscriptionMap};
use crate::ingestor::scheduler::service::historical_backfill_service::HistoricalBackfillService;

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

        let subscriptions: SubscriptionMap = load_subscriptions_map("subscriptions.yaml")
            .unwrap_or_else(|err| {
                warn!("Failed to load subscriptions file: {}. Using empty default.", err);
                Arc::new(Default::default())
            });
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

        let subscriptions: SubscriptionMap = load_subscriptions_map("subscriptions.yaml")
            .unwrap_or_else(|err| {
                warn!("Failed to load subscriptions file: {}. Using empty default.", err);
                Arc::new(Default::default())
            });

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
        // service.initialize_subscriptions(config).await;
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

    /// Control Task: 处理 Subscribe / Unsubscribe / HealthCheck
    pub fn spawn_control_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut rx = self.control_rx.write().await;
            while let Some(msg) = rx.recv().await {
                match msg {
                    ControlMsg::Start => { /* 启动 worker/backfill */ },
                    ControlMsg::Stop => { self.shutdown.notify_waiters(); break; },
                    ControlMsg::HealthCheck => { /* todo */ },
                    ControlMsg::AddSubscriptions(subs) => self.add_subscriptions(subs).await,
                    ControlMsg::RemoveSubscriptions(keys) => self.remove_subscriptions(keys).await,
                }
            }
        })
    }

    /// 动态新增订阅
    pub async fn add_subscriptions(&self, subs: Vec<Subscription>) {
        for sub in subs {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            self.subscriptions.insert(key, sub);
        }
        self.notify.notify_waiters();
    }

    /// 动态移除订阅
    pub async fn remove_subscriptions(&self, keys: Vec<(Arc<str>, Arc<str>)>) {
        for (exch, sym) in keys {
            let key = (exch.to_string(), sym.to_string());
            self.subscriptions.remove(&key);
        }
        self.backfill.notify.notify_waiters();
    }

    /// 启动实时订阅 task
    pub fn spawn_realtime_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // todo: 遍历 subscriptions，拉取实时数据
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    /// 启动历史回溯 task
    pub fn spawn_backfill_task(self: Arc<Self>) -> JoinHandle<()> {
        let svc = self.clone();
        tokio::spawn(async move {
            loop {
                svc.backfill.start_workers(4).await;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        })
    }

    /// 启动数据落库任务（处理缓冲区）
    pub fn spawn_buffer_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // todo: flush buffer -> DB
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
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

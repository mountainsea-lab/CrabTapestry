use crate::data::market_trade_data::StEmaData;
use crate::global;
use barter::engine::EngineOutput;
use barter::engine::audit::state_replica::StateReplicaManager;
use barter::engine::audit::{AuditTick, EngineAudit};
use barter::engine::clock::LiveClock;
use barter::engine::execution_tx::MultiExchangeTxMap;
use barter::engine::state::global::DefaultGlobalData;
use barter::error::BarterError;
use barter::risk::DefaultRiskManager;
use barter::strategy::DefaultStrategy;
use barter::system::builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder};
use barter::{
    EngineEvent,
    engine::{
        Engine,
        state::{EngineState, instrument::filter::InstrumentFilter, trading::TradingState},
    },
    system::System,
};
use barter_data::streams::builder::dynamic::indexed::init_indexed_multi_exchange_market_stream;
use barter_data::subscription::SubKind;
use barter_execution::order::request::OrderRequestOpen;
use barter_instrument::index::IndexedInstruments;
use barter_integration::{channel::UnboundedRx, collection::one_or_many::OneOrMany, snapshot::SnapUpdates};
use chrono::{DateTime, Utc};
use ms_tracing::tracing_utils::internal::{error, info};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};

const RISK_FREE_RETURN: Decimal = dec!(0.05);

// 定义具体的引擎类型
pub type DefaultEngine = Engine<
    LiveClock,
    EngineState<DefaultGlobalData, StEmaData>,
    MultiExchangeTxMap,
    DefaultStrategy<EngineState<DefaultGlobalData, StEmaData>>,
    DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub trading_enabled: bool,
    pub active_orders: usize,
    pub positions_open: usize,
    pub last_updated: DateTime<Utc>,
    pub uptime: std::time::Duration,
}

impl Default for SystemStatus {
    fn default() -> Self {
        Self {
            trading_enabled: false,
            active_orders: 0,
            positions_open: 0,
            last_updated: Utc::now(),
            uptime: std::time::Duration::default(),
        }
    }
}

#[derive(Debug)]
pub enum SystemCommand {
    EnableTrading,
    DisableTrading,
    PlaceOrders(Vec<OrderRequestOpen>),
    CancelAllOrders,
    CloseAllPositions,
    Shutdown,
    RequestStateSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum TradingError {
    #[error("System not available")]
    SystemNotAvailable,
    #[error("Command channel closed")]
    CommandChannelClosed,
    #[error("System shutdown failed: {0}")]
    ShutdownFailed(String),
    #[error("System already shutdown")]
    AlreadyShutdown,
    #[error("State replica not initialized")]
    StateReplicaNotInitialized,
    #[error("State replica error: {0}")]
    StateReplicaError(String),
    #[error("Audit stream not available")]
    AuditStreamNotAvailable,
    #[error("Failed to take audit stream: {0}")]
    AuditStreamTakeFailed(String),
}

// 类型别名简化复杂类型
type CrabStateReplica = StateReplicaManager<
    EngineState<DefaultGlobalData, StEmaData>,
    UnboundedRx<AuditTick<EngineAudit<EngineEvent, EngineOutput<(), ()>>>>,
>;

// 使用具体类型而不是泛型
pub struct CrabTrader {
    /// 核心交易系统实例
    system: Arc<Mutex<Option<System<DefaultEngine, EngineEvent>>>>,

    /// 系统状态
    status: Arc<Mutex<SystemStatus>>,

    /// 命令队列
    command_tx: mpsc::Sender<SystemCommand>,

    /// 启动时间
    startup_time: DateTime<Utc>,

    /// 状态副本管理器（使用 Barter-rs 官方实现）
    state_replica: Arc<RwLock<Option<CrabStateReplica>>>,

    /// 状态副本管理器运行状态
    replica_running: Arc<Mutex<bool>>,
}

impl CrabTrader {
    pub async fn new(mut system: System<DefaultEngine, EngineEvent>) -> Result<Self, TradingError> {
        let (command_tx, command_rx) = mpsc::channel(100);

        // 获取审计流并初始化状态副本管理器
        let state_replica = Self::init_state_replica(&mut system).await?;

        let service = Self {
            system: Arc::new(Mutex::new(Some(system))),
            status: Arc::new(Mutex::new(SystemStatus::default())),
            command_tx,
            startup_time: Utc::now(),
            state_replica: Arc::new(RwLock::new(Some(state_replica))),
            replica_running: Arc::new(Mutex::new(false)),
        };

        // 启动命令处理循环
        service.spawn_command_handler(command_rx);

        // 启动状态副本管理器
        service.start_state_replica().await?;

        Ok(service)
    }

    /// 初始化状态副本管理器 - 使用 System 的审计流
    async fn init_state_replica(
        system: &mut System<DefaultEngine, EngineEvent>,
    ) -> Result<CrabStateReplica, TradingError> {
        // Take ownership of the Engine audit snapshot with updates
        let SnapUpdates {
            snapshot: audit_snapshot,
            updates: audit_updates,
        } = system.audit.take().unwrap();

        // Construct StateReplicaManager w/ initial EngineState
        let state_replica_manager = StateReplicaManager::new(audit_snapshot, audit_updates);

        Ok(state_replica_manager)
    }

    /// 获取当前状态副本（克隆版）
    pub async fn get_current_state(&self) -> Result<EngineState<DefaultGlobalData, StEmaData>, TradingError> {
        // 异步获取读锁
        let replica_guard = self.state_replica.read().await;

        // 检查状态副本是否已初始化
        let replica = replica_guard.as_ref().ok_or(TradingError::StateReplicaNotInitialized)?;

        // 获取并克隆 EngineState
        Ok(replica.replica_engine_state().clone())
    }

    /// 安全访问状态（通过闭包）——避免在 async fn 中返回对局部 guard 的引用
    ///
    /// 用法：
    /// let result = trader.with_state(|state| {
    ///     // 在这里执行只读操作（可以 clone 子字段）
    ///     state.trading.clone()
    /// }).await?;
    pub async fn with_state<F, R>(&self, f: F) -> Result<R, TradingError>
    where
        F: FnOnce(&EngineState<DefaultGlobalData, StEmaData>) -> R,
    {
        let replica_guard = self.state_replica.read().await;
        let replica = replica_guard.as_ref().ok_or(TradingError::StateReplicaNotInitialized)?;
        let engine_state = replica.replica_engine_state();
        Ok(f(engine_state))
    }

    /// 从已构建的系统创建 CrabTrader
    pub async fn create() -> Result<Self, BarterError> {
        let system = Self::build_system().await?;
        Ok(Self::new(system)
            .await
            .map_err(|e| BarterError::ExecutionBuilder(e.to_string()))?)
    }

    /// 构建交易系统（启用审计模式）
    pub async fn build_system() -> Result<System<DefaultEngine, EngineEvent>, BarterError> {
        let strategy_config = global::get_strategy_config().get();
        let instruments = strategy_config.system_config.instruments.clone();
        let executions = strategy_config.system_config.executions.clone();

        // Construct IndexedInstruments
        let instruments = IndexedInstruments::new(instruments);

        // Initialise MarketData Stream
        let market_stream = init_indexed_multi_exchange_market_stream(&instruments, &[SubKind::PublicTrades]).await?;

        // Construct System Args
        let args = SystemArgs::new(
            &instruments,
            executions,
            LiveClock,
            DefaultStrategy::default(),
            DefaultRiskManager::default(),
            market_stream,
            DefaultGlobalData::default(),
            |_| StEmaData::default(),
        );

        // Construct SystemBuild - 确保启用审计模式
        let system = SystemBuilder::new(args)
            .engine_feed_mode(EngineFeedMode::Iterator)
            .audit_mode(AuditMode::Enabled) // 关键：启用审计
            .trading_state(TradingState::Disabled)
            .build::<EngineEvent, _>()?
            .init_with_runtime(tokio::runtime::Handle::current())
            .await?;

        Ok(system)
    }

    fn spawn_command_handler(&self, mut command_rx: mpsc::Receiver<SystemCommand>) {
        let system = Arc::clone(&self.system);
        let status = Arc::clone(&self.status);
        let startup_time = self.startup_time;

        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                if let Err(e) = Self::process_command(&system, &status, command).await {
                    eprintln!("Command processing error: {}", e);
                }

                // 在命令处理后更新状态时间戳
                let mut status_guard = status.lock().await;
                status_guard.last_updated = Utc::now();
                status_guard.uptime = Utc::now().signed_duration_since(startup_time).to_std().unwrap_or_default();
            }
        });
    }

    async fn process_command(
        system: &Arc<Mutex<Option<System<DefaultEngine, EngineEvent>>>>,
        status: &Arc<Mutex<SystemStatus>>,
        command: SystemCommand,
    ) -> Result<(), TradingError> {
        let system_guard = system.lock().await;
        let Some(system) = system_guard.as_ref() else {
            return Err(TradingError::SystemNotAvailable);
        };

        match command {
            SystemCommand::EnableTrading => {
                system.trading_state(TradingState::Enabled);
                let mut status_guard = status.lock().await;
                status_guard.trading_enabled = true;
            }
            SystemCommand::DisableTrading => {
                system.trading_state(TradingState::Disabled);
                let mut status_guard = status.lock().await;
                status_guard.trading_enabled = false;
            }
            SystemCommand::PlaceOrders(requests) => {
                system.send_open_requests(OneOrMany::Many(requests));
            }
            SystemCommand::CancelAllOrders => {
                system.cancel_orders(InstrumentFilter::None);
            }
            SystemCommand::CloseAllPositions => {
                system.close_positions(InstrumentFilter::None);
            }
            SystemCommand::Shutdown => {
                // 关闭逻辑在单独的 shutdown 方法中处理
            }
            SystemCommand::RequestStateSnapshot => {
                // 状态快照现在通过状态副本管理器获取
            }
        }

        Ok(())
    }

    /// 启动状态副本管理器（修正版）
    ///
    /// 说明：
    /// - 我们先用 `write().await` 将 `Option<CrabStateReplica>` 的所有权取出（`take()`），
    ///   然后把拥有的 `replica` 移入 `spawn_blocking` 执行 `replica.run()`（假设 run 是阻塞操作）。
    /// - run 返回后，将 `replica` 放回 `state_replica`。
    /// - 这样就不会在阻塞线程中持有 tokio 的 guard，也不会把 guard 传给阻塞线程。
    pub async fn start_state_replica(&self) -> Result<(), TradingError> {
        let mut running_guard = self.replica_running.lock().await;
        if *running_guard {
            return Ok(());
        }
        *running_guard = true;
        drop(running_guard);

        let state_replica = Arc::clone(&self.state_replica);
        let replica_running = Arc::clone(&self.replica_running);

        tokio::spawn(async move {
            info!("Starting state replica manager");

            // 1) 从 RwLock 中 take 出 Option<Replica> 的所有权
            let replica_opt = {
                let mut write_guard = state_replica.write().await;
                write_guard.take()
            };

            if let Some(mut replica) = replica_opt {
                // 2) 将拥有的 replica 移入阻塞线程执行 run()
                //    这里 spawn_blocking 返回的是 Result<run_result, JoinError>
                let run_res = tokio::task::spawn_blocking(move || {
                    // run() 是阻塞方法（假设签名为 `fn run(&mut self) -> Result<(), String>` 或类似）
                    // 保持 mutable ownership，run 完后我们还拥有 replica，可以返回它
                    let r = replica.run();
                    (r, replica) // 将结果和 replica 一并返回
                })
                .await;

                match run_res {
                    Ok((Ok(()), returned_replica)) => {
                        info!("State replica manager stopped gracefully");
                        // 把 replica 放回 state_replica
                        let mut guard = state_replica.write().await;
                        *guard = Some(returned_replica);
                    }
                    Ok((Err(e), returned_replica)) => {
                        error!("State replica manager error: {}", e);
                        // 仍将 replica 放回，以便后续重启或检查
                        let mut guard = state_replica.write().await;
                        *guard = Some(returned_replica);
                    }
                    Err(join_err) => {
                        error!("State replica manager task join error: {}", join_err);
                        // 无法获得 replica（理论上不会发生，因为我们 move 了 replica 进闭包并在返回时带回）
                        // 为安全起见，置 None（或记录日志）
                        let mut guard = state_replica.write().await;
                        *guard = None;
                    }
                }
            } else {
                error!("State replica not initialized when starting replica manager");
            }

            // 重置运行状态
            let mut running_guard = replica_running.lock().await;
            *running_guard = false;
            info!("State replica manager run finished");
        });

        Ok(())
    }

    /// 启用交易
    pub async fn enable_trading(&self) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::EnableTrading)
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 禁用交易
    pub async fn disable_trading(&self) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::DisableTrading)
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 发送开仓订单
    pub async fn place_orders(&self, requests: Vec<OrderRequestOpen>) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::PlaceOrders(requests))
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 取消所有订单
    pub async fn cancel_all_orders(&self) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::CancelAllOrders)
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 平仓所有头寸
    pub async fn close_all_positions(&self) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::CloseAllPositions)
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 请求状态快照
    pub async fn request_state_snapshot(&self) -> Result<(), TradingError> {
        self.command_tx
            .send(SystemCommand::RequestStateSnapshot)
            .await
            .map_err(|_| TradingError::CommandChannelClosed)
    }

    /// 获取系统状态
    pub async fn get_status(&self) -> Result<SystemStatus, TradingError> {
        let status_guard = self.status.lock().await;
        let mut status = status_guard.clone();

        // 更新实时信息
        status.uptime = Utc::now().signed_duration_since(self.startup_time).to_std().unwrap_or_default();

        Ok(status)
    }

    /// 优雅关闭
    pub async fn shutdown(self) -> Result<(), TradingError> {
        // 1. 发送关闭命令
        let _ = self.command_tx.send(SystemCommand::Shutdown).await;

        // 2. 等待命令处理完成
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 3. 关闭系统
        let mut system_guard = self.system.lock().await;
        if let Some(system) = system_guard.take() {
            system
                .shutdown()
                .await
                .map_err(|e| TradingError::ShutdownFailed(e.to_string()))?;
            Ok(())
        } else {
            Err(TradingError::AlreadyShutdown)
        }
    }
}

// 为 CrabTrader 实现 Clone
impl Clone for CrabTrader {
    fn clone(&self) -> Self {
        Self {
            system: Arc::clone(&self.system),
            status: Arc::clone(&self.status),
            command_tx: self.command_tx.clone(),
            startup_time: self.startup_time,
            state_replica: Arc::clone(&self.state_replica),
            replica_running: Arc::clone(&self.replica_running),
        }
    }
}

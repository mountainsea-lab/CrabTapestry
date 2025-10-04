use crate::data::st_ema_data::StEmaData;
use barter::engine::clock::LiveClock;
use barter::engine::execution_tx::MultiExchangeTxMap;
use barter::engine::state::global::DefaultGlobalData;
use barter::error::BarterError;
use barter::risk::DefaultRiskManager;
use barter::strategy::DefaultStrategy;
use barter::system::builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder};
use barter::system::config::SystemConfig;
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
use barter_integration::collection::one_or_many::OneOrMany;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

const FILE_PATH_SYSTEM_CONFIG: &str = "crab-strategy/config/system_config.json";

// Risk-free rate of 5% (configure as needed)
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
}

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
}

impl CrabTrader {
    pub fn new(system: System<DefaultEngine, EngineEvent>) -> Self {
        let (command_tx, command_rx) = mpsc::channel(100);

        let service = Self {
            system: Arc::new(Mutex::new(Some(system))),
            status: Arc::new(Mutex::new(SystemStatus::default())),
            command_tx,
            startup_time: Utc::now(),
        };

        // 启动命令处理循环
        service.spawn_command_handler(command_rx);

        service
    }

    /// 从已构建的系统创建 CrabTrader
    pub async fn create() -> Result<Self, BarterError> {
        let system = Self::build_system().await?;
        Ok(Self::new(system))
    }

    /// 构建交易系统
    pub async fn build_system() -> Result<System<DefaultEngine, EngineEvent>, BarterError> {
        // Load SystemConfig
        let SystemConfig { instruments, executions } =
            load_config().map_err(|e| BarterError::ExecutionBuilder(format!("Config error: {}", e)))?;

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

        // Construct SystemBuild
        let system = SystemBuilder::new(args)
            .engine_feed_mode(EngineFeedMode::Iterator)
            .audit_mode(AuditMode::Enabled)
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
                // 直接更新状态
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
        }

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

impl Clone for CrabTrader {
    fn clone(&self) -> Self {
        Self {
            system: Arc::clone(&self.system),
            status: Arc::clone(&self.status),
            command_tx: self.command_tx.clone(),
            startup_time: self.startup_time,
        }
    }
}

/// 独立的系统构建函数
pub async fn build_trading_system() -> Result<System<DefaultEngine, EngineEvent>, BarterError> {
    // Load SystemConfig
    let SystemConfig { instruments, executions } =
        load_config().map_err(|e| BarterError::ExecutionBuilder(format!("Config error: {}", e)))?;

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

    // Construct SystemBuild
    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Iterator)
        .audit_mode(AuditMode::Enabled)
        .trading_state(TradingState::Disabled)
        .build::<EngineEvent, _>()?
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    Ok(system)
}

pub fn load_config() -> Result<SystemConfig, Box<dyn std::error::Error>> {
    let file = File::open(FILE_PATH_SYSTEM_CONFIG)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}

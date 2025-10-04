use barter::{
    engine::{
        Engine, Processor,
        audit::{Auditor, context::EngineContext},
        command::Command,
        state::{instrument::filter::InstrumentFilter, trading::TradingState},
    },
    shutdown::Shutdown,
    system::System,
};
use barter_execution::order::request::OrderRequestOpen;
use barter_integration::collection::one_or_many::OneOrMany;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

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

pub struct CrabTrader<Eng, Evt>
where
    Eng: Processor<Evt> + Auditor<Eng::Audit, Context = EngineContext> + Send + 'static,
    Eng::Audit: Send + 'static,
    Eng::Snapshot: Send + 'static, // 添加 Snapshot Send 约束
    Evt: Debug + Clone + Send + From<Command> + From<TradingState> + 'static,
{
    /// 核心交易系统实例
    system: Arc<Mutex<Option<System<Eng, Evt>>>>,

    /// 系统状态
    status: Arc<Mutex<SystemStatus>>,

    /// 命令队列
    command_tx: mpsc::Sender<SystemCommand>,

    /// 启动时间
    startup_time: DateTime<Utc>,
}

impl<Eng, Evt> CrabTrader<Eng, Evt>
where
    Eng: Processor<Evt> + Auditor<Eng::Audit, Context = EngineContext> + Send + 'static,
    Eng::Audit: Send + 'static,
    Eng::Snapshot: Send + 'static, // 添加 Snapshot Send 约束
    Evt: Debug + Clone + Send + From<Command> + From<TradingState> + From<Shutdown> + 'static,
{
    pub fn new(system: System<Eng, Evt>) -> Self {
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
        system: &Arc<Mutex<Option<System<Eng, Evt>>>>,
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

impl<Eng, Evt> Clone for CrabTrader<Eng, Evt>
where
    Eng: Processor<Evt> + Auditor<Eng::Audit, Context = EngineContext> + Send + 'static,
    Eng::Audit: Send + 'static,
    Eng::Snapshot: Send + 'static, // 添加 Snapshot Send 约束
    Evt: Debug + Clone + Send + From<Command> + From<TradingState> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            system: Arc::clone(&self.system),
            status: Arc::clone(&self.status),
            command_tx: self.command_tx.clone(),
            startup_time: self.startup_time,
        }
    }
}

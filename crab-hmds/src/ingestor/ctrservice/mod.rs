use std::sync::Arc;
use crab_infras::config::sub_config::Subscription;

pub mod control_service;

/// 服务状态 用于控制和监控
#[derive(Debug, Clone)]
pub enum ServiceState {
    Initialized,
    Running,
    Stopped,
    Error(String), // 附带错误原因
}

/// 控制消息用于统一管理子服务的启动/停止/错误传播。
pub enum ControlMsg {
    Start,
    Stop,
    HealthCheck,
    AddSubscriptions(Vec<Subscription>),
    RemoveSubscriptions(Vec<(Arc<str>, Arc<str>)>), // exchange, symbol
}


/// 内部消息，用于任务错误或信息上报
#[derive(Debug)]
pub enum InternalMsg {
    Error(String),
    Info(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MarketDataType {
    OHLCV,
    Tick,
    Trade,
}

#[derive(Debug, Clone)]
pub struct DedupStats {
    pub historical: usize,
    pub realtime: usize,
}

#[derive(Debug, Clone)]
pub struct BufferStats {
    pub len: usize,
}

/// 服务健康状态
#[derive(Debug, Clone)]
pub struct IngestorHealth {
    pub state: ServiceState,
    pub backfill_pending: usize,
    pub realtime_subscriptions: usize,

    // 类型为 key 的 HashMap
    pub buffer_stats: std::collections::HashMap<MarketDataType, BufferStats>,
    pub dedup_stats: std::collections::HashMap<MarketDataType, DedupStats>,
}

/// 服务参数配置
pub struct ServiceParams {
    pub dedup_window_ms: i64,

    pub buffer_cap_ohlcv: usize,
    pub buffer_cap_tick: usize,
    pub buffer_cap_trade: usize,

    pub buffer_batch_size_ohlcv: usize,
    pub buffer_batch_size_tick: usize,
    pub buffer_batch_size_trade: usize,

    pub control_channel_size: usize,
    pub internal_channel_size: usize,
}

impl Default for ServiceParams {
    fn default() -> Self {
        Self {
            dedup_window_ms: 60_000,

            buffer_cap_ohlcv: 1000,
            buffer_cap_tick: 10_000,
            buffer_cap_trade: 10_000,

            buffer_batch_size_ohlcv: 10,
            buffer_batch_size_tick: 50,
            buffer_batch_size_trade: 50,

            control_channel_size: 1024,
            internal_channel_size: 64,
        }
    }
}

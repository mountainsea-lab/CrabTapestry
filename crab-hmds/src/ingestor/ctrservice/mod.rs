use std::sync::Arc;

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
#[derive(Debug)]
pub enum ControlMsg {
    Subscribe {
        exchange: Arc<str>,
        symbol: Arc<str>,
        periods: Vec<Arc<str>>,
    },
    Unsubscribe {
        exchange: Arc<str>,
        symbol: Arc<str>,
    },
    SubscribeMany {
        exchange: Arc<str>,
        symbols: Vec<Arc<str>>,
        periods: Vec<Arc<str>>,
    },
    UnsubscribeMany {
        exchange: Arc<str>,
        symbols: Vec<Arc<str>>,
    },
    HealthCheck,
    Start,
    Stop,
}
/// 单个交易所的订阅配置
#[derive(Debug, Clone)]
pub struct ExchangeConfig {
    pub exchange: String,     // 交易所
    pub symbols: Vec<String>, // 币种列表，例如 ["BTC/USDT", "ETH/USDT"]
    pub periods: Vec<String>, // 公共周期 ["1m", "5m", "1h"]
}

/// Ingestor 初始化配置
#[derive(Debug, Clone)]
pub struct IngestorConfig {
    pub exchanges: Vec<ExchangeConfig>,
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

use crate::ingestor::dedup::Deduplicatable;
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::types::{HistoricalBatch, OHLCVRecord};
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

/// 统一市场数据类型（历史批次 / 实时 tick）
pub enum MarketData<T> {
    Historical(HistoricalBatch<T>),
    Realtime(OHLCVRecord),
}

/// 服务健康状态
pub struct IngestorHealth {
    /// 当前服务状态（Running / Stopped / Error 等）
    pub state: ServiceState,

    /// Backfill 队列剩余任务数
    pub backfill_pending: usize,

    /// 当前实时订阅 symbol 数量
    pub realtime_subscriptions: usize,

    /// Buffer 当前长度
    pub buffer_len: usize,

    /// Deduplicator 历史数据缓存大小
    pub dedup_historical: usize,

    /// Deduplicator 实时数据缓存大小
    pub dedup_realtime: usize,
}

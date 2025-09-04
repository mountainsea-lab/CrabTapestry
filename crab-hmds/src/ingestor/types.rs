use crate::ingestor::dedup::Deduplicatable;
use chrono::Utc;
use crab_types::TimeRange;
use std::sync::Arc;

/**
4. 实践建议

Tick 是高频策略的核心数据源

实时行情、微结构策略、回测都需要 Tick

不必盲目存储所有衍生数据

高频策略可以在内存里维护衍生指标缓存

Tick + 内存计算足够大多数需求

Level2 / Quote 数据视策略决定

如果做微结构套利、流动性预测，则需要维护 OrderBook

否则仅 Tick 已足够

短周期 OHLCV 可缓存

为了策略快速访问或减少 Tick 遍历，可在内存维护 1s/1m OHLCV

这属于 Tick 的衍生缓存，不需要落库每秒 OHLCV
*/
/// 单笔成交 Tick 数据
/// Represents a single market trade tick (individual trade event).
#[derive(Debug, Clone)]
pub struct Tick {
    pub ts: i64,                  // 成交时间 UTC 时间戳
    pub symbol: Arc<str>,         // 交易对
    pub exchange: Arc<str>,       // 交易所
    pub price: f64,               // 成交价格
    pub qty: f64,                 // 成交数量
    pub side: Option<String>,     // 买/卖方向，可选
    pub order_id: Option<String>, // 原始订单号，可选
    pub tick_id: Option<u64>,     // 原始交易所序号，可选
}

/// K线数据
/// Represents a single OHLCV candlestick (K-line).
#[derive(Debug, Clone)]
pub struct OHLCV {
    pub ts: i64,                      // K线结束时间
    pub period_start_ts: Option<i64>, // K线开始时间，可选
    pub symbol: Arc<str>,
    pub exchange: Arc<str>,
    pub period: String, // K线周期 "1m", "5m", "1h"
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: Option<f64>,   // 成交额，可选
    pub num_trades: Option<u32>, // 成交笔数，可选
    pub vwap: Option<f64>,       // 成交量加权价格，可选
}
/// 注意：实时聚合组件已经处理了数据缓存这个结构可以不使用，暂时保留后续可能用到
/// 实时维护最新一条 K线
/// Maintains the latest real-time K-line in memory.
#[derive(Debug, Clone)]
pub struct RealtimeOHLCV {
    pub ts: i64,              // K线结束时间
    pub period_start_ts: i64, // K线开始时间
    pub symbol: Arc<str>,
    pub exchange: Arc<str>,
    pub period: String, // "1m", "5m" 等
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
    pub num_trades: u32,
}

/// 内存中流转的市场数据事件
/// memory-based market data event.
pub enum MarketDataEvent {
    // Trade(Trade),
    Tick(Tick),
    OHLCV(OHLCV),
}

//==========================storage strucutre===========================

/// 高频 Tick 数据库映射
/// Tick data mapping for high-frequency database storage.
#[derive(Debug, Clone)]
pub struct TickRecord {
    pub ts: i64,                  // 成交时间 UTC 时间戳
    pub symbol: Arc<str>,         // 交易对
    pub exchange: Arc<str>,       // 交易所
    pub price: f64,               // 成交价格
    pub qty: f64,                 // 成交量
    pub side: Option<String>,     // 买/卖方向，可选
    pub order_id: Option<String>, // 原始订单号，可选
    pub tick_id: Option<u64>,     // 原始交易所序号，可选
}

/// 低频 OHLCV 数据库映射
/// OHLCV data mapping for low-frequency database storage.
#[derive(Debug, Clone)]
pub struct OHLCVRecord {
    pub ts: i64,                      // K线结束时间
    pub period_start_ts: Option<i64>, // K线开始时间，可选
    pub symbol: Arc<str>,             // 交易对
    pub exchange: Arc<str>,           // 交易所
    pub period: String,               // "1m", "5m", "1h"
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: Option<f64>,   // 成交额，可选
    pub num_trades: Option<u32>, // 成交笔数，可选
    pub vwap: Option<f64>,       // 成交量加权价格，可选
}

/// 批量写入辅助
/// Helper type for batch inserts.
pub type TickBatch = Vec<TickRecord>;
pub type OHLCVBatch = Vec<OHLCVRecord>;

impl Deduplicatable for OHLCVRecord {
    fn unique_key(&self) -> Arc<str> {
        // 唯一 key = exchange + symbol + period + ts
        // unique key = exchange + symbol + period + ts
        Arc::from(format!("{}:{}:{}:{}", self.exchange, self.symbol, self.period, self.ts))
    }

    fn timestamp(&self) -> i64 {
        self.ts
    }
}

impl Deduplicatable for TickRecord {
    fn unique_key(&self) -> Arc<str> {
        // 唯一 key = exchange + symbol + ts (+ trade_id 如果有的话)
        // unique key = exchange + symbol + ts (+ trade_id if available)
        Arc::from(format!("{}:{}:{}", self.exchange, self.symbol, self.ts))
    }


    fn timestamp(&self) -> i64 {
        self.ts
    }
}

//==============HistoricalFetcher Base Model=================

/// 历史数据源信息
/// Metadata about a historical data source.
#[derive(Debug, Clone)]
pub struct HistoricalSource {
    pub name: String,         // Data source name, e.g., "Binance API"
    pub exchange: String,     // Exchange name, e.g., "binance"
    pub last_success_ts: i64, // Last successfully saved data timestamp
    pub last_fetch_ts: i64,   // Last fetch attempt timestamp
    pub batch_size: usize,    // Batch size for each fetch
    pub supports_tick: bool,  // Supports tick data
    pub supports_trade: bool, // Supports trade data
    pub supports_ohlcv: bool, // Supports OHLCV data
}

/// 拉取上下文信息
/// Context for fetching historical data.
#[derive(Debug, Clone)]
pub struct FetchContext {
    pub source: HistoricalSource,
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub period: Option<Arc<str>>, // ✅ 和 HistoricalBatch 对齐，也用 Arc<str>
    pub range: TimeRange,
}

/// 批量历史数据接口，统一 Trade 或Tick 或 OHLCV
/// Batch of historical data, unified for Trade, Tick, or OHLCV records.
#[derive(Debug)]
pub struct HistoricalBatch<T> {
    pub symbol: Arc<str>,
    pub exchange: Arc<str>,
    pub period: Option<Arc<str>>, // ✅ 改成 Arc<str>，和 FetchContext 对齐
    pub range: TimeRange,         // Covered time range
    pub data: Vec<T>,             // TickRecord, TradeRecord, or OHLCVRecord
}

impl FetchContext {
    pub fn new(source: HistoricalSource, exchange: &str, symbol: &str, period: Option<&str>, range: TimeRange) -> Self {
        Self {
            source,
            exchange: Arc::<str>::from(exchange),
            symbol: Arc::<str>::from(symbol),
            period: period.map(|p| Arc::<str>::from(p)),
            range,
        }
    }

    /// 创建 FetchContext，并生成一个安全的时间区间
    ///
    /// `past_hours` / `past_days` 可选，优先使用小时
    pub fn new_with_past(
        source: HistoricalSource,
        exchange: &str,
        symbol: &str,
        period: Option<&str>,
        past_hours: Option<i64>,
        past_days: Option<i64>,
    ) -> Arc<Self> {
        // 当前 UTC 时间（毫秒）
        let end_ts = Utc::now().timestamp_millis();

        // 计算开始时间
        let start_ts = if let Some(hours) = past_hours {
            end_ts - hours * 60 * 60 * 1000
        } else if let Some(days) = past_days {
            end_ts - days * 24 * 60 * 60 * 1000
        } else {
            end_ts - 60 * 60 * 1000 // 默认过去 1 小时
        };

        let range = TimeRange::new(start_ts, end_ts);

        Arc::new(Self {
            source,
            exchange: Arc::from(exchange),
            symbol: Arc::from(symbol),
            period: period.map(|p| Arc::from(p)),
            range,
        })
    }
}
//==============HistoricalFetcher Base Model===

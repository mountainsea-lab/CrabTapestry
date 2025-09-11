use crate::ingestor::dedup::Deduplicatable;
use barter_data::barter_instrument::Side;
use barter_data::barter_instrument::exchange::ExchangeId;
use barter_data::barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_data::event::MarketEvent;
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::PublicTrade;
use chrono::Utc;
use crab_types::TimeRange;
use std::sync::Arc;
use trade_aggregation::M1;

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

#[derive(Clone, Debug)]
pub struct PublicTradeEvent {
    pub exchange: String,
    pub symbol: String,
    pub trade: PublicTrade, // market realtime trade data
    pub timestamp: i64,     // data timestamp
    pub time_period: u64,
}

/// from barter trade data event to PublicTradeEvent
impl From<Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> for PublicTradeEvent {
    fn from(event: Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>) -> Self {
        match event {
            Event::Item(market_event) => {
                PublicTradeEvent {
                    exchange: market_event.exchange.to_string(),
                    symbol: format!("{}", market_event.instrument.base), // 根据你的需求调整
                    trade: market_event.kind,                            // PublicTrade
                    timestamp: market_event.time_exchange.timestamp_millis(), // 时间戳转换 注意毫秒单位
                    time_period: M1.get(),
                }
            }
            _ => {
                // if not PublicTrade data，return default
                PublicTradeEvent {
                    exchange: String::new(),
                    symbol: String::new(),
                    trade: PublicTrade {
                        id: String::new(),
                        price: 0.0,
                        amount: 0.0,
                        side: Side::Buy,
                    },
                    timestamp: 0,
                    time_period: M1.get(),
                }
            }
        }
    }
}

///  多类型统一封装 内存中流转的市场数据事件
/// memory-based market data event.
#[derive(Debug, Clone)]
pub enum MarketData {
    OHLCV(OHLCVRecord),
    Tick(TickRecord),
    Trade(TradeRecord),
}

impl Deduplicatable for MarketData {
    fn unique_key(&self) -> Arc<str> {
        match self {
            MarketData::OHLCV(r) => r.unique_key(),
            MarketData::Tick(r) => r.unique_key(),
            MarketData::Trade(r) => r.unique_key(),
        }
    }

    fn timestamp(&self) -> i64 {
        match self {
            MarketData::OHLCV(r) => r.timestamp(),
            MarketData::Tick(r) => r.timestamp(),
            MarketData::Trade(r) => r.timestamp(),
        }
    }
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

/// 设计占位
#[derive(Debug, Clone)]
pub struct TradeRecord {
    // TODO
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

impl Deduplicatable for TradeRecord {
    fn unique_key(&self) -> Arc<str> {
        todo!()
    }

    fn timestamp(&self) -> i64 {
        todo!()
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
#[derive(Debug, Clone)]
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

use crate::ingestor::dedup::Deduplicatable;
use crab_infras::aggregator::AggregatorOutput;
use crab_infras::aggregator::types::{PublicTradeEvent, TradeCandle};
use crab_types::TimeRange;
use std::sync::Arc;
use trade_aggregation::CandleComponent;

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

/// 拉取上下文信息
/// Context for fetching historical data.
#[derive(Debug, Clone)]
pub struct FetchContext {
    pub range_id: Option<u64>,
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub period: Option<Arc<str>>,
    pub range: TimeRange,
    pub limit: i32,
}

/// 批量历史数据接口，统一 Trade 或Tick 或 OHLCV
/// Batch of historical data, unified for Trade, Tick, or OHLCV records.
#[derive(Debug, Clone)]
pub struct HistoricalBatch<T> {
    pub range_id: Option<u64>,
    pub limit: i32,
    pub symbol: Arc<str>,
    pub exchange: Arc<str>,
    pub period: Option<Arc<str>>, // ✅ 改成 Arc<str>，和 FetchContext 对齐
    pub range: TimeRange,         // Covered time range
    pub data: Vec<T>,             // TickRecord, TradeRecord, or OHLCVRecord
}
impl FetchContext {
    pub fn new(
        range_id: Option<u64>,
        exchange: &str,
        symbol: &str,
        quote: &str,
        period: Option<&str>,
        range: TimeRange,
        limit: i32,
    ) -> Self {
        let full_symbol = format!("{}{}", symbol.to_uppercase(), quote.to_uppercase());
        Self {
            range_id,
            exchange: Arc::<str>::from(exchange),
            symbol: Arc::<str>::from(full_symbol),
            period: period.map(|p| Arc::<str>::from(p)),
            range,
            limit,
        }
    }
}
//==============HistoricalFetcher Base Model===
impl OHLCVRecord {
    // Convert PublicTradeEvent and TradeCandle into OHLCVRecord
    pub fn from_event_and_candle(event: PublicTradeEvent, candle: TradeCandle, period: &str) -> Self {
        let turnover = Some(event.trade.price * event.trade.amount); // Example turnover calculation
        let num_trades = Some(candle.num_trades.value());

        // Return the OHLCVRecord
        Self {
            ts: event.timestamp,                                // K线结束时间
            period_start_ts: Some(candle.time_range.open_time), // K线开始时间
            symbol: Arc::from(event.symbol),                    // 交易对
            exchange: Arc::from(event.exchange),                // 交易所
            period: period.to_string(),                         // 交易周期
            open: candle.open.value(),                          // 开盘价
            high: candle.high.value(),                          // 最高价
            low: candle.low.value(),                            // 最低价
            close: candle.close.value(),                        // 收盘价
            volume: candle.volume.value(),                      // 成交量
            turnover,                                           // 成交额
            num_trades,                                         // 成交笔数
            vwap: None,                                         // Optional VWAP, can be calculated later
        }
    }
}

pub struct OHLCVRecordBuilder {
    ts: i64,
    period_start_ts: Option<i64>,
    symbol: Arc<str>,
    exchange: Arc<str>,
    period: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    turnover: Option<f64>,
    num_trades: Option<u32>,
    vwap: Option<f64>,
}

impl OHLCVRecordBuilder {
    /// 初始化 Builder，从 PublicTradeEvent 和 TradeCandle 快速构造基础字段
    pub fn from_event_and_candle(
        exchange: &str,
        symbol: &str,
        period: &str,
        timestamp: i64,
        candle: TradeCandle,
    ) -> Self {
        let turnover = Some(candle.volume.value() * candle.close.value());
        let num_trades = Some(candle.num_trades.value());

        Self {
            ts: timestamp,
            period_start_ts: Some(candle.time_range.open_time),
            symbol: Arc::from(symbol),
            exchange: Arc::from(exchange),
            period: period.to_string(),
            open: candle.open.value(),
            high: candle.high.value(),
            low: candle.low.value(),
            close: candle.close.value(),
            volume: candle.volume.value(),
            turnover,
            num_trades,
            vwap: None,
        }
    }

    /// 可选设置 turnover
    pub fn turnover(mut self, turnover: f64) -> Self {
        self.turnover = Some(turnover);
        self
    }

    /// 可选设置 num_trades
    pub fn num_trades(mut self, num_trades: u32) -> Self {
        self.num_trades = Some(num_trades);
        self
    }

    /// 可选设置 VWAP
    pub fn vwap(mut self, vwap: f64) -> Self {
        self.vwap = Some(vwap);
        self
    }

    /// 构建 OHLCVRecord
    pub fn build(self) -> OHLCVRecord {
        OHLCVRecord {
            ts: self.ts,
            period_start_ts: self.period_start_ts,
            symbol: self.symbol,
            exchange: self.exchange,
            period: self.period,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            turnover: self.turnover,
            num_trades: self.num_trades,
            vwap: self.vwap,
        }
    }
}

impl AggregatorOutput for OHLCVRecord {
    fn from_trade_candle(exchange: &str, symbol: &str, period: &str, timestamp: i64, candle: TradeCandle) -> Self {
        OHLCVRecordBuilder::from_event_and_candle(exchange, symbol, period, timestamp, candle).build()
    }
}

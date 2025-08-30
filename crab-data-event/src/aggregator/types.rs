use crate::ingestion::types::PublicTradeEvent;
use barter_data::barter_instrument::Side;
use barter_data::subscription::trade::PublicTrade;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use trade_aggregation::candle_components::{Close, High, Low, NumTrades, Open, Volume};
use trade_aggregation::{CandleComponent, CandleComponentUpdate, ModularCandle, TakerTrade, Trade};

#[derive(Debug, Default, Clone)]
pub struct TradeCandle {
    pub open: Open,
    pub high: High,
    pub low: Low,
    pub close: Close,
    pub volume: Volume,
    pub num_trades: NumTrades<u32>,
    pub time_range: FastTimeRange,
}

impl<T: TakerTrade> ModularCandle<T> for TradeCandle {
    fn update(&mut self, trade: &T) {
        self.open.update(trade);
        self.high.update(trade);
        self.low.update(trade);
        self.close.update(trade);
        self.volume.update(trade);
        self.num_trades.update(trade);
        self.time_range.update(trade);
    }

    fn reset(&mut self) {
        self.open.reset();
        self.high.reset();
        self.low.reset();
        self.close.reset();
        self.volume.reset();
        self.num_trades.reset();
        self.time_range.reset();
    }
}

#[derive(Debug, Clone)]
pub struct FastTimeRange {
    pub open_time: i64,
    pub close_time: i64,
    initialized: bool,
}

impl Default for FastTimeRange {
    #[inline(always)]
    fn default() -> Self {
        Self {
            open_time: 0,
            close_time: 0,
            initialized: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimeRangeValue {
    pub open: i64,
    pub close: i64,
}

impl TimeRangeValue {
    #[inline(always)]
    pub fn duration(&self) -> i64 {
        self.close - self.open
    }
}

impl CandleComponent<TimeRangeValue> for FastTimeRange {
    #[inline(always)]
    fn value(&self) -> TimeRangeValue {
        TimeRangeValue {
            open: self.open_time,
            close: self.close_time,
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        *self = Self::default();
    }
}

impl<T: TakerTrade> CandleComponentUpdate<T> for FastTimeRange {
    #[inline(always)]
    fn update(&mut self, trade: &T) {
        let ts = trade.timestamp();
        if !self.initialized {
            self.open_time = ts;
            self.initialized = true;
        }
        self.close_time = ts;
    }
}

/// Trait 用于判断 trade 是否有效
/// valid trade
pub trait ValidatableTrade {
    fn is_valid(&self) -> bool;
}

impl ValidatableTrade for PublicTrade {
    fn is_valid(&self) -> bool {
        self.price > 0.0 && self.price.is_finite() && self.amount > 0.0
    }
}

impl From<&PublicTradeEvent> for Trade {
    fn from(event: &PublicTradeEvent) -> Self {
        let PublicTrade { price, amount, side, .. } = event.trade;

        Trade {
            timestamp: event.timestamp,
            price,
            size: match side {
                Side::Buy => amount,
                Side::Sell => -amount,
            },
        }
    }
}

/// BaseBar 结构体 - 对应 ta4r 的 BaseBar 类
/// BaseBar struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseBar {
    /// 时间周期（例如 1 天、15 分钟等）
    pub time_period: i64,
    /// Bar 周期的开始时间（UTC, ISO8601 字符串）
    pub begin_time: OffsetDateTime,
    /// Bar 周期的结束时间（UTC, ISO8601 字符串）
    pub end_time: OffsetDateTime,
    /// OHLC
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    /// 总交易量
    pub volume: f64,
    /// 总交易金额
    pub amount: f64,
    /// 交易次数
    pub trades: u64,
}

/// convert TradeCandle to BaseBar
impl From<TradeCandle> for BaseBar {
    fn from(c: TradeCandle) -> Self {
        let begin_time =
            OffsetDateTime::from_unix_timestamp(c.time_range.open_time).unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let end_time =
            OffsetDateTime::from_unix_timestamp(c.time_range.close_time).unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let time_period = c.time_range.close_time - c.time_range.open_time;

        let close_price = c.close.value();
        let volume = c.volume.value();
        let amount = close_price * volume;

        BaseBar {
            time_period,
            begin_time,
            end_time,
            open_price: c.open.value(),
            high_price: c.high.value(),
            low_price: c.low.value(),
            close_price,
            volume,
            amount,
            trades: c.num_trades.value() as u64,
        }
    }
}

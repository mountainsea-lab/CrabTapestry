use std::sync::Arc;
use barter_data::barter_instrument::Side;
use barter_data::subscription::trade::PublicTrade;
use crab_common_utils::time_utils::milliseconds_to_offsetdatetime;
use crab_infras::cache::BaseBar;
use serde::{Deserialize, Serialize};
use trade_aggregation::candle_components::{Close, High, Low, NumTrades, Open, Volume};
use trade_aggregation::{CandleComponent, CandleComponentUpdate, M1, ModularCandle, TakerTrade, Trade};
use crate::ingestor::types::{OHLCVRecord, PublicTradeEvent};

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

/// convert TradeCandle to BaseBar
impl From<TradeCandle> for BaseBar {
    fn from(c: TradeCandle) -> Self {
        let begin_time = milliseconds_to_offsetdatetime(c.time_range.open_time);

        let end_time = milliseconds_to_offsetdatetime(c.time_range.close_time);

        let time_period = M1.get() as i64;

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

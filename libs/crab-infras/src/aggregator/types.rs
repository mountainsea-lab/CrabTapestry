use crate::aggregator::AggregatorOutput;
use crate::cache::BaseBar;
use barter_data::event::{DataKind, MarketEvent};
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::PublicTrade;
use barter_instrument::Side;
use barter_instrument::exchange::ExchangeId;
use barter_instrument::instrument::market_data::MarketDataInstrument;
use crab_common_utils::time_utils::{milliseconds_to_offsetdatetime, parse_period_to_secs};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trade_aggregation::candle_components::{Close, High, Low, NumTrades, Open, Volume};
use trade_aggregation::{CandleComponent, CandleComponentUpdate, M1, ModularCandle, TakerTrade, Trade};

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
        self.price > 0.0
            && self.price.is_finite()
            && self.amount > 0.0
            && self.amount.is_finite()
            && matches!(self.side, Side::Buy | Side::Sell)
    }
}

/// The Trade data event wrapper at the time of transaction is used to contain public transaction data and additional information (such as timestamp)
#[derive(Clone, Debug)]
pub struct PublicTradeEvent {
    pub exchange: String,
    pub symbol: String,
    pub trade: PublicTrade, // market realtime trade data
    pub timestamp: i64,     // data timestamp
    pub time_period: u64,
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

#[derive(Debug, Error)]
pub enum TradeEventError {
    #[error("invalid trade data (price <= 0 or amount <= 0)")]
    InvalidTrade,
    #[error("event is not a PublicTrade item")]
    NotTradeEvent,
}

/// try from barter trade data event to PublicTradeEvent
impl TryFrom<Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> for PublicTradeEvent {
    type Error = TradeEventError;

    fn try_from(event: Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>) -> Result<Self, Self::Error> {
        match event {
            Event::Item(market_event) => {
                let public_trade = market_event.kind;
                if public_trade.is_valid() {
                    Ok(PublicTradeEvent {
                        exchange: market_event.exchange.to_string(),
                        symbol: format!("{}", market_event.instrument.base), // format!("{}{}", market_event.instrument.base, market_event.instrument.quote),
                        trade: public_trade,
                        timestamp: market_event.time_exchange.timestamp_millis(),
                        time_period: M1.get(),
                    })
                } else {
                    Err(TradeEventError::InvalidTrade)
                }
            }
            _ => Err(TradeEventError::NotTradeEvent),
        }
    }
}

/// convert TradeCandle to BaseBar
impl BaseBar {
    /// 构建 BaseBar，从 TradeCandle + 额外信息
    pub fn from_trade_candle(
        _exchange: &str,
        _symbol: &str,
        period: &str,
        _timestamp: i64,
        candle: TradeCandle,
    ) -> Self {
        let begin_time = milliseconds_to_offsetdatetime(candle.time_range.open_time);
        let end_time = milliseconds_to_offsetdatetime(candle.time_range.close_time);
        let time_period = parse_period_to_secs(period).unwrap_or(60); // 支持任意周期

        let close_price = candle.close.value();
        let volume = candle.volume.value();
        let amount = close_price * volume;

        BaseBar {
            time_period,
            begin_time,
            end_time,
            open_price: candle.open.value(),
            high_price: candle.high.value(),
            low_price: candle.low.value(),
            close_price,
            volume,
            amount,
            trades: candle.num_trades.value() as u64,
        }
    }
}

/// 保留 From<TradeCandle> 兼容原来的调用
impl From<TradeCandle> for BaseBar {
    fn from(c: TradeCandle) -> Self {
        BaseBar::from_trade_candle("", "", "1m", c.time_range.close_time, c)
    }
}

impl AggregatorOutput for BaseBar {
    fn from_trade_candle(exchange: &str, symbol: &str, period: &str, timestamp: i64, candle: TradeCandle) -> Self {
        BaseBar::from_trade_candle(exchange, symbol, period, timestamp, candle)
    }
}

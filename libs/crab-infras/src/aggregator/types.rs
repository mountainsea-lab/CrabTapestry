use crate::aggregator::AggregatorOutput;
use crate::cache::BaseBar;
use barter_data::barter_instrument::Side;
use barter_data::barter_instrument::exchange::ExchangeId;
use barter_data::barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_data::event::MarketEvent;
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::PublicTrade;
use crab_common_utils::time_utils::milliseconds_to_offsetdatetime;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
        self.price > 0.0 && self.price.is_finite() && self.amount > 0.0
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

/// from barter trade data event to PublicTradeEvent
impl From<Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> for PublicTradeEvent {
    fn from(event: Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>) -> Self {
        match event {
            Event::Item(market_event) => {
                PublicTradeEvent {
                    exchange: market_event.exchange.to_string(),
                    symbol: format!("{}", market_event.instrument.base), // 根据需求调整 format!("{}{}", market_event.instrument.base, market_event.instrument.quote)
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

impl<T: From<TradeCandle> + Send + 'static> AggregatorOutput for T {}

/// 实时订阅配置
#[derive(Debug, Clone)]
pub struct Subscription {
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub periods: Vec<Arc<str>>, // 多周期 ["1m","5m","1h"]
}

impl Subscription {
    /// 创建单周期订阅
    pub fn new(exchange: &str, symbol: &str, periods: &[&str]) -> Self {
        Self {
            exchange: Arc::from(exchange),
            symbol: Arc::from(symbol),
            periods: periods.iter().map(|p| Arc::from(*p)).collect(),
        }
    }

    /// 批量初始化 Vec
    pub fn batch_subscribe(exchange: &str, symbols: &[&str], periods: &[&str]) -> Vec<Subscription> {
        symbols.iter().map(|sym| Subscription::new(exchange, sym, periods)).collect()
    }

    /// 批量初始化订阅，返回 Arc<DashMap>
    pub fn init_subscriptions(subs: Vec<Subscription>) -> Arc<DashMap<(String, String), Subscription>> {
        let sub_map = Arc::new(DashMap::new());

        for sub in subs {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            sub_map.insert(key, sub);
        }

        sub_map
    }

    /// 获取指定交易所的所有 symbol
    pub fn get_symbols_for_exchange(
        sub_map: &Arc<DashMap<(String, String), Subscription>>,
        exchange: &str,
    ) -> Arc<Vec<Arc<str>>> {
        let symbols = sub_map
            .iter()
            .filter_map(|entry| {
                let (exch, symbol) = entry.key();
                if exch == exchange {
                    Some(Arc::from(symbol.as_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Arc::new(symbols)
    }

    /// 获取指定交易所的所有 Subscription
    pub fn get_subscriptions_for_exchange(
        sub_map: &Arc<DashMap<(String, String), Subscription>>,
        exchange: &str,
    ) -> Arc<Vec<Subscription>> {
        let subs = sub_map
            .iter()
            .filter_map(|entry| {
                let (exch, _) = entry.key();
                if exch == exchange {
                    Some(entry.value().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Arc::new(subs)
    }
}

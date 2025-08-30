use crate::ingestion::DataEvent;
use barter_data::barter_instrument::Side;
use barter_data::barter_instrument::exchange::ExchangeId;
use barter_data::barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_data::event::MarketEvent;
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::PublicTrade;
use trade_aggregation::M1;

/// The Trade data event wrapper at the time of transaction is used to contain public transaction data and additional information (such as timestamp)
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
                    symbol: format!("{}{}", market_event.instrument.base, market_event.instrument.quote), // 根据你的需求调整
                    trade: market_event.kind,                                                             // PublicTrade
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

impl DataEvent for PublicTradeEvent {
    fn id(&self) -> String {
        self.trade.id.clone()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

/// Twitter message data event
#[derive(Debug)]
#[allow(dead_code)]
pub struct TwitterEvent {
    pub message: String,
    pub user: String,
    pub timestamp: i64,
}

impl DataEvent for TwitterEvent {
    fn id(&self) -> String {
        self.user.clone()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

/// news message data event
#[derive(Debug)]
#[allow(dead_code)]
pub struct NewsEvent {
    pub headline: String,
    pub source: String,
    pub timestamp: i64,
}

impl DataEvent for NewsEvent {
    fn id(&self) -> String {
        self.source.clone()
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

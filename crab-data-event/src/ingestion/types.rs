use crate::ingestion::DataEvent;
use barter_data::barter_instrument::Side;
use barter_data::barter_instrument::exchange::ExchangeId;
use barter_data::barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_data::event::MarketEvent;
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::PublicTrade;
use serde::{Deserialize, Serialize};

/// The Trade data event wrapper at the time of transaction is used to contain public transaction data and additional information (such as timestamp)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PublicTradeEvent {
    pub symbol: String,
    pub trade: PublicTrade, // market realtime trade data
    pub timestamp: i64,     // data timestamp
}

/// from barter trade data event to PublicTradeEvent
impl From<Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> for PublicTradeEvent {
    fn from(event: Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>) -> Self {
        match event {
            Event::Item(market_event) => {
                PublicTradeEvent {
                    symbol: format!("{}{}", market_event.instrument.base, market_event.instrument.quote), // 根据你的需求调整
                    trade: market_event.kind,                                                             // PublicTrade
                    timestamp: market_event.time_exchange.timestamp(),                                    // 时间戳转换
                }
            }
            _ => {
                // if not PublicTrade data，return default
                PublicTradeEvent {
                    symbol: String::new(),
                    trade: PublicTrade {
                        id: String::new(),
                        price: 0.0,
                        amount: 0.0,
                        side: Side::Buy,
                    },
                    timestamp: 0,
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

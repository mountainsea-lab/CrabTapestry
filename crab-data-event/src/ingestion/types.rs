use crate::ingestion::DataEvent;
use crab_infras::aggregator::types::PublicTradeEvent;

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

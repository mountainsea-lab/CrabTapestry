use crate::ingestion::types::PublicTradeEvent;
use crossbeam::channel::Sender;

/// BarterIngestor acquires real-time trade data events from the exchange
pub struct BarterIngestor {
    sender: Sender<PublicTradeEvent>, // trade data transfer channel
}

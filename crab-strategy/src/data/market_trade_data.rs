use barter::Timed;
use barter::engine::Processor;
use barter::engine::state::instrument::data::InstrumentDataState;
use barter::engine::state::order::in_flight_recorder::InFlightRequestRecorder;
use barter_data::event::{DataKind, MarketEvent};
use barter_execution::AccountEvent;
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Basic [`InstrumentDataState`] that tracks the [`Kline`] and sets traded kline for an
/// instrument.
///
/// Trading strategies may wish to maintain more data here, such as candles, indicators,
/// KLines , etc.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor)]
pub struct MarketTradeData {
    pub last_traded_price: Option<Timed<Decimal>>,
}

impl InstrumentDataState for MarketTradeData {
    type MarketEventKind = DataKind;
    fn price(&self) -> Option<Decimal> {
        self.last_traded_price.as_ref().map(|timed| timed.value)
    }
}

impl<InstrumentKey: Display> Processor<&MarketEvent<InstrumentKey, DataKind>> for MarketTradeData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::Trade(_trade) => {
                todo!()
            }
            _ => {}
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey> Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
    for MarketTradeData
{
    type Audit = ();

    fn process(&mut self, _: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self::Audit {}
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey> for MarketTradeData {
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {}
}

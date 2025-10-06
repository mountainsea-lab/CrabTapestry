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

/// Basic [`InstrumentDataState`] implementation that tracks the [`OrderBookL1`] and last traded
/// price for an instrument.
///
/// This is a simple example of instrument level data. Trading strategies typically maintain more
/// comprehensive data, such as candles, technical indicators, market depth (L2 book), volatility metrics,
/// or bar_cache-specific state data.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor)]
pub struct StEmaData {
    pub last_traded_price: Option<Timed<Decimal>>,
}

impl InstrumentDataState for StEmaData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        // self.l1
        //     .volume_weighed_mid_price()
        //     .or(self.last_traded_price.as_ref().map(|timed| timed.value))
        todo!()
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>> for StEmaData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::Trade(trade) => {
                // info!("Processing trade event{:?}", trade);
                // if self
                //     .last_traded_price
                //     .as_ref()
                //     .is_none_or(|price| price.time < event.time_exchange)
                //     && let Some(price) = Decimal::from_f64(trade.price)
                // {
                //     self.last_traded_price
                //         .replace(Timed::new(price, event.time_exchange));
                // }
            }
            DataKind::OrderBookL1(_l1) => {
                // if self.l1.last_update_time < event.time_exchange {
                //     self.l1 = l1.clone()
                // }
            }
            _ => {}
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey> Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
    for StEmaData
{
    type Audit = ();

    fn process(&mut self, _: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self::Audit {}
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey> for StEmaData {
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {}
}

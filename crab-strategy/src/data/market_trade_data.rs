use barter::Timed;
use barter::engine::Processor;
use barter::engine::state::instrument::data::InstrumentDataState;
use barter::engine::state::order::in_flight_recorder::InFlightRequestRecorder;
use barter_data::event::{DataKind, MarketEvent};
use barter_execution::AccountEvent;
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use crab_infras::aggregator::trade_aggregator::TradeAggregatorPool;
use derive_more::Constructor;
use ms_tracing::tracing_utils::internal::info;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Basic [`InstrumentDataState`] implementation that tracks the [`OrderBookL1`] and last traded
/// price for an instrument.
///
/// This is a simple example of instrument level data. Trading strategies typically maintain more
/// comprehensive data, such as candles, technical indicators, market depth (L2 book), volatility metrics,
/// or bar_cache-specific state data.
// #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor)]
#[derive(Debug, Clone)]
pub struct StEmaData {
    pub last_traded_price: Option<Timed<Decimal>>,
    /// (exchange, symbol, period) -> MultiPeriodAggregator
    pub aggregators: Arc<TradeAggregatorPool>,
}

impl StEmaData {
    /// create a new StEmaData
    pub fn new() -> Self {
        Self {
            last_traded_price: None,
            aggregators: Arc::new(TradeAggregatorPool::new()),
        }
    }
}

impl InstrumentDataState for StEmaData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.last_traded_price.as_ref().map(|timed| timed.value)
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>> for StEmaData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        info!(
            "Processing Market {:#?}@{:#?}",
            event.exchange,
            // event.instrument,
            event.kind
        );
        match &event.kind {
            DataKind::Trade(trade) => {
                // todo 这里实时聚合然后 addbar
                // let latest_bar = agg(trade);
                // let series_cache_manager = global::get_bar_cache_manager();
                // append_bar
                // series_cache_manager.append_bar();
                // info!("Processing trade event{:?}", trade);
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

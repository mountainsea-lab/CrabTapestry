use crate::global;
use barter::Timed;
use barter::engine::Processor;
use barter::engine::state::instrument::data::InstrumentDataState;
use barter::engine::state::order::in_flight_recorder::InFlightRequestRecorder;
use barter::system::config::InstrumentConfig;
use barter_data::event::{DataKind, MarketEvent};
use barter_execution::AccountEvent;
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use barter_instrument::Keyed;
use barter_instrument::asset::AssetIndex;
use barter_instrument::exchange::{ExchangeId, ExchangeIndex};
use barter_instrument::index::IndexedInstruments;
use barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_instrument::instrument::{Instrument, InstrumentIndex};
use crab_infras::aggregator::trade_aggregator::TradeAggregatorPool;
use crab_infras::aggregator::types::PublicTradeEvent;
use crab_types::time_frame::TimeFrame::M1;
use derive_more::Constructor;
use ms_tracing::tracing_utils::internal::{info, warn};
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
    /// Index Relationships
    pub instruments: Arc<IndexedInstruments>,
}

impl StEmaData {
    /// create a new StEmaData
    pub fn new(instrument_config: Vec<InstrumentConfig>) -> Self {
        Self {
            last_traded_price: None,
            aggregators: Arc::new(TradeAggregatorPool::new()),
            instruments: Arc::new(IndexedInstruments::new(instrument_config.to_vec())),
        }
    }

    fn lookup_instrument(
        &self,
        index: InstrumentIndex,
    ) -> Option<(
        &Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>,
        ExchangeId,
        String,
    )> {
        let instrument = self.instruments.find_instrument(index).ok()?;
        let exchange = self.instruments.find_exchange(instrument.exchange.key).ok()?;
        let base = self.instruments.find_asset(instrument.underlying.base).ok()?;
        let quote = self.instruments.find_asset(instrument.underlying.quote).ok()?;
        let symbol = format!(
            "{}{}",
            base.asset.name_exchange.name().to_uppercase(),
            quote.asset.name_exchange.name().to_uppercase()
        );
        Some((instrument, exchange, symbol))
    }
}

impl InstrumentDataState for StEmaData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.last_traded_price.as_ref().map(|timed| timed.value)
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>> for StEmaData
where
    InstrumentKey: Clone + std::fmt::Display + Into<InstrumentIndex> + std::fmt::Debug,
{
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::Trade(trade) => {
                if let Some((_instrument, exchange_id, symbol)) =
                    self.lookup_instrument(event.instrument.clone().into())
                {
                    let pub_tv = PublicTradeEvent {
                        exchange: exchange_id.to_string(),
                        symbol,
                        trade: trade.clone(),
                        timestamp: event.time_exchange.timestamp_millis(),
                        time_period: M1.to_millis() as u64,
                    };
                    // info!("Trade Event: {:?}", pub_tv);
                    // 1. 聚合生成bar
                    let aggregator_pool = self.aggregators.clone();
                    let latest_bars = aggregator_pool.aggregate_trade(&pub_tv, &[M1.to_millis()]);
                    // 2. add bar
                    if !latest_bars.is_empty() {
                        // 3️⃣ 获取 BarCacheManager
                        let series_cache_manager = global::get_bar_cache_manager();
                        for latest_bar in latest_bars {
                            let _ = series_cache_manager.ensure_and_append_sync(&latest_bar.0, 300, latest_bar.1);
                        }
                    }
                } else {
                    warn!("Instrument lookup failed for {:?}", event.instrument);
                }
            }

            DataKind::OrderBookL1(_l1) => {
                // 可以复用 lookup_instrument 逻辑
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

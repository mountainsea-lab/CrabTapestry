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
                    let ev = PublicTradeEvent {
                        exchange: exchange_id.to_string(),
                        symbol,
                        trade: trade.clone(),
                        timestamp: event.time_exchange.timestamp_millis(),
                        time_period: M1.to_millis() as u64,
                    };
                    info!("Trade Event: {:?}", ev);
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

// impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>> for StEmaData
// where
//     InstrumentKey: Clone + std::fmt::Display + Into<InstrumentIndex> + std::fmt::Debug, // 关键：支持转换为 InstrumentIndex
// {
//     type Audit = ();
//
//     fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
//         let strategy_config = global::get_strategy_config().get();
//         let instruments_config = strategy_config.system_config.instruments.clone();
//
//         // 构建索引
//         let indexed = IndexedInstruments::new(instruments_config);
//
//         match &event.kind {
//             DataKind::Trade(trade) => {
//                 // 1️⃣ 获取 instrument 索引
//                 let instrument_index: InstrumentIndex = event.instrument.clone().into();
//
//                 // 2️⃣ 通过索引反查完整 Instrument 信息
//                 match indexed.find_instrument(instrument_index) {
//                     Ok(instrument) => {
//                         // 3️⃣ 获取 exchange / base / quote
//                         let exchange_id = indexed.find_exchange(instrument.exchange.key).unwrap();
//                         let base = indexed.find_asset(instrument.underlying.base).unwrap();
//                         let quote = indexed.find_asset(instrument.underlying.quote).unwrap();
//                         info!(
//                             "Trade @ {:?} | {:?}/{:?} ({:?})",
//                             exchange_id, base.asset, quote.asset, instrument.kind
//                         );
//
//                         // ✅ 构建更完整的 trade 事件
//                         let ev = PublicTradeEvent {
//                             exchange: exchange_id.to_string(),
//                             symbol: format!(
//                                 "{}{}",
//                                 base.asset.name_exchange.name().to_uppercase(),
//                                 quote.asset.name_exchange.name().to_uppercase(),
//                             ),
//                             trade: trade.clone(),
//                             timestamp: event.time_exchange.timestamp_millis(),
//                             time_period: 60,
//                         };
//
//                         info!("Trade Event: {:?}", ev);
//                     }
//                     Err(err) => {
//                         warn!("Instrument lookup failed for {:?}: {:?}", event.instrument, err);
//                     }
//                 }
//             }
//
//             DataKind::OrderBookL1(_l1) => {
//                 // 可在此同样反查资产信息
//             }
//
//             _ => {}
//         }
//     }
// }

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

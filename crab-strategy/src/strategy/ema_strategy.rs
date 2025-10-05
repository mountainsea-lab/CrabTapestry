use crate::data::market_trade_data::StEmaData;
use crate::strategy::gen_cid;
use barter::engine::Engine;
use barter::engine::clock::HistoricalClock;
use barter::engine::execution_tx::MultiExchangeTxMap;
use barter::engine::state::EngineState;
use barter::engine::state::global::DefaultGlobalData;
use barter::engine::state::instrument::data::InstrumentDataState;
use barter::engine::state::instrument::filter::InstrumentFilter;
use barter::execution::request::ExecutionRequest;
use barter::risk::DefaultRiskManager;
use barter::strategy::algo::AlgoStrategy;
use barter::strategy::close_positions::{ClosePositionsStrategy, close_open_positions_with_market_orders};
use barter::strategy::on_disconnect::OnDisconnectStrategy;
use barter::strategy::on_trading_disabled::OnTradingDisabled;
use barter_execution::order::id::{ClientOrderId, StrategyId};
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen, RequestOpen};
use barter_execution::order::{OrderKey, OrderKind, TimeInForce};
use barter_instrument::Side;
use barter_instrument::asset::AssetIndex;
use barter_instrument::exchange::{ExchangeId, ExchangeIndex};
use barter_instrument::instrument::InstrumentIndex;
use barter_integration::channel::UnboundedTx;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

pub struct EmaStrategy {
    pub(crate) id: StrategyId,
}

impl AlgoStrategy for EmaStrategy {
    type State = EngineState<DefaultGlobalData, StEmaData>;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let opens = state.instruments.instruments(&InstrumentFilter::None).filter_map(|state| {
            // Don't open more if we have a Position already
            if state.position.current.is_some() {
                return None;
            }

            // Don't open more orders if there are already some InFlight
            if !state.orders.0.is_empty() {
                return None;
            }

            // Don't open if there is no instrument market price available
            let price = state.data.price()?;
            // todo!() ta4r指标计算
            // Generate Market order to buy the minimum allowed quantity
            Some(OrderRequestOpen {
                key: OrderKey {
                    exchange: state.instrument.exchange,
                    instrument: state.key,
                    strategy: self.id.clone(),
                    cid: gen_cid(state.key.index()),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    price,
                    quantity: Decimal::from_f64(1f64).unwrap(),
                },
            })
        });

        (std::iter::empty(), opens)
    }
}

impl ClosePositionsStrategy for EmaStrategy {
    type State = EngineState<DefaultGlobalData, StEmaData>;

    fn close_positions_requests<'a>(
        &'a self,
        state: &'a Self::State,
        filter: &'a InstrumentFilter<ExchangeIndex, AssetIndex, InstrumentIndex>,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        close_open_positions_with_market_orders(&self.id, state, filter, |state| {
            ClientOrderId::new(state.key.to_string())
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct OnDisconnectOutput;
impl
    OnDisconnectStrategy<
        HistoricalClock,
        EngineState<DefaultGlobalData, StEmaData>,
        MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
        DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
    > for EmaStrategy
{
    type OnDisconnect = OnDisconnectOutput;

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, StEmaData>,
            MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
        OnDisconnectOutput
    }
}

#[derive(Debug, PartialEq)]
pub struct OnTradingDisabledOutput;
impl
    OnTradingDisabled<
        HistoricalClock,
        EngineState<DefaultGlobalData, StEmaData>,
        MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
        DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
    > for EmaStrategy
{
    type OnTradingDisabled = OnTradingDisabledOutput;

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, StEmaData>,
            MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
        >,
    ) -> Self::OnTradingDisabled {
        OnTradingDisabledOutput
    }
}

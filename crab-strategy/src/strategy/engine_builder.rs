use crate::data::st_ema_data::StEmaData;
use crate::strategy::ema_strategy::EmaStrategy;
use crate::strategy::{
    STARTING_BALANCE_BTC, STARTING_BALANCE_ETH, STARTING_BALANCE_USDT, STARTING_TIMESTAMP, strategy_id,
};
use barter::engine::Engine;
use barter::engine::clock::HistoricalClock;
use barter::engine::execution_tx::MultiExchangeTxMap;
use barter::engine::state::EngineState;
use barter::engine::state::global::DefaultGlobalData;
use barter::engine::state::trading::TradingState;
use barter::execution::request::ExecutionRequest;
use barter::risk::DefaultRiskManager;
use barter_instrument::Underlying;
use barter_instrument::exchange::ExchangeId;
use barter_instrument::index::IndexedInstruments;
use barter_instrument::instrument::Instrument;
use barter_instrument::instrument::spec::{
    InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity, OrderQuantityUnits,
};
use barter_integration::channel::UnboundedTx;
use fnv::FnvHashMap;
use rust_decimal_macros::dec;

fn build_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> Engine<
    HistoricalClock,
    EngineState<DefaultGlobalData, StEmaData>,
    MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
    EmaStrategy,
    DefaultRiskManager<EngineState<DefaultGlobalData, StEmaData>>,
> {
    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdt",
            "BTCUSDT",
            Underlying::new("btc", "usdt"),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
                InstrumentSpecQuantity::new(OrderQuantityUnits::Quote, dec!(0.00001), dec!(0.00001)),
                InstrumentSpecNotional::new(dec!(5.0)),
            )),
        ))
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_eth_btc",
            "ETHBTC",
            Underlying::new("eth", "btc"),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.00001), dec!(0.00001)),
                InstrumentSpecQuantity::new(OrderQuantityUnits::Quote, dec!(0.0001), dec!(0.0001)),
                InstrumentSpecNotional::new(dec!(0.0001)),
            )),
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);

    let state = EngineState::builder(&instruments, DefaultGlobalData::default(), |_| StEmaData::default())
        .time_engine_start(STARTING_TIMESTAMP)
        .trading_state(trading_state)
        .balances([
            (ExchangeId::BinanceSpot, "usdt", STARTING_BALANCE_USDT),
            (ExchangeId::BinanceSpot, "btc", STARTING_BALANCE_BTC),
            (ExchangeId::BinanceSpot, "eth", STARTING_BALANCE_ETH),
        ])
        .build();

    let initial_account = FnvHashMap::from(&state);
    assert_eq!(initial_account.len(), 1);

    let execution_txs = MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]);

    Engine::new(
        clock,
        state,
        execution_txs,
        EmaStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

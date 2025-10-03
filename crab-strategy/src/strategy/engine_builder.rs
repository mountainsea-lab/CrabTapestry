use crate::data::st_ema_data::StEmaData;
use crate::strategy::ema_strategy::EmaStrategy;
use barter::engine::Engine;
use barter::engine::clock::HistoricalClock;
use barter::engine::execution_tx::MultiExchangeTxMap;
use barter::engine::state::EngineState;
use barter::engine::state::global::DefaultGlobalData;
use barter::engine::state::trading::TradingState;
use barter::execution::request::ExecutionRequest;
use barter::risk::DefaultRiskManager;
use barter_integration::channel::UnboundedTx;

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
    todo!()
}

use crate::strategy::STARTING_TIMESTAMP;
use barter::EngineEvent;
use barter::engine::command::Command;
use barter::engine::state::instrument::filter::InstrumentFilter;
use barter::test_utils::time_plus_days;
use barter_data::event::{DataKind, MarketEvent};
use barter_data::streams::consumer::MarketStreamEvent;
use barter_data::subscription::trade::PublicTrade;
use barter_instrument::Side;
use barter_instrument::exchange::ExchangeId;
use barter_instrument::instrument::InstrumentIndex;
use barter_integration::collection::one_or_many::OneOrMany;

/// 构造市场成交事件
pub fn market_event_trade(time_plus: u64, instrument: usize, price: f64) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        time_received: time_plus_days(STARTING_TIMESTAMP, time_plus),
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex(instrument),
        kind: DataKind::Trade(PublicTrade {
            id: time_plus.to_string(),
            price,
            amount: 1.0,
            side: Side::Buy,
        }),
    }))
}

/// 构造关闭仓位命令
pub fn command_close_position(instrument: usize) -> EngineEvent<DataKind> {
    EngineEvent::Command(Command::ClosePositions(InstrumentFilter::Instruments(OneOrMany::One(
        InstrumentIndex(instrument),
    ))))
}

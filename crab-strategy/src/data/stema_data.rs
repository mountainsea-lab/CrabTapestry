use crate::data::market_trade_data::MarketTradeData;
use barter::engine::Processor;
use barter::engine::state::instrument::data::InstrumentDataState;
use barter::engine::state::order::in_flight_recorder::InFlightRequestRecorder;
use barter_data::event::{DataKind, MarketEvent};
use barter_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use barter_execution::{AccountEvent, AccountEventKind};
use barter_instrument::exchange::ExchangeIndex;
use barter_instrument::instrument::InstrumentIndex;
use rust_decimal::Decimal;

//====== 策略市场数据对象和自定元数据对象集合=====
#[derive(Debug, Clone, Default)]
pub(crate) struct EmaStData {
    pub(crate) market_data: MarketTradeData,
}

impl EmaStData {
    pub fn init() -> Self {
        Self { market_data: MarketTradeData::default() }
    }
}

impl InstrumentDataState for EmaStData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.market_data.price()
    }
}

impl<InstrumentKey: std::fmt::Display> Processor<&MarketEvent<InstrumentKey, DataKind>> for EmaStData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        // todo process逻辑里面完成 BarSeries的初始化以及 最新bar的追加逻辑
        self.market_data.process(event)
    }
}

impl Processor<&AccountEvent> for EmaStData {
    type Audit = ();

    fn process(&mut self, event: &AccountEvent) -> Self::Audit {
        let AccountEventKind::Trade(_trade) = &event.kind else {
            return;
        };
        // 账号相关数据处理
        todo!()
    }
}

impl InFlightRequestRecorder for EmaStData {
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeIndex, InstrumentIndex>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeIndex, InstrumentIndex>) {}
}

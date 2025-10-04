use barter_execution::balance::Balance;
use barter_execution::order::id::{ClientOrderId, OrderId, StrategyId};
use barter_execution::trade::TradeId;
use barter_instrument::instrument::InstrumentIndex;
use chrono::{DateTime, Utc};
use rust_decimal_macros::dec;

pub mod ema_strategy;
pub mod engine_builder;
pub mod events;

const STARTING_TIMESTAMP: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

const STARTING_BALANCE_USDT: Balance = Balance {
    total: dec!(40_000.0),
    free: dec!(40_000.0),
};
const STARTING_BALANCE_BTC: Balance = Balance { total: dec!(1.0), free: dec!(1.0) };
const STARTING_BALANCE_ETH: Balance = Balance { total: dec!(10.0), free: dec!(10.0) };

const QUOTE_FEES_PERCENT: f64 = 0.1; // 10%

pub fn strategy_id() -> StrategyId {
    StrategyId::new("EmaStrategy")
}

pub fn gen_cid(instrument: usize) -> ClientOrderId {
    ClientOrderId::new(InstrumentIndex(instrument).to_string())
}

pub fn gen_trade_id(instrument: usize) -> TradeId {
    TradeId::new(InstrumentIndex(instrument).to_string())
}

pub fn gen_order_id(instrument: usize) -> OrderId {
    OrderId::new(InstrumentIndex(instrument).to_string())
}

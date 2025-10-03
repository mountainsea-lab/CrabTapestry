use crate::strategy::{QUOTE_FEES_PERCENT, STARTING_TIMESTAMP, gen_cid, gen_order_id, gen_trade_id, strategy_id};
use barter::EngineEvent;
use barter::engine::state::asset::AssetStates;
use barter::execution::AccountStreamEvent;
use barter::test_utils::time_plus_days;
use barter_data::event::DataKind;
use barter_execution::balance::{AssetBalance, Balance};
use barter_execution::order::state::{Open, OrderState};
use barter_execution::order::{Order, OrderKey, OrderKind, TimeInForce};
use barter_execution::trade::{AssetFees, Trade};
use barter_execution::{AccountEvent, AccountEventKind, AccountSnapshot};
use barter_instrument::Side;
use barter_instrument::asset::AssetIndex;
use barter_instrument::exchange::ExchangeIndex;
use barter_instrument::instrument::InstrumentIndex;
use barter_integration::snapshot::Snapshot;
use rust_decimal::Decimal;

/// 构造账户快照事件
pub fn account_event_snapshot(assets: &AssetStates) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Snapshot(AccountSnapshot {
            exchange: ExchangeIndex(0),
            balances: assets
                .0
                .iter()
                .enumerate()
                .map(|(index, (_, state))| AssetBalance {
                    asset: AssetIndex(index),
                    balance: state.balance.unwrap().value,
                    time_exchange: state.balance.unwrap().time,
                })
                .collect(),
            instruments: vec![],
        }),
    }))
}

/// 构造订单响应事件
pub fn account_event_order_response(
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: f64,
    quantity: f64,
    filled: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(instrument),
                strategy: strategy_id(),
                cid: gen_cid(instrument),
            },
            side,
            price: Decimal::try_from(price).unwrap(),
            quantity: Decimal::try_from(quantity).unwrap(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            state: OrderState::active(Open {
                id: gen_order_id(instrument),
                time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
                filled_quantity: Decimal::try_from(filled).unwrap(),
            }),
        })),
    }))
}

/// 构造余额更新事件
pub fn account_event_balance(asset: usize, time_plus: u64, total: f64, free: f64) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance {
            asset: AssetIndex(asset),
            balance: Balance::new(Decimal::try_from(total).unwrap(), Decimal::try_from(free).unwrap()),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        })),
    }))
}

/// 构造账户成交事件
pub fn account_event_trade(
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: f64,
    quantity: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: gen_trade_id(instrument),
            order_id: gen_order_id(instrument),
            instrument: InstrumentIndex(instrument),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
            side,
            price: Decimal::try_from(price).unwrap(),
            quantity: Decimal::try_from(quantity).unwrap(),
            fees: AssetFees::quote_fees(Decimal::try_from(price * quantity * QUOTE_FEES_PERCENT).unwrap()),
        }),
    }))
}

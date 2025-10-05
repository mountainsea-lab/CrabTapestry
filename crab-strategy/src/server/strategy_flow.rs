use crate::trader::crab_trader::CrabTrader;
use ms_tracing::tracing_utils::internal::info;
use std::sync::Arc;

pub async fn start_strategy_flow() {
    // 2️⃣ 初始化交易器
    let trader = CrabTrader::create().await.expect("Crab trader created failed");
    let trader_arc = Arc::new(trader);
    if trader_arc.enable_trading().await.is_ok() {
        info!("Trader enable trading");
    }
}

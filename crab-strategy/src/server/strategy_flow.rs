use crate::global;
use crab_infras::cache::bar_cache::bar_key::BarKey;
use ms_tracing::tracing_utils::internal::{error, info};
use std::time::Duration;

pub async fn start_strategy_flow() {
    // 1️⃣ 获取交易器 (后续可扩展实时交易、监控等)
    // let _trader = global::get_crab_trader();

    // 2️⃣ 获取策略配置管理器 读取 subscriptions（clone 内部数据避免 move）
    // let strategy_config = global::get_strategy_config().get();
    // let subscriptions = strategy_config.subscriptions.clone();

    // 3️⃣ 获取 BarCacheManager 先采用实时维护方式 这里暂时注释
    // let series_cache_manager = global::get_bar_cache_manager();

    // 4️⃣ 根据 subscriptions 生成所有 BarKey
    // let mut keys = Vec::new();
    // for sub in subscriptions {
    //     for symbol in &sub.symbols {
    //         let periods = symbol
    //             .periods
    //             .clone()
    //             .or_else(|| sub.default_periods.clone())
    //             .unwrap_or_else(|| vec!["1m".into()]);
    //         for period in periods {
    //             keys.push(BarKey {
    //                 exchange: sub.exchange.clone(),
    //                 symbol: symbol.name.clone(),
    //                 period,
    //             });
    //         }
    //     }
    // }
    // info!("starting strategy flow  keys {:#?}", keys);
    //
    // // 5️⃣ 批量加载历史 K 线
    // series_cache_manager
    //     .ensure_loaded_default_batch(keys.clone(), 300)
    //     .await
    //     .expect("ensure load batch failed");
    //
    // // ✅ 缓存初始化完成，可直接构建策略指标
    // let ready = series_cache_manager
    //     .wait_ready_batch_majority(&keys, Some(Duration::from_secs(5)))
    //     .await;
    // if !ready {
    //     error!("⚠️ 大部分 K 线数据未加载完成，策略指标可能不完整");
    // }
    // todo 策略需要初始化
    // let series_map = keys.iter()
    //     .map(|key| (key.clone(), series_cache_manager.get_series_arc(key).await))
    //     .collect::<HashMap<_, _>>();
    //
    // // 传给策略构建函数 todo 考虑策略指标组合是否也可以全局初始化
    // let strategy = build_strategy(series_map);
    //
    // 开启交易（可选）
    // trader.enable_trading().await.unwrap();
}

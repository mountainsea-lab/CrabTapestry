use crab_hmds::config::AppConfig;
use crab_hmds::load_app_config;
use crab_infras::config::sub_config::Subscription;
use dashmap::DashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 假设 hmds.toml 在当前目录
    let app_config = load_app_config().expect("系统应用配置信息读取失败");
    let lookback_days = app_config.app.lookback_days;
    // -------------------------------
    // 2️⃣ 加载订阅配置
    // -------------------------------
    let subscriptions = load_subscriptionMaps(app_config.clone())?;

    for (_key_tuple, sub) in subscriptions.iter().map(|e| (e.key().clone(), e.value().clone())) {
        for period in &sub.periods {
            // 交易所
            let exchange = sub.exchange.to_string();
            // 币种
            let symbol = sub.symbol.to_string();
            // 周期
            let period = period.to_string();
            // todo 结合lookback_days 预生成历史数据回溯区间从当前时间开始;注意周期对于的时间是否需要对齐,封装为异步函数generateMarketFillRanges
            // 返回值为&[NewHmdsMarketFillRange];
            // 另外 是否需要增加兼容HmdsMarketFillRange 存在历史数据的情况,如果存在新配置时间超过历史最早则追加新区间记录,否则无需处理
        }
    }
    Ok(())
}

/// 从 TOML 文件加载并初始化 SubscriptionMap
pub fn load_subscriptionMaps(app_config: AppConfig) -> anyhow::Result<Arc<DashMap<(String, String), Subscription>>> {
    let map = Arc::new(DashMap::new());
    for exch_cfg in &app_config.subscriptions {
        for sub in exch_cfg.to_subscriptions() {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            map.insert(key, sub);
        }
    }
    Ok(map)
}

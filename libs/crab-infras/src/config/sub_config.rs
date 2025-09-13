use dashmap::DashMap;
use serde::Deserialize;
use std::fs;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct SymbolConfig {
    pub name: String,
    pub periods: Option<Vec<String>>, // 可以为空，使用默认周期
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionConfig {
    pub exchange: String,
    pub default_periods: Option<Vec<String>>, // 新增可选默认周期
    pub symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionsFile {
    subscriptions: Vec<SubscriptionConfig>,
}

/// 实时订阅配置
#[derive(Debug, Clone)]
pub struct Subscription {
    pub exchange: Arc<str>,
    pub symbol: Arc<str>,
    pub periods: Vec<Arc<str>>, // 多周期 ["1m","5m","1h"]
}

impl SubscriptionConfig {
    pub fn to_subscriptions(&self) -> Vec<Subscription> {
        let default_periods = self
            .default_periods
            .as_ref()
            .map(|v| v.iter().map(|p| Arc::from(p.as_str())).collect::<Vec<_>>())
            .unwrap_or_default();

        self.symbols
            .iter()
            .map(|s| {
                let periods = s
                    .periods
                    .as_ref()
                    .map(|v| v.iter().map(|p| Arc::from(p.as_str())).collect::<Vec<_>>())
                    .unwrap_or_else(|| default_periods.clone());

                Subscription {
                    exchange: Arc::from(self.exchange.as_str()),
                    symbol: Arc::from(s.name.as_str()),
                    periods,
                }
            })
            .collect()
    }
}

/// 从 TOML 文件加载并初始化 SubscriptionMap
pub fn load_subscriptions_map(path: &str) -> anyhow::Result<Arc<DashMap<(String, String), Subscription>>> {
    let content = fs::read_to_string(path)?;
    let file: SubscriptionsFile = toml::from_str(&content)?;

    let map = Arc::new(DashMap::new());
    for exch_cfg in file.subscriptions {
        for sub in exch_cfg.to_subscriptions() {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            map.insert(key, sub);
        }
    }
    Ok(map)
}

impl Subscription {
    /// 创建单周期订阅
    pub fn new(exchange: &str, symbol: &str, periods: &[&str]) -> Self {
        Self {
            exchange: Arc::from(exchange),
            symbol: Arc::from(symbol),
            periods: periods.iter().map(|p| Arc::from(*p)).collect(),
        }
    }

    /// 批量初始化 Vec
    pub fn batch_subscribe(exchange: &str, symbols: &[&str], periods: &[&str]) -> Vec<Subscription> {
        symbols.iter().map(|sym| Subscription::new(exchange, sym, periods)).collect()
    }

    /// 批量初始化订阅，返回 Arc<DashMap>
    pub fn init_subscriptions(subs: Vec<Subscription>) -> Arc<DashMap<(String, String), Subscription>> {
        let sub_map = Arc::new(DashMap::new());

        for sub in subs {
            let key = (sub.exchange.to_string(), sub.symbol.to_string());
            sub_map.insert(key, sub);
        }

        sub_map
    }

    /// 获取指定交易所的所有 symbol
    pub fn get_symbols_for_exchange(
        sub_map: &Arc<DashMap<(String, String), Subscription>>,
        exchange: &str,
    ) -> Arc<Vec<Arc<str>>> {
        let symbols = sub_map
            .iter()
            .filter_map(|entry| {
                let (exch, symbol) = entry.key();
                if exch == exchange {
                    Some(Arc::from(symbol.as_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Arc::new(symbols)
    }

    /// 获取指定交易所的所有 Subscription
    pub fn get_subscriptions_for_exchange(
        sub_map: &Arc<DashMap<(String, String), Subscription>>,
        exchange: &str,
    ) -> Arc<Vec<Subscription>> {
        let subs = sub_map
            .iter()
            .filter_map(|entry| {
                let (exch, _) = entry.key();
                if exch == exchange {
                    Some(entry.value().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Arc::new(subs)
    }
}

/// SubscriptionMap 类型别名
pub type SubscriptionMap = Arc<DashMap<(String, String), Subscription>>;

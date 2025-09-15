use crate::config::sub_config::{Subscription, SubscriptionMap};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Notify;

/// 订阅管理器
#[derive(Debug, Clone)]
pub struct SubscriptionManager {
    inner: SubscriptionMap,
    notify: Arc<Notify>, // 通知依赖组件订阅变化
}

impl SubscriptionManager {
    /// 创建一个空 SubscriptionManager
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// 从现有 SubscriptionMap 创建
    pub fn from_map(map: SubscriptionMap) -> Self {
        Self {
            inner: map,
            notify: Arc::new(Notify::new()),
        }
    }

    /// 添加或更新订阅
    pub fn add(&self, sub: Subscription) {
        let key = (sub.exchange.to_string(), sub.symbol.to_string());
        self.inner.insert(key, sub);
        self.notify.notify_waiters(); // 通知监听者
    }

    /// 批量添加订阅
    pub fn add_batch(&self, subs: Vec<Subscription>) {
        for sub in subs {
            self.add(sub);
        }
    }

    /// 删除订阅
    pub fn remove(&self, exchange: &str, symbol: &str) {
        let key = (exchange.to_string(), symbol.to_string());
        self.inner.remove(&key);
        self.notify.notify_waiters();
    }

    /// 获取指定交易所的所有订阅
    pub fn get_subscriptions_for_exchange(&self, exchange: &str) -> Vec<Subscription> {
        self.inner
            .iter()
            .filter_map(|entry| {
                let (exch, _) = entry.key();
                if exch == exchange {
                    Some(entry.value().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// 获取指定交易所的所有 symbol
    pub fn get_symbols_for_exchange(&self, exchange: &str) -> Vec<Arc<str>> {
        self.inner
            .iter()
            .filter_map(|entry| {
                let (exch, sym) = entry.key();
                if exch == exchange {
                    Some(Arc::from(sym.as_str()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// 检查订阅是否存在
    pub fn exists(&self, exchange: &str, symbol: &str) -> bool {
        let key = (exchange.to_string(), symbol.to_string());
        self.inner.contains_key(&key)
    }

    /// 订阅更新通知
    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    /// 获取底层 SubscriptionMap
    pub fn inner_map(&self) -> SubscriptionMap {
        self.inner.clone()
    }
}

use crate::meta::view::StrategyVisualization;
use crate::strategy::CrabStrategyAny;
use chrono::{DateTime, Utc};
use crab_types::bar_cache::bar_key::BarKey;
use dashmap::DashMap;
use std::rc::Rc;

/// ✅ Ta4rRuntimeRegistry: 运行时策略注册中心
#[derive(Default, Clone)]
pub struct Ta4rRuntimeRegistry {
    pub strategy_bundles: Rc<DashMap<BarKey, Rc<dyn CrabStrategyAny>>>,
    pub metadata: Rc<DashMap<BarKey, StrategyMeta>>,
}

/// 📄 策略元信息
#[derive(Clone, Debug)]
pub struct StrategyMeta {
    pub name: String,
    pub registered_at: DateTime<Utc>,
    pub description: Option<String>,
}

impl Ta4rRuntimeRegistry {
    // =========================================================
    // ✅ 基础注册 / 查询
    // =========================================================

    /// 注册策略（带可选描述），同一 BarKey 只注册一次
    pub fn register_strategy(&self, key: BarKey, strategy: Rc<dyn CrabStrategyAny>, description: Option<String>) {
        use dashmap::mapref::entry::Entry;

        // 1️⃣ 尝试插入策略，如果已经存在就忽略
        match self.strategy_bundles.entry(key.clone()) {
            Entry::Occupied(_) => {
                // 已经注册，直接返回
                return;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(strategy.clone());
            }
        }

        // 2️⃣ 插入元数据
        let meta = StrategyMeta {
            name: strategy.name().to_string(), // 使用策略自身名称
            registered_at: Utc::now(),
            description,
        };
        self.metadata.insert(key, meta);
    }

    /// 获取策略实例
    pub fn get_strategy(&self, key: &BarKey) -> Option<Rc<dyn CrabStrategyAny>> {
        self.strategy_bundles.get(key).map(|v| v.clone())
    }

    /// 移除策略（返回移除的实例）
    pub fn remove_strategy(&self, key: &BarKey) -> Option<Rc<dyn CrabStrategyAny>> {
        self.metadata.remove(key);
        self.strategy_bundles.remove(key).map(|(_, v)| v)
    }

    // =========================================================
    // 🔍 查询辅助接口
    // =========================================================

    /// 检查是否存在某策略
    pub fn contains(&self, key: &BarKey) -> bool {
        self.strategy_bundles.contains_key(key)
    }

    /// 获取所有策略 key
    pub fn list_keys(&self) -> Vec<BarKey> {
        self.strategy_bundles.iter().map(|kv| kv.key().clone()).collect()
    }

    /// 获取元信息
    pub fn get_meta(&self, key: &BarKey) -> Option<StrategyMeta> {
        self.metadata.get(key).map(|v| v.clone())
    }

    /// 获取所有策略名
    pub fn list_strategy_names(&self) -> Vec<String> {
        self.strategy_bundles
            .iter()
            .filter_map(|kv| Some(kv.value().name().to_string()))
            .collect()
    }

    // =========================================================
    // 🧠 策略运行接口（直接调用策略行为）
    // =========================================================

    /// 在注册中心中执行 should_enter / should_exit
    pub fn evaluate_signal(&self, key: &BarKey, index: usize) -> Option<(bool, bool)> {
        self.get_strategy(key).map(|s| (s.should_enter(index), s.should_exit(index)))
    }

    /// 获取策略的可视化数据
    pub fn get_visualization(&self, key: &BarKey) -> Option<StrategyVisualization> {
        self.get_strategy(key).and_then(|s| s.get_visualization_data())
    }
}

use crate::meta::view::StrategyVisualization;
use crate::strategy::CrabStrategyAny;
use chrono::{DateTime, Utc};
use crab_types::bar_cache::bar_key::BarKey;
use dashmap::DashMap;
use std::sync::Arc;

/// ✅ Ta4rRuntimeRegistry: 运行时策略注册中心
#[derive(Default, Clone)]
pub struct Ta4rRuntimeRegistry {
    /// 策略实例：BarKey -> 策略对象（线程安全、并发读写）
    pub strategy_bundles: Arc<DashMap<BarKey, Arc<dyn CrabStrategyAny>>>,

    /// 元数据表（可选）：记录注册时间、状态、描述
    pub metadata: Arc<DashMap<BarKey, StrategyMeta>>,
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

    /// 注册策略（带可选描述）
    pub fn register_strategy(&self, key: BarKey, strategy: Arc<dyn CrabStrategyAny>, description: Option<String>) {
        self.strategy_bundles.insert(key.clone(), strategy);
        self.metadata.insert(
            key,
            StrategyMeta {
                name: "Unknown".to_string(),
                registered_at: Utc::now(),
                description,
            },
        );
    }

    /// 获取策略实例
    pub fn get_strategy(&self, key: &BarKey) -> Option<Arc<dyn CrabStrategyAny>> {
        self.strategy_bundles.get(key).map(|v| v.clone())
    }

    /// 移除策略（返回移除的实例）
    pub fn remove_strategy(&self, key: &BarKey) -> Option<Arc<dyn CrabStrategyAny>> {
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

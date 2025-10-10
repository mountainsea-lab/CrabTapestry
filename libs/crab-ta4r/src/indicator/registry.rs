use crate::meta::indicator_meta::IndicatorMeta;
use crate::meta::rule_meta::RuleMeta;
use crate::meta::strategy_meta::StrategyMeta;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 顶层注册中心：指标 + 规则 + 策略
///
/// 注册中心：存储指标与规则的描述信息
#[derive(Default, Clone)]
pub struct CrabRegistry {
    /// 指标元信息注册表
    pub indicators: Arc<RwLock<HashMap<String, IndicatorMeta>>>,

    /// 规则元信息注册表
    pub rules: Arc<RwLock<HashMap<String, RuleMeta>>>,
}

impl CrabRegistry {
    // -------------------- 注册接口 --------------------
    pub fn register_indicator(&self, meta: IndicatorMeta) {
        self.indicators.write().insert(meta.name.clone(), meta);
    }

    pub fn register_rule(&self, meta: RuleMeta) {
        self.rules.write().insert(meta.name.clone(), meta);
    }

    // -------------------- 查询接口 --------------------
    pub fn list_indicators(&self) -> Vec<IndicatorMeta> {
        self.indicators.read().values().cloned().collect()
    }

    pub fn list_rules(&self) -> Vec<RuleMeta> {
        self.rules.read().values().cloned().collect()
    }

    // -------------------- 按名称查询 --------------------
    pub fn get_indicator(&self, name: &str) -> Option<IndicatorMeta> {
        self.indicators.read().get(name).cloned()
    }

    pub fn get_rule(&self, name: &str) -> Option<RuleMeta> {
        self.rules.read().get(name).cloned()
    }
}

use crate::rule::typs::RuleMeta;
use crate::types::IndicatorMeta;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 注册中心：存储指标与规则的描述信息
#[derive(Default, Clone)]
pub struct CrabRegistry {
    pub indicators: Arc<RwLock<HashMap<String, IndicatorMeta>>>,
    pub rules: Arc<RwLock<HashMap<String, RuleMeta>>>,
}

impl CrabRegistry {
    pub fn register_indicator(&self, meta: IndicatorMeta) {
        self.indicators.write().insert(meta.name.clone(), meta);
    }

    pub fn register_rule(&self, meta: RuleMeta) {
        self.rules.write().insert(meta.name.clone(), meta);
    }

    pub fn list_indicators(&self) -> Vec<IndicatorMeta> {
        self.indicators.read().values().cloned().collect()
    }

    pub fn list_rules(&self) -> Vec<RuleMeta> {
        self.rules.read().values().cloned().collect()
    }
}

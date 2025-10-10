use crate::strategy::strategy_meta::StrategyMeta;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct CrabRegistry {
    pub strategies: Arc<RwLock<HashMap<String, StrategyMeta>>>,
}

impl CrabRegistry {
    pub fn register_strategy(&self, meta: StrategyMeta) {
        self.strategies.write().insert(meta.name.clone(), meta);
    }

    pub fn get_strategy(&self, name: &str) -> Option<StrategyMeta> {
        self.strategies.read().get(name).cloned()
    }
}

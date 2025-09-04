use std::sync::Arc;
use dashmap::DashSet;
use crate::ingestor::dedup::DeduplicatorBackend;

/// 历史回补模式
/// history replay mode
#[derive(Debug, Clone)]
pub struct DashSetBackend {
    seen: Arc<DashSet<Arc<str>>>,
}

impl DashSetBackend {
    pub fn new() -> Self {
        Self { seen: Arc::new(DashSet::new()) }
    }
}

impl DeduplicatorBackend for DashSetBackend {
    fn insert(&self, key: Arc<str>, _ts: i64) -> bool {
        self.seen.insert(key)
    }

    fn reset(&self) {
        self.seen.clear();
    }

    fn gc(&self, _now: i64) {
        // 历史回补无需 gc
        // no need to gc in history replay mode
    }
}
use crate::ingestor::dedup::{Deduplicatable, DeduplicatorBackend};
use dashmap::{DashMap, DashSet};
use std::sync::Arc;

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

/// 实时流式模式
/// real-time streaming mode
#[derive(Debug, Clone)]
pub struct DashMapBackend {
    seen: Arc<DashMap<Arc<str>, i64>>, // key -> last_seen_ts
    ttl_ms: i64,
}

impl DashMapBackend {
    pub fn new(ttl_ms: i64) -> Self {
        Self { seen: Arc::new(DashMap::new()), ttl_ms }
    }
}

impl DeduplicatorBackend for DashMapBackend {
    fn insert(&self, key: Arc<str>, ts: i64) -> bool {
        match self.seen.insert(key.clone(), ts) {
            None => true,                          // 第一次 first time
            Some(prev_ts) if ts > prev_ts => true, // 新数据覆盖旧数据 new data overrides old data
            _ => false,                            // 重复 repeated
        }
    }

    fn reset(&self) {
        self.seen.clear();
    }

    fn gc(&self, now: i64) {
        let ttl = self.ttl_ms;
        self.seen.retain(|_, &mut ts| now - ts <= ttl);
    }
}


#[derive(Debug, Clone)]
pub struct Deduplicator<T, B>
where
    T: Deduplicatable + Send + Sync,
    B: DeduplicatorBackend,
{
    backend: B,
    _marker: std::marker::PhantomData<T>,
}

impl<T, B> Deduplicator<T, B>
where
    T: Deduplicatable + Send + Sync,
    B: DeduplicatorBackend,
{
    pub fn new(backend: B) -> Self {
        Self { backend, _marker: std::marker::PhantomData }
    }

    pub fn deduplicate(&self, records: Vec<T>) -> Vec<T> {
        let mut result = Vec::with_capacity(records.len());
        for record in records {
            let key = record.unique_key();
            let ts = record.timestamp();
            if self.backend.insert(key, ts) {
                result.push(record);
            }
        }
        result
    }

    pub fn reset(&self) {
        self.backend.reset();
    }

    pub fn gc(&self, now: i64) {
        self.backend.gc(now);
    }
}
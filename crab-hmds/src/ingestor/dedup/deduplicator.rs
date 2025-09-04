use crate::ingestor::dedup::{Deduplicatable, DeduplicatorBackend};
use dashmap::{DashMap, DashSet};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
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

#[derive(Debug)]
pub struct Deduplicator<T, B>
where
    T: Deduplicatable + Send + Sync,
    B: DeduplicatorBackend,
{
    backend: B,
    _marker: std::marker::PhantomData<T>,
}

// 只要求 B 可 Clone
// only require B can Clone
impl<T, B> Clone for Deduplicator<T, B>
where
    T: Deduplicatable + Send + Sync, // 继承结构体约束
    B: DeduplicatorBackend + Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, B> Deduplicator<T, B>
where
    T: Deduplicatable + Send + Sync + 'static,
    B: DeduplicatorBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            _marker: std::marker::PhantomData,
        }
    }
    /// 去重单条（实时流场景）
    /// Deduplicate a single record (real-time streaming scenario)
    pub fn dedup_one(&self, record: T) -> Option<T> {
        let key = record.unique_key();
        let ts = record.timestamp();
        if self.backend.insert(key, ts) {
            Some(record)
        } else {
            None
        }
    }

    /// 批量去重（历史数据批量场景）
    /// Deduplicate a batch of records (historical data batch scenario)
    pub fn deduplicate(&self, records: Vec<T>) -> Vec<T> {
        let mut result = Vec::with_capacity(records.len());
        for record in records {
            if let Some(rec) = self.dedup_one(record) {
                result.push(rec);
            }
        }
        result
    }

    /// 对流式数据去重（直接接到 HistoricalFetcher::stream_xxx 输出）
    /// Deduplicate streaming data (directly received from HistoricalFetcher::stream_xxx output)
    /// 异步流去重
    pub fn deduplicate_stream<S>(self: Arc<Self>, stream: S) -> BoxStream<'static, T>
    where
        S: futures_util::stream::Stream<Item = T> + Send + 'static,
    {
        stream
            .filter_map(move |record| {
                let dedup = Arc::clone(&self);
                async move {
                    // dedup_one 返回 Option<T>
                    dedup.dedup_one(record)
                }
            })
            .boxed()
    }

    pub fn reset(&self) {
        self.backend.reset();
    }

    pub fn gc(&self, now: i64) {
        self.backend.gc(now);
    }
}

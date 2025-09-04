use crate::ingestor::dedup::{Deduplicatable, DeduplicatorBackend};
use dashmap::{DashMap, DashSet};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use std::sync::Arc;

/// Deduplicator 支持历史/实时/统一去重模式
/// dedup supports historical/realtime/unified deduplication mode
#[derive(Clone, Copy, Debug)]
pub enum DedupMode {
    /// 历史批量回补模式
    /// - 使用 bulk_insert / deduplicate 批量去重
    /// - 更新 historical backend，实时 backend 不变
    Historical,

    /// 实时流式模式
    /// - 使用 dedup_one / deduplicate_stream 单条去重
    /// - 更新 realtime backend，历史 backend 不变
    Realtime,

    /// 历史 + 实时统一模式
    /// - 同时更新 historical + realtime backend
    /// - 用于历史数据和实时数据混合场景，保证时间重叠去重
    Unified,
}

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
    /// 支持批量插入
    /// Support bulk insertion
    pub fn bulk_insert(&self, keys: &[Arc<str>]) -> Vec<bool> {
        keys.iter().map(|k| self.seen.insert(k.clone())).collect()
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

    /// 支持批量插入
    /// Support bulk insertion
    pub fn bulk_insert(&self, keys_ts: &[(Arc<str>, i64)]) -> Vec<bool> {
        keys_ts
            .iter()
            .map(|(k, ts)| match self.seen.insert(k.clone(), *ts) {
                None => true,
                Some(prev) if *ts > prev => true,
                _ => false,
            })
            .collect()
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

/// Deduplicator 统一结构多模式支持
/// Deduplicator supports multiple modes
#[derive(Debug, Clone)]
pub struct Deduplicator<T>
where
    T: Deduplicatable + Send + Sync + Clone,
{
    historical: DashSetBackend,
    realtime: DashMapBackend,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Deduplicator<T>
where
    T: Deduplicatable + Send + Sync + 'static + Clone,
{
    pub fn new(ttl_ms: i64) -> Self {
        Self {
            historical: DashSetBackend::new(),
            realtime: DashMapBackend::new(ttl_ms),
            _marker: std::marker::PhantomData,
        }
    }

    /// 单条去重
    /// Deduplicate a single record
    pub fn dedup_one(&self, record: T, mode: DedupMode) -> Option<T> {
        let key = record.unique_key();
        let ts = record.timestamp();
        let inserted = match mode {
            DedupMode::Historical => self.historical.insert(key, ts),
            DedupMode::Realtime => self.realtime.insert(key, ts),
            DedupMode::Unified => {
                let hist = self.historical.insert(key.clone(), ts);
                let realtime = self.realtime.insert(key, ts);
                hist || realtime
            }
        };
        if inserted { Some(record) } else { None }
    }

    /// 批量去重（统一模式使用 bulk 插入双 backend）
    /// Deduplicate a batch of records (unified mode uses bulk insert on both backends)
    pub fn deduplicate(&self, records: Vec<T>, mode: DedupMode) -> Vec<T> {
        match mode {
            DedupMode::Historical => {
                let keys: Vec<_> = records.iter().map(|r| r.unique_key()).collect();
                let results = self.historical.bulk_insert(&keys);
                records
                    .into_iter()
                    .zip(results)
                    .filter_map(|(r, ok)| if ok { Some(r) } else { None })
                    .collect()
            }
            DedupMode::Realtime => {
                let keys_ts: Vec<_> = records.iter().map(|r| (r.unique_key(), r.timestamp())).collect();
                let results = self.realtime.bulk_insert(&keys_ts);
                records
                    .into_iter()
                    .zip(results)
                    .filter_map(|(r, ok)| if ok { Some(r) } else { None })
                    .collect()
            }
            DedupMode::Unified => {
                // Unified 批量插入双 backend
                let keys: Vec<_> = records.iter().map(|r| r.unique_key()).collect();
                let keys_ts: Vec<_> = records.iter().map(|r| (r.unique_key(), r.timestamp())).collect();

                let results_hist = self.historical.bulk_insert(&keys);
                let results_realtime = self.realtime.bulk_insert(&keys_ts);

                records
                    .into_iter()
                    .zip(results_hist.into_iter().zip(results_realtime))
                    .filter_map(|(r, (h, rt))| if h || rt { Some(r) } else { None })
                    .collect()
            }
        }
    }

    /// 流式去重
    /// Deduplicate streaming data
    pub fn deduplicate_stream<S>(self: Arc<Self>, stream: S, mode: DedupMode) -> BoxStream<'static, T>
    where
        S: Stream<Item = T> + Send + 'static,
    {
        stream
            .filter_map(move |record| {
                let dedup = Arc::clone(&self);
                async move { dedup.dedup_one(record, mode) }
            })
            .boxed()
    }

    pub fn reset(&self) {
        self.historical.reset();
        self.realtime.reset();
    }

    pub fn gc(&self, now: i64) {
        self.realtime.gc(now);
    }
}

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub mod data_buffer;

#[derive(Debug, Default)]
pub struct BufferMetrics {
    /// 入队总数
    pub pushed: AtomicUsize,
    /// 批量处理总数（pop_batch 调用次数）
    pub batches_processed: AtomicUsize,
    /// 处理的记录总数
    pub records_processed: AtomicUsize,
    /// 重复记录数量（deduped 被过滤掉的数量）
    pub duplicates: AtomicUsize,
    /// 最近一次批量 flush 延迟（ms）
    pub last_flush_ms: AtomicU64,
}

impl BufferMetrics {
    pub fn record_push(&self, n: usize) {
        self.pushed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_batch(&self, records: usize, flush_duration: Duration, duplicates: usize) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
        self.duplicates.fetch_add(duplicates, Ordering::Relaxed);
        self.last_flush_ms.store(flush_duration.as_millis() as u64, Ordering::Relaxed);
    }
}

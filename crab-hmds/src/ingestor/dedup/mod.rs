use std::sync::Arc;

pub mod deduplicator;

/// 定义一个 trait，泛型去重对象必须实现生成唯一 key
/// defining a trait that deduplicatable objects must implement generating unique key
pub trait Deduplicatable {
    /// 返回唯一 key，建议使用 Arc<str> 或组合元组，避免 heap 分配
    /// return unique key, it's recommended to use Arc<str> or a combination of tuples to avoid heap allocation
    fn unique_key(&self) -> Arc<str>;
}

/// insert → 返回 true 表示新 key，false 表示重复
///
/// insert → returns true if new key, false if duplicate
///
/// ts → 方便实时模式下做 TTL 清理
///
/// ts → useful for TTL cleanup in real-time mode
///
/// gc → 定期垃圾回收
///
/// gc → periodically garbage collection
///
pub trait DeduplicatorBackend: Send + Sync + 'static {
    fn insert(&self, key: Arc<str>, ts: i64) -> bool;
    fn reset(&self);
    // 实时模式才需要，历史模式可以空实现
    // only needed in real-time mode, can be empty implementation in historical mode
    fn gc(&self, now: i64);
}
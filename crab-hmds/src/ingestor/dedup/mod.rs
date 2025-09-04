use std::sync::Arc;

pub mod deduplicator;

/// 定义一个 trait，泛型去重对象必须实现生成唯一 key
/// defining a trait that deduplicatable objects must implement generating unique key
pub trait Deduplicatable {
    /// 返回唯一 key，建议使用 Arc<str> 或组合元组，避免 heap 分配
    /// return unique key, it's recommended to use Arc<str> or a combination of tuples to avoid heap allocation
    fn unique_key(&self) -> Arc<str>;
}

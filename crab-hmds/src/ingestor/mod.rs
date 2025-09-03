pub mod buffer;
mod deduplicator;
pub mod historical;
mod normalizer;
mod realtime;
mod scheduler;
pub mod storage;
mod types;

/// 时间范围查询
/// time range query
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

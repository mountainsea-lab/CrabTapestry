pub mod bar_cache;
pub mod crab_time;
pub mod time_frame;

use std::ops::RangeInclusive;

/// 时间范围查询（UTC 毫秒）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

impl TimeRange {
    /// 创建新的时间范围，保证 start <= end
    pub fn new(start: i64, end: i64) -> Self {
        assert!(start <= end, "TimeRange start must <= end");
        Self { start, end }
    }

    /// 判断时间戳是否在范围内
    pub fn contains(&self, ts: i64) -> bool {
        self.start <= ts && ts <= self.end
    }

    /// 转为 RangeInclusive<i64>，方便迭代
    pub fn to_range(&self) -> RangeInclusive<i64> {
        self.start..=self.end
    }

    /// 返回时间跨度（毫秒）
    pub fn duration_ms(&self) -> i64 {
        self.end - self.start + 1
    }

    /// 将时间范围拆分为多个小区间，每个区间长度为 chunk_ms 毫秒
    pub fn split(&self, chunk_ms: i64) -> Vec<TimeRange> {
        assert!(chunk_ms > 0, "chunk_ms must be positive");
        let mut ranges = Vec::new();
        let mut current = self.start;
        while current <= self.end {
            let next = (current + chunk_ms - 1).min(self.end);
            ranges.push(TimeRange::new(current, next));
            current = next + 1;
        }
        ranges
    }

    /// 返回两个时间范围的交集，如果没有交集返回 None
    pub fn intersect(&self, other: &TimeRange) -> Option<TimeRange> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        if start <= end {
            Some(TimeRange::new(start, end))
        } else {
            None
        }
    }

    /// 将时间戳限制在范围内
    pub fn clamp(&self, ts: i64) -> i64 {
        ts.max(self.start).min(self.end)
    }

    /// 判断是否为空区间（start > end）
    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    /// 判断两个区间是否相交
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split() {
        let tr = TimeRange::new(0, 9999);
        let chunks = tr.split(3000);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0], TimeRange::new(0, 2999));
        assert_eq!(chunks[3], TimeRange::new(9000, 9999));
    }

    #[test]
    fn test_intersect() {
        let a = TimeRange::new(0, 100);
        let b = TimeRange::new(50, 150);
        let c = TimeRange::new(200, 300);
        assert_eq!(a.intersect(&b), Some(TimeRange::new(50, 100)));
        assert_eq!(a.intersect(&c), None);
    }

    #[test]
    fn test_clamp() {
        let tr = TimeRange::new(10, 20);
        assert_eq!(tr.clamp(5), 10);
        assert_eq!(tr.clamp(15), 15);
        assert_eq!(tr.clamp(25), 20);
    }

    #[test]
    fn test_overlaps() {
        let a = TimeRange::new(0, 100);
        let b = TimeRange::new(50, 150);
        let c = TimeRange::new(200, 300);
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
    }

    #[test]
    fn test_is_empty() {
        let tr = TimeRange::new(10, 5);
        assert!(tr.is_empty());
        let tr2 = TimeRange::new(0, 0);
        assert!(!tr2.is_empty());
    }
}

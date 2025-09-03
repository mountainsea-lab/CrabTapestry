use std::ops::RangeInclusive;

/// 时间范围查询
/// time range query
#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start: i64, // UTC 毫秒时间戳
    pub end: i64,
}

impl TimeRange {
    pub fn new(start: i64, end: i64) -> Self {
        assert!(start <= end, "TimeRange start must <= end");
        Self { start, end }
    }

    pub fn contains(&self, ts: i64) -> bool {
        self.start <= ts && ts <= self.end
    }

    pub fn to_range(&self) -> RangeInclusive<i64> {
        self.start..=self.end
    }
}

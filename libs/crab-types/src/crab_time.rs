use chrono::{DateTime, TimeZone, Utc};

pub trait MillisToUtc {
    fn to_utc(self) -> Option<DateTime<Utc>>;
}

impl MillisToUtc for i64 {
    fn to_utc(self) -> Option<DateTime<Utc>> {
        let secs = self / 1000;
        let nsecs = ((self % 1000) * 1_000_000) as u32;

        // chrono::Utc.timestamp_opt(secs, nsecs) 返回 LocalResult
        match Utc.timestamp_opt(secs, nsecs) {
            chrono::LocalResult::Single(dt) => Some(dt),
            _ => None, // Ambiguous/None 都返回 None
        }
    }
}

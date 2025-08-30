use ms_tracing::tracing_utils::internal::warn;
use std::ops::Add;
use time::OffsetDateTime;

pub fn milliseconds_to_offsetdatetime(milliseconds: i64) -> OffsetDateTime {
    let seconds = milliseconds / 1000; // 计算秒部分
    let nanoseconds = (milliseconds % 1000) * 1_000_000; // 计算纳秒部分

    // 直接使用 from_unix_timestamp 创建 OffsetDateTime，并加上 nanoseconds
    OffsetDateTime::from_unix_timestamp(seconds)
        .unwrap_or_else(|_| {
            warn!("Invalid timestamp: {}", milliseconds);
            OffsetDateTime::UNIX_EPOCH
        })
        .add(time::Duration::nanoseconds(nanoseconds))
}

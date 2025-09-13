use crab_types::time_frame::TimeFrame;
use ms_tracing::tracing_utils::internal::warn;
use std::ops::Add;
use std::str::FromStr;
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

/// 将 K 线周期字符串转换为秒数
pub fn parse_period_to_secs(period: &str) -> Option<u64> {
    match TimeFrame::from_str(period) {
        Ok(tf) => Some((tf.to_millis() / 1000) as u64), // 转换成秒
        Err(_) => None,
    }
}

/// 将 K 线周期字符串转换为毫秒数
pub fn parse_period_to_millis(period: &str) -> Option<u64> {
    match TimeFrame::from_str(period) {
        Ok(tf) => Some(tf.to_millis() as u64), // 转换成秒
        Err(_) => None,
    }
}

use crate::domain::model::market_fill_range::NewHmdsMarketFillRange;
use crab_infras::config::sub_config::Subscription;
use crab_types::time_frame::TimeFrame;
use std::str::FromStr;

/// 生成区间
pub fn generate_fill_ranges(
    sub: &Subscription,
    start_ts: i64,
    end_ts: i64,
    max_count: u64,
    period_str: &str,
) -> Vec<NewHmdsMarketFillRange> {
    let mut ranges = Vec::new();

    if let Ok(tf) = TimeFrame::from_str(period_str) {
        let period_ms = tf.to_millis();
        let max_span = period_ms * max_count as i64;

        let mut start_time = start_ts - start_ts % period_ms;

        while start_time < end_ts {
            let mut end_time = (start_time + max_span).min(end_ts);

            // 对齐周期
            let remainder = (end_time - start_time) % period_ms;
            if remainder != 0 {
                end_time -= remainder;
            }

            if end_time <= start_time {
                break;
            }

            ranges.push(NewHmdsMarketFillRange {
                exchange: sub.exchange.to_string(),
                symbol: sub.symbol.to_string(),
                period: period_str.to_string(),
                start_time,
                end_time,
                status: 0,
                retry_count: 0,
                last_try_time: None,
            });

            start_time = end_time;
        }
    }

    ranges
}

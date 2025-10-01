use crate::domain::model::market_fill_range::NewHmdsMarketFillRange;
use crab_infras::config::sub_config::Subscription;
use crab_types::time_frame::TimeFrame;
use ms_tracing::tracing_utils::internal::{debug, info, warn};
use std::str::FromStr;

/// 生成填充区间 - 支持初始化情况和增量情况
pub fn generate_fill_ranges_full(
    sub: &Subscription,
    existing_start_ts: i64,
    existing_end_ts: i64,
    lookback_ts: i64,
    now_ts: i64,
    max_count: u64,
    period_str: &str,
) -> Vec<NewHmdsMarketFillRange> {
    let mut ranges = Vec::new();

    if let Ok(tf) = TimeFrame::from_str(period_str) {
        let period_ms = tf.to_millis();
        let max_span = period_ms * max_count as i64;

        // 情况1: 初始化 - 没有任何记录（通过比较判断）
        let is_initial = existing_start_ts == existing_end_ts && existing_end_ts == lookback_ts;

        if is_initial {
            // 初始化：从 lookback_ts 到 now_ts 生成完整区间
            debug!(
                "Initializing fill ranges for {}/{}/{} from {} to {}",
                sub.exchange, sub.symbol, period_str, lookback_ts, now_ts
            );
            ranges.extend(generate_ranges_for_span(
                sub,
                period_str,
                lookback_ts,
                now_ts,
                max_span,
                period_ms,
            ));
        } else {
            // 情况2: 已有记录，需要检查两个部分

            // 2.1 回溯区间：如果配置的 lookback_ts 比现有记录更早，需要补全历史
            if lookback_ts < existing_start_ts {
                debug!(
                    "Generating lookback ranges for {}/{}/{} from {} to {}",
                    sub.exchange, sub.symbol, period_str, lookback_ts, existing_start_ts
                );
                ranges.extend(generate_ranges_for_span(
                    sub,
                    period_str,
                    lookback_ts,
                    existing_start_ts,
                    max_span,
                    period_ms,
                ));
            }

            // 2.2 增量区间：从现有记录的结束时间到当前时间
            // 只有当时间跨度 >= max_span 时才生成新区间
            let incremental_span = now_ts - existing_end_ts;
            if incremental_span >= max_span {
                ranges.extend(generate_ranges_for_span(
                    sub,
                    period_str,
                    existing_end_ts,
                    now_ts,
                    max_span,
                    period_ms,
                ));
            } else {
                debug!(
                    "Skipping incremental range for {}/{}/{}: span {}ms < max_span {}ms, leaving for real-time task",
                    sub.exchange, sub.symbol, period_str, incremental_span, max_span
                );
            }
        }
    }

    info!(
        "Generated {} fill ranges for {}/{}/{}",
        ranges.len(),
        sub.exchange,
        sub.symbol,
        period_str
    );
    ranges
}

/// 为指定的时间跨度生成区间，确保时间正确对齐
fn generate_ranges_for_span(
    sub: &Subscription,
    period_str: &str,
    start_ts: i64,
    end_ts: i64,
    max_span: i64,
    period_ms: i64,
) -> Vec<NewHmdsMarketFillRange> {
    let mut ranges = Vec::new();

    if start_ts >= end_ts {
        warn!("Invalid time span: start_ts {} >= end_ts {}", start_ts, end_ts);
        return ranges;
    }

    // 将开始时间对齐到周期边界
    let aligned_start = align_to_period(start_ts, period_ms, false);
    let mut current_start = aligned_start;

    while current_start < end_ts {
        // 计算当前区间的结束时间
        let mut current_end = current_start + max_span;

        // 确保不超过目标结束时间
        if current_end > end_ts {
            current_end = end_ts;
        }

        // 将结束时间对齐到周期边界（向下对齐）
        let aligned_end = align_to_period(current_end, period_ms, true);

        // 避免无效区间
        if aligned_end <= current_start {
            // 如果对齐后区间无效，尝试使用未对齐的结束时间（但确保不超过end_ts）
            current_end = current_end.min(end_ts);
            if current_end <= current_start {
                break;
            }
        } else {
            current_end = aligned_end;
        }

        // ✅ 校验：区间必须至少包含一个完整周期
        if current_end - current_start >= period_ms {
            ranges.push(NewHmdsMarketFillRange {
                exchange: sub.exchange.to_string(),
                symbol: sub.symbol.to_string(),
                quote: sub.quote.to_string(),
                period: period_str.to_string(),
                start_time: current_start,
                end_time: current_end,
                status: 0,
                retry_count: 0,
                batch_size: 0,
                last_try_time: None,
            });
        } else {
            debug!(
                "Skipped invalid short range: start={} end={} (diff {} < period {})",
                current_start,
                current_end,
                current_end - current_start,
                period_ms
            );
        }
        // 下一个区间的开始时间就是当前区间的结束时间
        current_start = current_end;
    }

    ranges
}

/// 时间对齐函数
/// `floor = true` 表示向下对齐，`floor = false` 表示向上对齐
fn align_to_period(timestamp: i64, period_ms: i64, floor: bool) -> i64 {
    let remainder = timestamp % period_ms;
    if remainder == 0 {
        timestamp
    } else if floor {
        timestamp - remainder // 向下对齐
    } else {
        timestamp + (period_ms - remainder) // 向上对齐
    }
}

/// 检查是否需要生成新区间的工具函数
pub fn should_generate_ranges(
    existing_start_ts: i64,
    existing_end_ts: i64,
    lookback_ts: i64,
    now_ts: i64,
) -> (bool, bool) {
    let needs_lookback = lookback_ts < existing_start_ts;
    let needs_incremental = existing_end_ts < now_ts;
    (needs_lookback, needs_incremental)
}

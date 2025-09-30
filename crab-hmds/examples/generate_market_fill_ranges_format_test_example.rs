use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use crab_hmds::domain::model::market_fill_range::NewHmdsMarketFillRange;
use crab_hmds::domain::service::generate_and_insert_fill_ranges;
use crab_hmds::global::init_global_services;
use crab_types::time_frame::TimeFrame;
use ms_tracing::tracing_utils::internal::info;
use std::env;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 设置临时的 DATABASE_URL 环境变量，包裹在 unsafe 块中
    unsafe {
        env::set_var(
            "DATABASE_URL",
            "mysql://root:root@mysql.infra.orb.local:3306/crabtapestry",
        );
    }

    // 初始化日志
    ms_tracing::setup_tracing();
    // 初始化全局变量
    setup().await;

    Ok(())
}

// 1. 初始化测试数据库和配置
async fn setup() {
    // 初始化全局服务，连接到数据库
    // init global comments domain
    let _ = init_global_services().await;
}

/// 在填充区间生成后添加格式化日志
pub fn log_formatted_ranges(ranges: &[NewHmdsMarketFillRange], period_str: &str) {
    if let Ok(tf) = TimeFrame::from_str(period_str) {
        let period_ms = tf.to_millis();
        let time_ranges: Vec<(i64, i64)> = ranges.iter().map(|r| (r.start_time, r.end_time)).collect();

        let formatted = format_time_ranges_with_count(&time_ranges, period_ms);

        info!("生成的{}周期区间:", period_str);
        for range_str in formatted {
            info!("  {}", range_str);
        }
    }
}

/// 格式化时间戳为东八区时间并计算K线根数
pub fn format_time_ranges_with_count(time_ranges: &[(i64, i64)], period_ms: i64) -> Vec<String> {
    let east_8_offset = FixedOffset::east_opt(8 * 3600).unwrap(); // 东八区偏移
    let mut results = Vec::new();

    for (start_ts, end_ts) in time_ranges {
        // 转换为东八区时间
        let start_dt: DateTime<FixedOffset> =
            Utc.timestamp_millis_opt(*start_ts).unwrap().with_timezone(&east_8_offset);
        let end_dt: DateTime<FixedOffset> = Utc.timestamp_millis_opt(*end_ts).unwrap().with_timezone(&east_8_offset);

        // 计算K线根数（1分钟周期）
        let kline_count = (end_ts - start_ts) / period_ms;

        // 格式化输出
        let formatted = format!(
            "{}\t{}\t{}根",
            start_dt.format("%Y-%m-%d %H:%M:%S"),
            end_dt.format("%Y-%m-%d %H:%M:%S"),
            kline_count
        );

        results.push(formatted);
    }

    results
}

/// 检查区间连续性
fn check_continuity(time_ranges: &[(i64, i64)]) {
    println!("\n区间连续性检查:");

    for i in 0..time_ranges.len() {
        if i > 0 {
            let prev_end = time_ranges[i - 1].1;
            let curr_start = time_ranges[i].0;

            if prev_end == curr_start {
                println!("区间 {} -> {}: 连续 ✓", i - 1, i);
            } else {
                println!("区间 {} -> {}: 不连续 ✗ (间隙: {}ms)", i - 1, i, curr_start - prev_end);
            }
        }
    }
}

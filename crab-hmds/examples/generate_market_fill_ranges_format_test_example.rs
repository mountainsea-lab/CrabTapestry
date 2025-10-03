use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use crab_hmds::domain::model::market_fill_range::HmdsMarketFillRange;
use crab_hmds::domain::service::query_fill_range_list;
use crab_hmds::global::init_global_services;
use crab_types::time_frame::TimeFrame;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 设置临时的 DATABASE_URL 环境变量
    unsafe {
        env::set_var(
            "DATABASE_URL",
            "mysql://root:root@mysql.infra.orb.local:3306/crabtapestry",
        );
    }

    // 初始化全局变量
    setup().await;

    // 查询填充区间列表
    let fill_range_list = query_fill_range_list().await?;

    if fill_range_list.is_empty() {
        println!("没有找到填充区间数据");
        return Ok(());
    }

    println!("找到 {} 个填充区间", fill_range_list.len());

    // 按周期分组显示
    let mut grouped_by_period: HashMap<&str, Vec<&HmdsMarketFillRange>> = HashMap::new();

    for range in &fill_range_list {
        grouped_by_period.entry(&range.period).or_insert_with(Vec::new).push(range);
    }

    for (period, ranges) in &grouped_by_period {
        println!("\n{}周期区间详情:", period);
        println!("开始时间\t\t结束时间\t\t\tK线根数\t\t交易对");
        println!("{}", "-".repeat(100));

        // 修正：传递引用切片
        let formatted_ranges = format_time_ranges_with_count(ranges);
        for range_str in formatted_ranges {
            println!("{}", range_str);
        }
    }

    // 创建所有区间的引用集合用于后续函数
    let all_ranges_refs: Vec<&HmdsMarketFillRange> = fill_range_list.iter().collect();

    // 打印统计信息 - 修正：传递引用切片
    print_statistics(&all_ranges_refs);

    // 检查区间连续性 - 修正：传递引用切片
    check_continuity(&all_ranges_refs);

    // 额外分析：显示状态分布
    let status_count: HashMap<i32, usize> =
        fill_range_list
            .iter()
            .map(|r| r.status)
            .fold(HashMap::new(), |mut acc, status| {
                *acc.entry(status as i32).or_insert(0) += 1;
                acc
            });

    println!("\n状态分布:");
    for (status, count) in status_count {
        let status_desc = match status {
            0 => "未处理",
            1 => "处理中",
            2 => "已完成",
            3 => "失败可重试",
            _ => "未知状态",
        };
        println!("  {} ({}): {}个", status_desc, status, count);
    }

    println!("\n验证完成!");
    Ok(())
}

// 初始化测试数据库和配置
async fn setup() {
    // 初始化全局服务，连接到数据库
    let _ = init_global_services().await;
}

/// 格式化时间戳为东八区时间并计算K线根数
pub fn format_time_ranges_with_count(time_ranges: &[&HmdsMarketFillRange]) -> Vec<String> {
    let east_8_offset = FixedOffset::east_opt(8 * 3600).unwrap(); // 东八区偏移
    let mut results = Vec::new();

    for range in time_ranges {
        // 使用 TimeFrame 解析周期
        let period_ms = if let Ok(tf) = TimeFrame::from_str(&range.period) {
            tf.to_millis()
        } else {
            // 如果解析失败，使用默认的1分钟
            eprintln!("Warning: Unknown timeframe '{}', using 1m as default", range.period);
            60 * 1000
        };

        // 转换为东八区时间
        let start_dt: DateTime<FixedOffset> = Utc
            .timestamp_millis_opt(range.start_time)
            .unwrap()
            .with_timezone(&east_8_offset);
        let end_dt: DateTime<FixedOffset> =
            Utc.timestamp_millis_opt(range.end_time).unwrap().with_timezone(&east_8_offset);

        // 计算K线根数
        let kline_count = (range.end_time - range.start_time) / period_ms;

        // 格式化输出
        let formatted = format!(
            "{}\t{}\t{}根\t{}/{}/{}",
            start_dt.format("%Y-%m-%d %H:%M:%S"),
            end_dt.format("%Y-%m-%d %H:%M:%S"),
            kline_count,
            range.exchange,
            range.symbol,
            range.period
        );

        results.push(formatted);
    }

    results
}

/// 检查区间连续性
fn check_continuity(time_ranges: &[&HmdsMarketFillRange]) {
    println!("\n区间连续性检查:");

    // 按交易对和周期分组检查连续性
    let mut grouped: HashMap<(&str, &str, &str), Vec<&HmdsMarketFillRange>> = HashMap::new();

    for range in time_ranges {
        let key = (range.exchange.as_str(), range.symbol.as_str(), range.period.as_str());
        grouped.entry(key).or_insert_with(Vec::new).push(range);
    }

    for ((exchange, symbol, period), ranges) in grouped {
        println!("\n{}/{}/{} 连续性检查:", exchange, symbol, period);
        let mut sorted_ranges = ranges;
        sorted_ranges.sort_by_key(|r| r.start_time);

        // 获取周期毫秒数用于更好的错误信息
        let period_ms = if let Ok(tf) = TimeFrame::from_str(period) {
            tf.to_millis()
        } else {
            60 * 1000
        };

        for i in 0..sorted_ranges.len() {
            if i > 0 {
                let prev_end = sorted_ranges[i - 1].end_time;
                let curr_start = sorted_ranges[i].start_time;

                if prev_end == curr_start {
                    println!("  区间 {} -> {}: 连续 ✓", i - 1, i);
                } else if prev_end < curr_start {
                    let gap_periods = (curr_start - prev_end) / period_ms;
                    println!(
                        "  区间 {} -> {}: 有间隙 ✗ (间隙: {}ms, 约{}个周期)",
                        i - 1,
                        i,
                        curr_start - prev_end,
                        gap_periods
                    );
                } else {
                    let overlap_periods = (prev_end - curr_start) / period_ms;
                    println!(
                        "  区间 {} -> {}: 有重叠 ✗ (重叠: {}ms, 约{}个周期)",
                        i - 1,
                        i,
                        prev_end - curr_start,
                        overlap_periods
                    );
                }
            }
        }
    }
}

/// 统计信息
fn print_statistics(time_ranges: &[&HmdsMarketFillRange]) {
    println!("\n统计信息:");

    // 按周期分组统计
    let mut period_stats: HashMap<String, (usize, i64)> = HashMap::new();
    let mut exchange_stats: HashMap<&str, usize> = HashMap::new();
    let mut symbol_stats: HashMap<&str, usize> = HashMap::new();

    for range in time_ranges {
        // 使用 TimeFrame 计算周期毫秒数
        let period_ms = if let Ok(tf) = TimeFrame::from_str(&range.period) {
            tf.to_millis()
        } else {
            eprintln!("Warning: Unknown timeframe '{}'", range.period);
            continue;
        };

        let kline_count = (range.end_time - range.start_time) / period_ms;
        let entry = period_stats.entry(range.period.clone()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += kline_count;

        // 交易所统计
        *exchange_stats.entry(&range.exchange).or_insert(0) += 1;

        // 交易对统计
        *symbol_stats.entry(&range.symbol).or_insert(0) += 1;
    }

    println!("按周期统计:");
    for (period, (count, total_bars)) in &period_stats {
        println!("  {}: {}个区间, 总{}根K线", period, count, total_bars);
    }

    println!("按交易所统计:");
    for (exchange, count) in &exchange_stats {
        println!("  {}: {}个区间", exchange, count);
    }

    println!("按交易对统计:");
    for (symbol, count) in &symbol_stats {
        println!("  {}: {}个区间", symbol, count);
    }

    println!("总区间数: {}个", time_ranges.len());

    // 计算总K线根数
    let total_bars: i64 = period_stats.values().map(|(_, bars)| bars).sum();
    println!("总K线根数: {}根", total_bars);
}

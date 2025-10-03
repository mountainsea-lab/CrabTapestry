use crab_hmds::domain::model::AppError;
use crab_hmds::domain::model::ohlcv_record::{HmdsOhlcvRecord, OhlcvFilter};
use crab_hmds::domain::service::query_ohlcv_list;
use crab_hmds::global::init_global_services;
use crab_hmds::load_app_config;
use crab_types::time_frame::TimeFrame;
use dotenvy::dotenv;
use ms_tracing::tracing_utils::internal::info;
use std::env;
use std::str::FromStr;

/// ==============================
///  测试k线数据连续性
/// ==============================
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 设置临时的 DATABASE_URL 环境变量，包裹在 unsafe 块中
    unsafe {
        env::set_var(
            "DATABASE_URL",
            // "mysql://root:root@mysql.infra.orb.local:3306/crabtapestry",
            "mysql://root:root@113.44.153.48:3306/crabtapestry",
        );
    }

    // 初始化日志
    ms_tracing::setup_tracing();
    // 初始化全局变量
    setup().await;

    // 定义过滤器
    let ohlcv_filter = OhlcvFilter {
        exchange: Some("BinanceFuturesUsd".to_string()),
        symbol: Some("ETHUSDT".to_string()),
        period: Some("1m".to_string()),
        close_time: None,
        sort_by_close_time: None,
        page: None,
        page_size: None,
    };

    // 调用查询函数
    let result = query_ohlcv_list(ohlcv_filter).await.expect("Failed to query OHLCV list");

    // 验证查询结果
    info!("len {:?}", result.len());
    // info!("len {:?}", result.iter().take(5));
    // 在此处可以进行数据连续性的验证
    match validate_kline_continuity(&result[..50]) {
        Ok(_) => println!("All Klines are continuous."),
        Err(e) => {
            if let AppError::InvalidInput(msg) = e {
                println!("{}", msg);
            }
        }
    }

    Ok(())
}

// 1. 初始化测试数据库和配置
async fn setup() {
    // 初始化全局服务，连接到数据库
    // init global comments domain
    let _ = init_global_services().await;
}

#[derive(Debug)]
pub struct Discontinuity {
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub expected_period: i64,
}

// 允许的最大容差（毫秒）
const TOLERANCE: i64 = 1000; // 允许的最大误差是 1000 毫秒，即 1 秒

fn validate_kline_continuity(records: &[HmdsOhlcvRecord]) -> Result<Vec<Discontinuity>, AppError> {
    let mut discontinuities = Vec::new();

    // 遍历记录，检查时间戳连续性
    for i in 1..records.len() {
        let prev_record = &records[i - 1];
        let current_record = &records[i];

        // 根据周期（TimeFrame）获取期望的时间差（毫秒）
        let time_frame = match TimeFrame::from_str(&current_record.period) {
            Ok(tf) => tf,
            Err(_) => {
                return Err(AppError::InvalidInput(format!(
                    "Invalid TimeFrame: {}",
                    current_record.period
                )));
            }
        };

        let expected_timestamp = prev_record.ts + time_frame.to_millis();

        // 如果当前 K 线的时间戳与预期的时间戳不匹配，但误差在容差范围内，则允许
        if (current_record.ts - expected_timestamp).abs() > TOLERANCE {
            discontinuities.push(Discontinuity {
                start_timestamp: prev_record.ts,
                end_timestamp: current_record.ts,
                expected_period: time_frame.to_millis(),
            });
        }
    }

    // 如果没有不连续的区间，返回 Ok(())，否则返回所有不连续区间
    if discontinuities.is_empty() {
        Ok(Vec::new())
    } else {
        Err(AppError::InvalidInput(format!(
            "Kline timestamps are not continuous. Found {} discontinuities. Details:\n{}",
            discontinuities.len(),
            discontinuities
                .iter()
                .map(|discontinuity| {
                    let gap = discontinuity.end_timestamp - discontinuity.start_timestamp;
                    format!(
                        "Discontinuity from {} to {} (Expected period: {} ms). Gap: {} ms",
                        discontinuity.start_timestamp, discontinuity.end_timestamp, discontinuity.expected_period, gap
                    )
                })
                .collect::<Vec<String>>()
                .join("\n")
        )))
    }
}

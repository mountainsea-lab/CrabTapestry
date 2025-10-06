use chrono::NaiveDateTime;
use crab_types::time_frame::TimeFrame;
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ta4r::bar::base_bar::BaseBar;
use ta4r::num::decimal_num::DecimalNum;
use time::{Duration, OffsetDateTime};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OhlcvRecord {
    pub id: u64,                           // 自增主键 (BIGINT UNSIGNED)
    pub hash_id: Vec<u8>,                  // 幂等校验用的 MD5 哈希 (16字节)
    pub ts: i64,                           // K线结束时间戳
    pub period_start_ts: Option<i64>,      // K线开始时间戳 (可选)
    pub symbol: String,                    // 交易对 (BTC/USDT 等)
    pub exchange: String,                  // 交易所名称 (binance 等)
    pub period: String,                    // K线周期 (1m, 5m, 1h 等)
    pub open: f64,                         // 开盘价
    pub high: f64,                         // 最高价
    pub low: f64,                          // 最低价
    pub close: f64,                        // 收盘价
    pub volume: f64,                       // 成交量
    pub turnover: Option<f64>,             // 成交额 (可选)
    pub num_trades: Option<u32>,           // 成交笔数 (可选)
    pub vwap: Option<f64>,                 // 成交量加权平均价 (可选)
    pub created_at: Option<NaiveDateTime>, // 记录创建时间
    pub updated_at: Option<NaiveDateTime>, // 记录更新时间
}

/// 单条转换，固定为 DecimalNum
pub fn ohlcv_to_basebar(record: &OhlcvRecord) -> Result<BaseBar<DecimalNum>, String> {
    // 解析 period 字符串
    let period_tf = TimeFrame::from_str(&record.period).map_err(|e| format!("Invalid period: {}", e))?;

    // 毫秒 -> std::time::Duration
    let period_duration = Duration::milliseconds(period_tf.to_millis());

    // f64 -> DecimalNum
    let to_decimal = |v: f64| DecimalNum::from_f64(v);

    let open = to_decimal(record.open);
    let high = to_decimal(record.high);
    let low = to_decimal(record.low);
    let close = to_decimal(record.close);
    let volume = to_decimal(record.volume).unwrap_or(DecimalNum::new(0));
    let amount = record.turnover.map(to_decimal).unwrap_or(DecimalNum::from_f64(0.0));

    // ✅ 修复时间戳：毫秒 -> 秒 begin_time 优先 period_start_ts，否则 end_time - period
    let end_time = to_datetime(record.ts)?;
    let begin_time = match record.period_start_ts {
        Some(ts) => to_datetime(ts)?,
        None => end_time - period_duration,
    };

    Ok(BaseBar {
        time_period: period_duration,
        begin_time,
        end_time,
        open_price: open,
        high_price: high,
        low_price: low,
        close_price: close,
        volume,
        amount,
        trades: record.num_trades.unwrap_or(0) as u64,
    })
}

/// 批量转换
pub fn ohlcv_vec_to_basebars(records: Vec<OhlcvRecord>) -> Result<Vec<BaseBar<DecimalNum>>, String> {
    records.into_iter().map(|r| ohlcv_to_basebar(&r)).collect()
}

/// 安全转换 timestamp（自动识别秒/毫秒）
#[inline]
fn to_datetime(ts: i64) -> Result<OffsetDateTime, String> {
    // 自动识别毫秒时间戳
    let nanos = if ts > 1_000_000_000_000 {
        (ts as i128) * 1_000_000
    } else {
        (ts as i128) * 1_000_000_000
    };
    OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|e| format!("Invalid timestamp: {}", e))
}

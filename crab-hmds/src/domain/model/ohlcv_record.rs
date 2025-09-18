use crate::domain::model::SortOrder;
use chrono::NaiveDateTime;
use diesel::prelude::*;

/// 查询模型：对应表 `crab_ohlcv_record`
///
/// - `id` 为自增主键，只在数据库生成
/// - `hash_id` 是 BINARY(16)，这里映射为 `Vec<u8>`
/// - `created_at` 和 `updated_at` 使用 `chrono::NaiveDateTime`
#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(table_name = crab_ohlcv_record)]
pub struct CrabOhlcvRecord {
    pub id: u64,                      // 自增主键 (BIGINT UNSIGNED)
    pub hash_id: Vec<u8>,             // 幂等校验用的 MD5 哈希 (16字节)
    pub ts: i64,                      // K线结束时间戳
    pub period_start_ts: Option<i64>, // K线开始时间戳 (可选)
    pub symbol: String,               // 交易对 (BTC/USDT 等)
    pub exchange: String,             // 交易所名称 (binance 等)
    pub period: String,               // K线周期 (1m, 5m, 1h 等)
    pub open: f64,                    // 开盘价
    pub high: f64,                    // 最高价
    pub low: f64,                     // 最低价
    pub close: f64,                   // 收盘价
    pub volume: f64,                  // 成交量
    pub turnover: Option<f64>,        // 成交额 (可选)
    pub num_trades: Option<u32>,      // 成交笔数 (可选)
    pub vwap: Option<f64>,            // 成交量加权平均价 (可选)
    pub created_at: NaiveDateTime,    // 记录创建时间
    pub updated_at: NaiveDateTime,    // 记录更新时间
}

/// 插入模型：用于 `insert_into(crab_ohlcv_record)`
///
/// - 不包含 `id`，由数据库自增生成
/// - 不包含 `created_at` 和 `updated_at`，交由 MySQL 默认值自动生成
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = crab_ohlcv_record)]
pub struct NewCrabOhlcvRecord {
    pub hash_id: Vec<u8>,             // 幂等校验用的 MD5 哈希 (16字节)
    pub ts: i64,                      // K线结束时间戳
    pub period_start_ts: Option<i64>, // K线开始时间戳 (可选)
    pub symbol: String,               // 交易对
    pub exchange: String,             // 交易所名称
    pub period: String,               // K线周期
    pub open: f64,                    // 开盘价
    pub high: f64,                    // 最高价
    pub low: f64,                     // 最低价
    pub close: f64,                   // 收盘价
    pub volume: f64,                  // 成交量
    pub turnover: Option<f64>,        // 成交额 (可选)
    pub num_trades: Option<u32>,      // 成交笔数 (可选)
    pub vwap: Option<f64>,            // 成交量加权平均价 (可选)
}

#[derive(Debug, Clone)]
pub struct OhlcvFilter {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub period: Option<String>,
    pub close_time: Option<i64>,
    pub sort_by_close_time: Option<SortOrder>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

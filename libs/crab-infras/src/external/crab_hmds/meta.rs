use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

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

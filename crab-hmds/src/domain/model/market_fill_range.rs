use crate::domain::model::SortOrder;
use crate::schema::hmds_market_fill_range;
use chrono::NaiveDateTime;
use diesel::{AsChangeset, Identifiable, Insertable, Queryable, Selectable};
use serde::{Deserialize, Serialize};

/// 查询模型：对应表 `hmds_market_fill_range`
///
/// - `id` 为自增主键，只在数据库生成
/// - `created_at` 和 `updated_at` 使用 `chrono::NaiveDateTime`
#[derive(Queryable, Selectable, Serialize, Deserialize, Identifiable, Debug, Clone)]
#[diesel(table_name = hmds_market_fill_range)]
pub struct HmdsMarketFillRange {
    pub id: u64,                              // 自增主键 (BIGINT UNSIGNED)
    pub exchange: String,                     // 交易所名称 (binance 等)
    pub symbol: String,                       // 交易对 (BTC/USDT 等)
    pub period: String,                       // K线周期 (1m, 5m, 1h 等)
    pub start_time: i64,                      // 数据区间起始时间戳
    pub end_time: i64,                        // 数据区间结束时间戳
    pub status: i8,                           // 同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败
    pub retry_count: i32,                     // 重试次数
    pub last_try_time: Option<NaiveDateTime>, // 最后一次尝试同步时间
    pub created_at: Option<NaiveDateTime>,    // 记录创建时间
    pub updated_at: Option<NaiveDateTime>,    // 记录更新时间
}

/// 插入模型：用于 `insert_into(hmds_market_fill_range)`
///
/// - 不包含 `id`，由数据库自增生成
/// - 不包含 `created_at` 和 `updated_at`，交由 MySQL 默认值自动生成
#[derive(Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = hmds_market_fill_range)]
pub struct NewHmdsMarketFillRange {
    pub exchange: String,                     // 交易所名称
    pub symbol: String,                       // 交易对
    pub period: String,                       // K线周期
    pub start_time: i64,                      // 数据区间起始时间戳
    pub end_time: i64,                        // 数据区间结束时间戳
    pub status: i8,                           // 同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败
    pub retry_count: i32,                     // 重试次数
    pub last_try_time: Option<NaiveDateTime>, // 最后一次尝试同步时间（可选）
}

/// 更新模型：用于更新 `hmds_market_fill_range` 数据
///
/// - 只包含可更新字段，避免修改唯一标识
/// - 支持部分更新，可选择性设置字段
#[derive(AsChangeset, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = hmds_market_fill_range)]
pub struct UpdateHmdsMarketFillRange {
    pub id: u64,                              // 必须：定位记录
    pub status: Option<i8>,                   // 可选更新同步状态
    pub retry_count: Option<i32>,             // 可选更新重试次数
    pub last_try_time: Option<NaiveDateTime>, // 可选更新最后一次尝试同步时间
}

#[derive(Debug, Clone, Deserialize)]
pub struct FillRangeFilter {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub period: Option<String>,
    pub status: Option<i8>,
    pub retry_count: Option<i32>,
    pub last_try_time: Option<NaiveDateTime>, // 可选更新最后一次尝试同步时间
    pub start_time: Option<SortOrder>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

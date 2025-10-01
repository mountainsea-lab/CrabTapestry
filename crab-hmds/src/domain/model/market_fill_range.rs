use crate::domain::model::SortOrder;
use crate::schema::hmds_market_fill_range;
use chrono::NaiveDateTime;
use diesel::{AsChangeset, Identifiable, Insertable, Queryable, QueryableByName, Selectable, table};
use serde::{Deserialize, Serialize};

/// 查询模型：对应表 `hmds_market_fill_range`
///
/// - `id` 为自增主键，只在数据库生成
/// - `created_at` 和 `updated_at` 使用 `chrono::NaiveDateTime`
#[derive(Queryable, Selectable, Serialize, Deserialize, Identifiable, QueryableByName, Debug, Clone)]
#[diesel(table_name = hmds_market_fill_range)]
pub struct HmdsMarketFillRange {
    pub id: u64,                              // 自增主键 (BIGINT UNSIGNED)
    pub exchange: String,                     // 交易所名称 (binance 等)
    pub symbol: String,                       // 交易对本币 (BTC 等)
    pub quote: String,                        // 交易对结算币种 (USDT 等)
    pub period: String,                       // K线周期 (1m, 5m, 1h 等)
    pub start_time: i64,                      // 数据区间起始时间戳
    pub end_time: i64,                        // 数据区间结束时间戳
    pub status: i8,                           // 同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败
    pub retry_count: i32,                     // 重试次数
    pub batch_size: i32,                      // 每批次最大数据量
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
    pub quote: String,                        // 交易对结算币种 (USDT 等)
    pub period: String,                       // K线周期
    pub start_time: i64,                      // 数据区间起始时间戳
    pub end_time: i64,                        // 数据区间结束时间戳
    pub status: i8,                           // 同步状态，0: 未同步, 1: 同步中, 2: 已同步, 3: 同步失败
    pub retry_count: i32,                     // 重试次数
    pub batch_size: i32,                      // 每批次最大数据量
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
    pub end_time: i64,                        // 数据区间结束时间戳
    pub retry_count: Option<i32>,             // 可选更新重试次数
    pub last_try_time: Option<NaiveDateTime>, // 可选更新最后一次尝试同步时间
}

/// Upsert 模型：兼容新增和更新
///
/// - `id = None` 表示新增
/// - `id = Some(id)` 表示更新
#[derive(AsChangeset, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = hmds_market_fill_range)]
pub struct UpsertHmdsMarketFillRange {
    pub id: Option<u64>,                      // 可选：更新时必填，新增时 None
    pub exchange: Option<String>,             // 新增时必填，更新时可选
    pub symbol: Option<String>,               // 新增时必填，更新时可选
    pub quote: Option<String>,                // 新增时必填，更新时可选
    pub period: Option<String>,               // 新增时必填，更新时可选
    pub start_time: Option<i64>,              // 新增时必填，更新时可选
    pub end_time: Option<i64>,                // 新增/更新均可
    pub status: Option<i8>,                   // 同步状态
    pub retry_count: Option<i32>,             // 重试次数
    pub batch_size: Option<i32>,              // 每批次最大数据量
    pub last_try_time: Option<NaiveDateTime>, // 最后一次尝试同步时间
}

/// 转换：新增模型 -> Upsert
impl From<NewHmdsMarketFillRange> for UpsertHmdsMarketFillRange {
    fn from(new: NewHmdsMarketFillRange) -> Self {
        UpsertHmdsMarketFillRange {
            id: None,
            exchange: Some(new.exchange),
            symbol: Some(new.symbol),
            quote: Some(new.quote),
            period: Some(new.period),
            start_time: Some(new.start_time),
            end_time: Some(new.end_time),
            status: Some(new.status),
            retry_count: Some(new.retry_count),
            batch_size: Some(new.batch_size),
            last_try_time: new.last_try_time,
        }
    }
}

/// 转换：更新模型 -> Upsert
impl From<UpdateHmdsMarketFillRange> for UpsertHmdsMarketFillRange {
    fn from(upd: UpdateHmdsMarketFillRange) -> Self {
        UpsertHmdsMarketFillRange {
            id: Some(upd.id),
            exchange: None,
            symbol: None,
            quote: None,
            period: None,
            start_time: None,
            end_time: Some(upd.end_time),
            status: upd.status,
            retry_count: upd.retry_count,
            batch_size: None,
            last_try_time: upd.last_try_time,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillRangeStatus {
    UnSynced = 0, // 未同步
    Syncing = 1,  // 同步中
    Synced = 2,   // 已同步
    Failed = 3,   // 同步失败
}

impl Default for FillRangeStatus {
    fn default() -> Self {
        FillRangeStatus::UnSynced
    }
}

impl FillRangeStatus {
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(FillRangeStatus::UnSynced),
            1 => Some(FillRangeStatus::Syncing),
            2 => Some(FillRangeStatus::Synced),
            3 => Some(FillRangeStatus::Failed),
            _ => None,
        }
    }

    pub fn as_i8(self) -> i8 {
        self as i8
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FillRangeFilter {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub period: Option<String>,
    pub status: Option<i8>,
    pub retry_count: Option<i32>,
    pub last_try_time: Option<NaiveDateTime>, // 可选更新最后一次尝试同步时间
    pub sort_by_start_time: Option<SortOrder>,
    pub limit: Option<i32>, // 限制查询条数 默认100条(每次处理100个归档区间)
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

// 伪装成一张表，为了获取最新插入的自动自增的ID （MySQL特有）
table! {
    sequences(id) {
        id -> BigInt,
    }
}

// 用于获取id
#[derive(QueryableByName)]
#[diesel(table_name = sequences)]
pub struct Sequence {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub id: i64,
}

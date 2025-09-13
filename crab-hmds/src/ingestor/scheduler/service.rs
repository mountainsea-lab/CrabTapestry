pub mod HistoricalBackfillService;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// 交易所/币种/周期的唯一键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketKey {
    pub exchange: String,
    pub symbol: String,
    pub interval: String, // e.g. "1m", "5m", "1h"
}

/// 每个市场的回补元信息
#[derive(Debug, Clone)]
pub struct BackfillMeta {
    /// 最后一次成功回补到的时间（含）
    pub last_filled: Option<DateTime<Utc>>,
    /// 最近一次完整维护的时间（一般是定期检查时更新）
    pub last_checked: Option<DateTime<Utc>>,
    /// 是否标记有缺口
    pub missing_ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

/// 存储抽象
#[async_trait]
pub trait BackfillMetaStore: Send + Sync {
    /// 获取某个市场的回补状态
    async fn get_meta(&self, key: &MarketKey) -> anyhow::Result<Option<BackfillMeta>>;

    /// 批量获取
    async fn get_metas(&self, keys: &[MarketKey]) -> anyhow::Result<HashMap<MarketKey, BackfillMeta>>;

    /// 更新某个市场的最后回补时间
    async fn update_last_filled(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()>;

    /// 更新缺口区间
    async fn update_missing_ranges(
        &self,
        key: &MarketKey,
        ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> anyhow::Result<()>;

    /// 标记一次完整检查完成
    async fn update_last_checked(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()>;
}

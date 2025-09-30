pub mod back_fill_job;
// pub mod historical_backfill_service_update;
pub mod historical_backfill_service;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::borrow::Borrow;
use std::collections::HashMap;
use tokio::sync::RwLock;

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
    /// 缺口管理：覆盖式更新
    async fn set_missing_ranges(
        &self,
        key: &MarketKey,
        ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> anyhow::Result<()>;

    /// 标记一次完整检查完成
    async fn update_last_checked(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()>;

    /// 原子更新 last_filled + missing_ranges
    async fn update_meta(
        &self,
        key: &MarketKey,
        last_filled: Option<DateTime<Utc>>,
        missing_ranges: Option<Vec<(DateTime<Utc>, DateTime<Utc>)>>,
    ) -> anyhow::Result<()>;
}

pub struct InMemoryBackfillMetaStore {
    inner: RwLock<HashMap<MarketKey, BackfillMeta>>,
}

impl InMemoryBackfillMetaStore {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    fn get_or_insert_meta<K: Borrow<MarketKey>>(
        map: &mut HashMap<MarketKey, BackfillMeta>,
        key: K,
    ) -> &mut BackfillMeta {
        map.entry(key.borrow().clone()).or_insert_with(|| BackfillMeta {
            last_filled: None,
            last_checked: None,
            missing_ranges: vec![],
        })
    }
}

#[async_trait]
impl BackfillMetaStore for InMemoryBackfillMetaStore {
    async fn get_meta(&self, key: &MarketKey) -> anyhow::Result<Option<BackfillMeta>> {
        let map = self.inner.read().await; // await 获取锁
        Ok(map.get(key).cloned())
    }

    async fn get_metas(&self, keys: &[MarketKey]) -> anyhow::Result<HashMap<MarketKey, BackfillMeta>> {
        let map = self.inner.read().await; // await 获取读锁
        let result = keys
            .iter()
            .filter_map(|k| map.get(k).cloned().map(|v| (k.clone(), v)))
            .collect();
        Ok(result)
    }

    async fn update_last_filled(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()> {
        let mut map = self.inner.write().await; // ✅ await 获取写锁
        let meta = Self::get_or_insert_meta(&mut map, key);
        meta.last_filled = Some(ts);
        Ok(())
    }

    async fn set_missing_ranges(
        &self,
        key: &MarketKey,
        ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> anyhow::Result<()> {
        let mut map = self.inner.write().await;
        let meta = Self::get_or_insert_meta(&mut map, key);
        meta.missing_ranges = ranges;
        Ok(())
    }

    async fn update_last_checked(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()> {
        let mut map = self.inner.write().await;
        let meta = Self::get_or_insert_meta(&mut map, key);
        meta.last_checked = Some(ts);
        Ok(())
    }

    /// 原子更新 last_filled + missing_ranges
    async fn update_meta(
        &self,
        key: &MarketKey,
        last_filled: Option<DateTime<Utc>>,
        missing_ranges: Option<Vec<(DateTime<Utc>, DateTime<Utc>)>>,
    ) -> anyhow::Result<()> {
        let mut map = self.inner.write().await;
        let meta = Self::get_or_insert_meta(&mut map, key);
        if let Some(ts) = last_filled {
            meta.last_filled = Some(ts);
        }
        if let Some(ranges) = missing_ranges {
            meta.missing_ranges = ranges;
        }
        Ok(())
    }
}

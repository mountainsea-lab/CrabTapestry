pub mod back_fill_job;
pub mod historical_backfill_service;

use crate::domain::model::market_fill_range::{FillRangeStatus, UpsertHmdsMarketFillRange};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use crab_types::time_frame::TimeFrame;
use dashmap::DashMap;
use std::collections::HashMap;
use std::str::FromStr;

/// 交易所/币种/周期的唯一键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketKey {
    /// 维护区间数据id
    pub range_id: Option<u64>,
    pub exchange: String,
    pub symbol: String,
    pub period: String, // e.g. "1m", "5m", "1h"
}

impl MarketKey {
    fn new(range_id: Option<u64>, exchange: &str, symbol: &str, period: &str) -> MarketKey {
        MarketKey {
            range_id,
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            period: period.to_string(),
        }
    }
}

/// 每个市场的回补元信息
#[derive(Debug, Clone)]
pub struct BackfillMeta {
    /// 维护区间数据id
    pub range_id: Option<u64>,
    /// 交易所/币种/周期的唯一键 这个可以保留
    pub market_key: MarketKey,
    /// 区间开始时间
    pub start_time: DateTime<Utc>,
    /// 最后一次成功回补到的时间（含）
    pub last_filled: Option<DateTime<Utc>>,
    /// 每批次最大数据量
    pub batch_size: i32,
    /// 交易对结算币种 (USDT 等)
    pub quote: String,
}

impl BackfillMeta {
    /// 产生新的维护区间
    fn new_for_next_range(&self, now: DateTime<Utc>) -> Self {
        let period_millis = TimeFrame::from_str(&self.market_key.period)
            .map(|tf| tf.to_millis())
            .unwrap_or(60_000);
        let max_span = period_millis * self.batch_size as i64;

        let new_start_time = self.start_time + Duration::milliseconds(max_span);

        Self {
            range_id: None, // 新插入时 DB 会返回 id
            market_key: self.market_key.clone(),
            start_time: new_start_time,
            last_filled: Some(now),
            batch_size: self.batch_size,
            quote: self.quote.clone(),
        }
    }
    /// 转换为数据库新增/更新结构
    pub fn to_update_synced(&self) -> UpsertHmdsMarketFillRange {
        // 安全解析周期，如果解析失败默认 1 分钟
        let period_millis = TimeFrame::from_str(&self.market_key.period)
            .map(|tf| tf.to_millis())
            .unwrap_or(60_000); // 默认 1 分钟 = 60000 毫秒

        let max_span = period_millis * self.batch_size as i64;

        // 计算 elapsed
        let filled = self.last_filled.unwrap_or(self.start_time);
        let elapsed = (filled - self.start_time).num_milliseconds();

        let status = if elapsed >= max_span {
            FillRangeStatus::Synced.as_i8()
        } else {
            FillRangeStatus::Syncing.as_i8()
        };

        UpsertHmdsMarketFillRange {
            id: self.range_id,
            exchange: Some(self.market_key.exchange.clone()),
            symbol: Some(self.market_key.symbol.clone()),
            quote: Some(self.quote.clone()),
            period: Some(self.market_key.period.clone()),
            status: Some(status),
            end_time: Some(filled.timestamp_millis()),
            retry_count: None,
            batch_size: Some(self.batch_size),
            last_try_time: None,
            start_time: Some(self.start_time.timestamp_millis()),
        }
    }
}

pub struct InMemoryBackfillMetaStore {
    pub inner: DashMap<MarketKey, BackfillMeta>,
}

impl InMemoryBackfillMetaStore {
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// 获取或插入 meta
    pub fn get_or_insert_meta(&self, key: MarketKey) -> BackfillMeta {
        self.inner
            .entry(key.clone())
            .or_insert_with(|| BackfillMeta {
                range_id: None,
                market_key: key,
                start_time: Utc::now(),
                last_filled: None,
                batch_size: 500,
                quote: "USDT".to_string(),
            })
            .clone()
    }

    /// 获取所有市场的 meta 副本
    pub fn get_all_metas(&self) -> HashMap<MarketKey, BackfillMeta> {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// 单条更新 last_filled
    pub fn update_last_filled(&self, key: &MarketKey, ts: DateTime<Utc>) {
        if let Some(mut meta) = self.inner.get_mut(key) {
            meta.last_filled = Some(ts);
        }
    }
}

/// 存储抽象
#[async_trait]
pub trait BackfillMetaStore: Send + Sync {
    /// 新增市场的最后时间
    async fn add_last_filled(&self, key: &MarketKey, fill_meta: BackfillMeta) -> anyhow::Result<()>;

    /// 更新某个市场的最后回补时间
    async fn update_last_filled(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()>;

    /// 获取所有缓存数据
    async fn get_all_metas(&self) -> HashMap<MarketKey, BackfillMeta>;

    /// 获取指定的缓存数据
    async fn get_meta_by_key(&self, key: &MarketKey) -> Option<BackfillMeta>;
}

#[async_trait]
impl BackfillMetaStore for InMemoryBackfillMetaStore {
    async fn add_last_filled(&self, key: &MarketKey, fill_meta: BackfillMeta) -> anyhow::Result<()> {
        self.inner.insert(key.clone(), fill_meta);
        Ok(())
    }

    async fn update_last_filled(&self, key: &MarketKey, ts: DateTime<Utc>) -> anyhow::Result<()> {
        self.inner
            .entry(key.clone())
            .and_modify(|meta| {
                meta.last_filled = Some(ts);
            })
            .or_insert_with(|| BackfillMeta {
                range_id: None,
                market_key: key.clone(),
                start_time: Utc::now(),
                last_filled: Some(ts),
                batch_size: 500,
                quote: "USDT".to_string(),
            });
        Ok(())
    }

    async fn get_all_metas(&self) -> HashMap<MarketKey, BackfillMeta> {
        self.get_all_metas()
    }

    async fn get_meta_by_key(&self, key: &MarketKey) -> Option<BackfillMeta> {
        self.inner.get(key).map(|entry| entry.clone())
    }
}

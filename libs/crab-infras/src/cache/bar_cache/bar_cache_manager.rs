use crate::cache::bar_cache::bar_key::BarKey;
use crate::cache::bar_cache::series_entry::{STATE_LOADING, STATE_READY, STATE_UNINIT, SeriesEntry};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use ta4r::bar::base_bar::BaseBar;
use ta4r::bar::base_bar_series::BaseBarSeries;
use ta4r::bar::base_bar_series_builder::BaseBarSeriesBuilder;
use ta4r::bar::types::{BarSeries, BarSeriesBuilder};
use ta4r::num::decimal_num::DecimalNum;
use tokio::time::timeout;

#[derive(Clone)]
pub struct BarCacheManager {
    caches: Arc<DashMap<BarKey, Arc<SeriesEntry>>>,
    default_capacity: usize,
}

impl BarCacheManager {
    pub fn new(default_capacity: usize) -> Self {
        Self {
            caches: Arc::new(DashMap::new()),
            default_capacity,
        }
    }

    /// 直接初始化 series（同步构建好后插入）
    /// 等同于你原先 `init_series(&self, key, series)` 的实现，但异步安全
    pub async fn init_series(&self, key: BarKey, series: BaseBarSeries<DecimalNum>) {
        let entry = Arc::new(SeriesEntry::new_with_series(series));
        self.caches.insert(key, entry);
    }

    /// 如果 entry 不存在，则插入一个 placeholder（state = Uninit）并返回 Arc<SeriesEntry>
    fn get_or_create_placeholder(&self, key: &BarKey) -> Arc<SeriesEntry> {
        use dashmap::mapref::entry::Entry;
        match self.caches.entry(key.clone()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                let e = Arc::new(SeriesEntry::new_empty());
                v.insert(e.clone());
                e
            }
        }
    }

    /// 等待某序列 ready，带超时
    pub async fn wait_ready(&self, key: &BarKey, dur: Duration) -> Result<(), String> {
        if let Some(entry_ref) = self.caches.get(key) {
            let entry = entry_ref.clone();
            // 快路径
            if entry.is_ready() {
                return Ok(());
            }
            // 等待 notify 或超时
            let f = async {
                while !entry.is_ready() {
                    entry.notify.notified().await;
                }
                Ok::<(), ()>(())
            };
            timeout(dur, f)
                .await
                .map_err(|_| format!("timeout waiting for series {} ready", key.id()))?
                .map_err(|_| "unexpected wait failure".to_string())?;
            Ok(())
        } else {
            Err(format!("series {} not found", key.id()))
        }
    }

    /// 从 HTTP endpoint 加载某个序列（分页/一次性均可），只有第一个调用者会真正执行加载
    /// `fetch_fn`：由调用方提供如何从远端获取条目的异步闭包（支持分页/流式），返回 Vec<BaseBar<DecimalNum>>
    pub async fn ensure_loaded_with_fetch<F, Fut>(&self, key: BarKey, mut fetch_fn: F) -> Result<(), String>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<BaseBar<DecimalNum>>, String>> + Send,
    {
        // 获取或创建 placeholder entry
        let entry = self.get_or_create_placeholder(&key);

        // 只有 state 从 UNINIT -> LOADING 的那个 caller 会成为 loader
        let prev = entry
            .state
            .compare_exchange(STATE_UNINIT, STATE_LOADING, Ordering::SeqCst, Ordering::SeqCst)
            .ok();

        if prev.is_some() {
            // 我是 loader
            // 调用 fetch_fn 拉取所有 bars（可能很大，fetch_fn 可做分页）
            let bars = fetch_fn().await.map_err(|e| format!("fetch error: {}", e))?;

            // 构建 series（在本地构建，避免长时间持写锁）
            let mut builder = BaseBarSeriesBuilder::<DecimalNum>::new().with_name(key.id());
            for bar in bars {
                builder.bars.push(bar);
            }
            let new_series = builder.build().map_err(|e| format!("build error: {:?}", e))?;

            // 写入 entry.series（替换内容）
            {
                let mut w = entry.series.write().await;
                *w = new_series;
            }

            // 标记 Ready 并通知等待者
            entry.state.store(STATE_READY, Ordering::SeqCst);
            entry.notify.notify_waiters();

            Ok(())
        } else {
            // 不是 loader：要么别人已在 loading，要么已 ready
            // 如果已经 ready 直接返回
            if entry.is_ready() {
                return Ok(());
            }
            // 否则等待别人加载完成（给予合理超时）
            let wait_res = timeout(Duration::from_secs(30), async {
                while !entry.is_ready() {
                    entry.notify.notified().await;
                }
            })
            .await;

            match wait_res {
                Ok(_) => Ok(()),
                Err(_) => Err("timeout waiting for loader to finish".into()),
            }
        }
    }

    /// 将新 bar 追加到 series（实时更新）
    pub async fn append_bar(&self, key: &BarKey, bar: BaseBar<DecimalNum>) -> Result<(), String> {
        if let Some(entry_ref) = self.caches.get(key) {
            let entry = entry_ref.clone();
            let mut w = entry.series.write().await;
            w.add_bar(bar);
            Ok(())
        } else {
            Err(format!("series {} not registered", key.id()))
        }
    }

    /// 读取最近 n 根 bar（异步）
    pub async fn get_last_n_bars(&self, key: &BarKey, n: usize) -> Result<Vec<BaseBar<DecimalNum>>, String> {
        if let Some(entry_ref) = self.caches.get(key) {
            let entry = entry_ref.clone();
            let r = entry.series.read().await;
            let len = r.get_bar_count();
            let start = len.saturating_sub(n);
            let mut out = Vec::with_capacity(n.min(len));
            for i in start..len {
                if let Some(b) = r.get_bar(i).cloned() {
                    out.push(b);
                }
            }
            Ok(out)
        } else {
            Err(format!("series {} not found", key.id()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external::crab_hmds::DefaultHmdsExchange;
    use crate::external::crab_hmds::meta::{OhlcvRecord, ohlcv_to_basebar};
    use ms_tracing::setup_tracing;
    use ms_tracing::tracing_utils::internal::info;
    use std::time::Duration;
    use ta4r::num::decimal_num::DecimalNum;

    #[tokio::test]
    async fn test_ensure_loaded_with_fetch_real_dbe() {
        setup_tracing();
        // 初始化缓存管理器
        let manager = BarCacheManager::new(100);
        let key = BarKey::new("BinanceFuturesUsd", "BTCUSDT", "5m");

        // 初始化真实交易所接口
        let dbe = DefaultHmdsExchange::default();
        let exchange = "BinanceFuturesUsd";
        let symbol = "BTCUSDT";
        let period = "1m";
        let limit = 5;

        /// fetch_fn 使用 dbe.get_klines 异步获取数据
        let fetch_fn = move || {
            let dbe = dbe.clone();
            let exchange = exchange.to_string();
            let symbol = symbol.to_string();
            let period = period.to_string();
            let limit = limit;

            async move {
                // 1️⃣ 获取 K 线
                let klines: Vec<OhlcvRecord> = dbe.get_klines(&exchange, &symbol, &period, limit, None, None).await;
                info!("klines {:?}", klines);
                // 2️⃣ 转换成 BaseBar<DecimalNum>
                let mut bars = Vec::with_capacity(klines.len());
                for k in klines {
                    let bar = ohlcv_to_basebar(&k)?;
                    bars.push(bar);
                }

                Ok::<Vec<BaseBar<DecimalNum>>, String>(bars)
            }
        };

        // 确保加载（第一个调用者负责拉取）
        manager.ensure_loaded_with_fetch(key.clone(), fetch_fn).await.unwrap();

        // 等待 series ready
        manager.wait_ready(&key, Duration::from_secs(10)).await.unwrap();

        // 获取最近 3 根 bar
        let last_three = manager.get_last_n_bars(&key, 3).await.unwrap();
        assert!(!last_three.is_empty());

        // 打印日志，便于调试
        for b in last_three {
            println!(
                "Close: {:?}, High: {:?}, Low: {:?}, Open: {:?}",
                b.close_price, b.high_price, b.low_price, b.open_price
            );
        }
    }
}

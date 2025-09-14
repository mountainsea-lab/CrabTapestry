use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::BackfillDataType;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::scheduler::service::back_fill_job::{BackfillJob, BackfillPriority};
use crate::ingestor::scheduler::service::{BackfillMeta, BackfillMetaStore, MarketKey};
use crate::ingestor::types::{FetchContext, HistoricalSource};
use chrono::{DateTime, Duration, MappedLocalTime, TimeZone, Utc};
use crab_infras::config::sub_config::SubscriptionMap;
use ms_tracing::tracing_utils::internal::{error, info};
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, broadcast};
//
// pub struct HistoricalBackfillService<F>
// where
//     F: HistoricalFetcherExt + 'static,
// {
//     scheduler: Arc<BaseBackfillScheduler<F>>,
//     db: Arc<dyn BackfillMetaStore>,
//     default_max_batch_hours: i64,
//     max_retries: usize,
//     job_queue: Arc<Mutex<BinaryHeap<BackfillJob<F>>>>,
// }
//
// impl<F> HistoricalBackfillService<F>
// where
//     F: HistoricalFetcherExt + 'static,
// {
//     pub fn new(
//         scheduler: Arc<BaseBackfillScheduler<F>>,
//         db: Arc<dyn BackfillMetaStore>,
//         default_max_batch_hours: i64,
//         max_retries: usize,
//     ) -> Self {
//         Self {
//             scheduler,
//             db,
//             default_max_batch_hours,
//             max_retries,
//             job_queue: Arc::new(Mutex::new(BinaryHeap::new())),
//         }
//     }
//
//     fn market_key(exchange: &str, symbol: &str, period: &str) -> MarketKey {
//         MarketKey {
//             exchange: exchange.to_string(),
//             symbol: symbol.to_string(),
//             interval: period.to_string(),
//         }
//     }
//
//     fn merge_ranges(ranges: &mut Vec<(DateTime<Utc>, DateTime<Utc>)>) {
//         if ranges.is_empty() { return; }
//         ranges.sort_by_key(|r| r.0);
//         let mut merged = vec![ranges[0]];
//         for &(start, end) in ranges.iter().skip(1) {
//             let last = merged.last_mut().unwrap();
//             if start <= last.1 {
//                 last.1 = std::cmp::max(last.1, end);
//             } else {
//                 merged.push((start, end));
//             }
//         }
//         *ranges = merged;
//     }
//
//     fn compute_batch_hours(&self, source: &HistoricalSource, period_str: &str) -> i64 {
//         let default_hours = self.default_max_batch_hours;
//         let period_ms = match period_str {
//             "1m" => 60_000,
//             "5m" => 5 * 60_000,
//             "15m" => 15 * 60_000,
//             "30m" => 30 * 60_000,
//             "1h" => 60 * 60_000,
//             "4h" => 4 * 60 * 60_000,
//             "1d" => 24 * 60 * 60_000,
//             _ => 60_000,
//         };
//         let max_batch_millis = source.batch_size as i64 * period_ms;
//         let max_batch_hours = max_batch_millis / (60 * 60 * 1000);
//         std::cmp::max(1, std::cmp::min(default_hours, max_batch_hours))
//     }
//
//     async fn enqueue_job(&self, job: BackfillJob<F>) {
//         let mut q = self.job_queue.lock().await;
//         q.push(job);
//     }
//
//     async fn worker(&self) {
//         loop {
//             let job_opt = {
//                 let mut q = self.job_queue.lock().await;
//                 q.pop()
//             };
//
//             let job = match job_opt {
//                 Some(j) => j,
//                 None => {
//                     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//                     continue;
//                 }
//             };
//
//             let mut attempts = 0;
//             while attempts < self.max_retries {
//                 attempts += 1;
//                 match job.scheduler.add_batch_tasks(job.ctx.clone(), job.data_type.clone(), job.step_millis, vec![]).await {
//                     Ok(_) => {
//                         if let Some(dt) = match Utc.timestamp_millis_opt(job.ctx.range.end) {
//                             MappedLocalTime::Single(dt) => Some(dt),
//                             _ => None,
//                         } {
//                             if let Err(e) = job.db.update_last_filled(&job.key, dt).await {
//                                 error!("Failed to update last_filled: {:?}", e);
//                             }
//                         } else {
//                             error!("Timestamp out-of-range for last_filled: {}", job.ctx.range.end);
//                         }
//                         break;
//                     }
//                     Err(e) => {
//                         error!("Job failed (attempt {}/{}): {:?}", attempts, self.max_retries, e);
//                         tokio::time::sleep(std::time::Duration::from_millis(200)).await;
//                     }
//                 }
//             }
//         }
//     }
//
//     pub fn run(self: Arc<Self>, worker_count: usize) {
//         let svc = self.clone();
//         task::spawn(async move {
//             svc.worker().await;
//         });
//     }
//
//     async fn schedule_backfill(
//         &self,
//         ctx: Arc<FetchContext>,
//         data_type: BackfillDataType,
//         key: MarketKey,
//         priority: BackfillPriority,
//         step_millis: i64,
//     ) {
//         let job = BackfillJob {
//             ctx,
//             data_type,
//             priority,
//             retries: 0,
//             scheduler: self.scheduler.clone(),
//             db: self.db.clone(),
//             key,
//             step_millis,
//         };
//         self.enqueue_job(job).await;
//     }
//
//     /// 初始化最近数据，优先缺口
//     pub async fn init_recent_tasks(
//         &self,
//         subscriptions: &SubscriptionMap,
//         past_hours: i64,
//         step_millis: i64,
//         data_type: BackfillDataType,
//     ) {
//         for entry in subscriptions.iter() {
//             let (_key, sub) = entry.pair();
//             for period in &sub.periods {
//                 let key = Self::market_key(&sub.exchange, &sub.symbol, period);
//
//                 let last_filled = self.db.get_meta(&key).await.ok().flatten()
//                     .and_then(|meta| meta.last_filled)
//                     .unwrap_or_else(|| Utc::now() - Duration::hours(past_hours));
//
//                 let end_ts = Utc::now();
//                 let mut start_ts = last_filled;
//
//                 while start_ts < end_ts {
//                     let batch_hours = self.compute_batch_hours(&HistoricalSource {
//                         name: "".to_string(),
//                         exchange: sub.exchange.to_string(),
//                         last_success_ts: 0,
//                         last_fetch_ts: 0,
//                         batch_size: 500, // 可改成实际 source.batch_size
//                         supports_tick: true,
//                         supports_trade: true,
//                         supports_ohlcv: true,
//                     }, period);
//
//                     let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);
//
//                     let ctx = Arc::new(FetchContext::new_with_range(
//                         &sub.exchange,
//                         &sub.symbol,
//                         period,
//                         start_ts,
//                         batch_end,
//                     ));
//
//                     self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Lookback, step_millis).await;
//
//                     start_ts = batch_end;
//                 }
//             }
//         }
//     }
//
//     /// 后台回溯历史数据，缺口优先
//     pub async fn backfill_historical(
//         &self,
//         subscriptions: &SubscriptionMap,
//         step_millis: i64,
//         data_type: BackfillDataType,
//         lookback_days: i64,
//     ) {
//         let now = Utc::now();
//         let oldest_allowed = now - Duration::days(lookback_days);
//
//         for entry in subscriptions.iter() {
//             let (_key, sub) = entry.pair();
//             for period in &sub.periods {
//                 let key = Self::market_key(&sub.exchange, &sub.symbol, period);
//
//                 let mut meta = self.db.get_meta(&key).await.unwrap_or(None)
//                     .unwrap_or(BackfillMeta {
//                         last_filled: None,
//                         last_checked: None,
//                         missing_ranges: vec![],
//                     });
//
//                 Self::merge_ranges(&mut meta.missing_ranges);
//
//                 // 1️⃣ 缺口任务
//                 for gap in meta.missing_ranges.iter() {
//                     let mut start_ts = gap.0;
//                     let end_ts = gap.1;
//                     while start_ts < end_ts {
//                         let batch_hours = self.compute_batch_hours(&HistoricalSource {
//                             name: "".to_string(),
//                             exchange: sub.exchange.to_string(),
//                             last_success_ts: 0,
//                             last_fetch_ts: 0,
//                             batch_size: 500,
//                             supports_tick: true,
//                             supports_trade: true,
//                             supports_ohlcv: true,
//                         }, period);
//
//                         let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);
//
//                         let ctx = Arc::new(FetchContext::new_with_range(
//                             &sub.exchange,
//                             &sub.symbol,
//                             period,
//                             start_ts,
//                             batch_end,
//                         ));
//
//                         self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Gap, step_millis).await;
//
//                         start_ts = batch_end;
//                     }
//                 }
//
//                 // 2️⃣ 回溯任务
//                 let mut oldest = meta.last_filled.unwrap_or(now);
//                 while oldest > oldest_allowed {
//                     let start = std::cmp::max(oldest - Duration::days(1), oldest_allowed);
//
//                     let ctx = Arc::new(FetchContext::new_with_range(
//                         &sub.exchange,
//                         &sub.symbol,
//                         period,
//                         start,
//                         oldest,
//                     ));
//
//                     self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Lookback, step_millis).await;
//
//                     oldest = start;
//                 }
//
//                 // 3️⃣ 更新 last_checked
//                 if let Err(e) = self.db.update_last_checked(&key, now).await {
//                     error!("Failed to update last_checked: {:?}", e);
//                 }
//             }
//         }
//     }
// }
//
/// HistoricalBackfillService
pub struct HistoricalBackfillService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    scheduler: Arc<BaseBackfillScheduler<F>>,
    db: Arc<dyn BackfillMetaStore>,
    default_max_batch_hours: i64,
    max_retries: usize,
    job_queue: Arc<Mutex<BinaryHeap<BackfillJob<F>>>>,
}
//
// impl<F> HistoricalBackfillService<F>
// where
//     F: HistoricalFetcherExt + 'static,
// {
//     pub fn new(
//         scheduler: Arc<BaseBackfillScheduler<F>>,
//         db: Arc<dyn BackfillMetaStore>,
//         default_max_batch_hours: i64,
//         max_retries: usize,
//     ) -> Self {
//         Self {
//             scheduler,
//             db,
//             default_max_batch_hours,
//             max_retries,
//             job_queue: Arc::new(Mutex::new(BinaryHeap::new())),
//         }
//     }
//
//     fn market_key(exchange: &str, symbol: &str, period: &str) -> MarketKey {
//         MarketKey {
//             exchange: exchange.to_string(),
//             symbol: symbol.to_string(),
//             interval: period.to_string(),
//         }
//     }
//
//     fn merge_ranges(ranges: &mut Vec<(DateTime<Utc>, DateTime<Utc>)>) {
//         if ranges.is_empty() { return; }
//         ranges.sort_by_key(|r| r.0);
//         let mut merged = vec![ranges[0]];
//         for &(start, end) in ranges.iter().skip(1) {
//             let last = merged.last_mut().unwrap();
//             if start <= last.1 {
//                 last.1 = std::cmp::max(last.1, end);
//             } else {
//                 merged.push((start, end));
//             }
//         }
//         *ranges = merged;
//     }
//
//     fn compute_batch_hours(&self, source: &HistoricalSource, period_str: &str) -> i64 {
//         let default_hours = self.default_max_batch_hours;
//         let period_ms = match period_str {
//             "1m" => 60_000,
//             "5m" => 5 * 60_000,
//             "15m" => 15 * 60_000,
//             "30m" => 30 * 60_000,
//             "1h" => 60 * 60_000,
//             "4h" => 4 * 60 * 60_000,
//             "1d" => 24 * 60 * 60_000,
//             _ => 60_000,
//         };
//         let max_batch_millis = source.batch_size as i64 * period_ms;
//         let max_batch_hours = max_batch_millis / (60 * 60 * 1000);
//         std::cmp::max(1, std::cmp::min(default_hours, max_batch_hours))
//     }
//
//     async fn enqueue_job(&self, job: BackfillJob<F>) {
//         let mut q = self.job_queue.lock().await;
//         q.push(job);
//     }
//
//     /// Worker 循环（优先缺口任务 + 回溯任务），支持优雅退出
//     async fn worker_loop(&self, mut shutdown_rx: broadcast::Receiver<()>) {
//         loop {
//             tokio::select! {
//             _ = shutdown_rx.recv() => {
//                 info!("Worker received shutdown signal, exiting");
//                 break;
//             }
//             else => {
//                 // 执行任务
//                 if let Some(job) = self.next_job().await {
//                     self.execute_job(job).await;
//                 } else {
//                     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//                 }
//             }
//         }
//         }
//     }
//
//
//     /// 启动 worker 并返回 shutdown 通道
//     pub fn start_workers(self: Arc<Self>, worker_count: usize) -> broadcast::Sender<()> {
//         // 使用 broadcast channel 支持多消费者
//         let (shutdown_tx, _) = broadcast::channel::<()>(worker_count);
//
//         for _ in 0..worker_count {
//             let svc = self.clone();
//             let mut shutdown_rx = shutdown_tx.subscribe();
//
//             tokio::spawn(async move {
//                 svc.worker_loop(shutdown_rx).await;
//             });
//         }
//
//         shutdown_tx
//     }
//
//     async fn schedule_backfill(
//         &self,
//         ctx: Arc<FetchContext>,
//         data_type: BackfillDataType,
//         key: MarketKey,
//         priority: BackfillPriority,
//         step_millis: i64,
//     ) {
//         let job = BackfillJob {
//             ctx,
//             data_type,
//             priority,
//             retries: 0,
//             scheduler: self.scheduler.clone(),
//             db: self.db.clone(),
//             key,
//             step_millis,
//         };
//         self.enqueue_job(job).await;
//     }
//
//     /// 初始化最近数据，优先缺口
//     pub async fn init_recent_tasks(
//         &self,
//         subscriptions: &SubscriptionMap,
//         past_hours: i64,
//         step_millis: i64,
//         data_type: BackfillDataType,
//     ) {
//         for entry in subscriptions.iter() {
//             let (_key, sub) = entry.pair();
//             for period in &sub.periods {
//                 let key = Self::market_key(&sub.exchange, &sub.symbol, period);
//
//                 let last_filled = self.db.get_meta(&key).await.ok().flatten()
//                     .and_then(|meta| meta.last_filled)
//                     .unwrap_or_else(|| Utc::now() - Duration::hours(past_hours));
//
//                 let end_ts = Utc::now();
//                 let mut start_ts = last_filled;
//
//                 while start_ts < end_ts {
//                     let batch_hours = self.compute_batch_hours(&HistoricalSource {
//                         name: "".to_string(),
//                         exchange: sub.exchange.to_string(),
//                         last_success_ts: 0,
//                         last_fetch_ts: 0,
//                         batch_size: 500,
//                         supports_tick: true,
//                         supports_trade: true,
//                         supports_ohlcv: true,
//                     }, period);
//
//                     let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);
//
//                     let ctx = Arc::new(FetchContext::new_with_range(
//                         &sub.exchange,
//                         &sub.symbol,
//                         period,
//                         start_ts,
//                         batch_end,
//                     ));
//
//                     self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Lookback, step_millis).await;
//
//                     start_ts = batch_end;
//                 }
//             }
//         }
//     }
//
//     /// 后台回溯历史数据，缺口优先
//     pub async fn backfill_historical(
//         &self,
//         subscriptions: &SubscriptionMap,
//         step_millis: i64,
//         data_type: BackfillDataType,
//         lookback_days: i64,
//     ) {
//         let now = Utc::now();
//         let oldest_allowed = now - Duration::days(lookback_days);
//
//         for entry in subscriptions.iter() {
//             let (_key, sub) = entry.pair();
//             for period in &sub.periods {
//                 let key = Self::market_key(&sub.exchange, &sub.symbol, period);
//
//                 let mut meta = self.db.get_meta(&key).await.unwrap_or(None)
//                     .unwrap_or(BackfillMeta {
//                         last_filled: None,
//                         last_checked: None,
//                         missing_ranges: vec![],
//                     });
//
//                 Self::merge_ranges(&mut meta.missing_ranges);
//
//                 // 缺口任务
//                 for gap in meta.missing_ranges.iter() {
//                     let mut start_ts = gap.0;
//                     let end_ts = gap.1;
//                     while start_ts < end_ts {
//                         let batch_hours = self.compute_batch_hours(&HistoricalSource {
//                             name: "".to_string(),
//                             exchange: sub.exchange.to_string(),
//                             last_success_ts: 0,
//                             last_fetch_ts: 0,
//                             batch_size: 500,
//                             supports_tick: true,
//                             supports_trade: true,
//                             supports_ohlcv: true,
//                         }, period);
//
//                         let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);
//
//                         let ctx = Arc::new(FetchContext::new_with_range(
//                             &sub.exchange,
//                             &sub.symbol,
//                             period,
//                             start_ts,
//                             batch_end,
//                         ));
//
//                         self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Gap, step_millis).await;
//
//                         start_ts = batch_end;
//                     }
//                 }
//
//                 // 回溯任务
//                 let mut oldest = meta.last_filled.unwrap_or(now);
//                 while oldest > oldest_allowed {
//                     let start = std::cmp::max(oldest - Duration::days(1), oldest_allowed);
//
//                     let ctx = Arc::new(FetchContext::new_with_range(
//                         &sub.exchange,
//                         &sub.symbol,
//                         period,
//                         start,
//                         oldest,
//                     ));
//
//                     self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Lookback, step_millis).await;
//
//                     oldest = start;
//                 }
//
//                 // 更新 last_checked
//                 if let Err(e) = self.db.update_last_checked(&key, now).await {
//                     error!("Failed to update last_checked: {:?}", e);
//                 }
//             }
//         }
//     }
// }

impl<F> HistoricalBackfillService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fn market_key(exchange: &str, symbol: &str, period: &str) -> MarketKey {
        MarketKey {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            interval: period.to_string(),
        }
    }

    fn merge_ranges(ranges: &mut Vec<(DateTime<Utc>, DateTime<Utc>)>) {
        if ranges.is_empty() {
            return;
        }
        ranges.sort_by_key(|r| r.0);
        let mut merged = vec![ranges[0]];
        for &(start, end) in ranges.iter().skip(1) {
            let last = merged.last_mut().unwrap();
            if start <= last.1 {
                last.1 = std::cmp::max(last.1, end);
            } else {
                merged.push((start, end));
            }
        }
        *ranges = merged;
    }

    fn compute_batch_hours(&self, source: &HistoricalSource, period_str: &str) -> i64 {
        let default_hours = self.default_max_batch_hours;
        let period_ms = match period_str {
            "1m" => 60_000,
            "5m" => 5 * 60_000,
            "15m" => 15 * 60_000,
            "30m" => 30 * 60_000,
            "1h" => 60 * 60_000,
            "4h" => 4 * 60 * 60_000,
            "1d" => 24 * 60 * 60_000,
            _ => 60_000,
        };
        let max_batch_millis = source.batch_size as i64 * period_ms;
        let max_batch_hours = max_batch_millis / (60 * 60 * 1000);
        std::cmp::max(1, std::cmp::min(default_hours, max_batch_hours))
    }

    async fn enqueue_job(&self, job: BackfillJob<F>) {
        let mut q = self.job_queue.lock().await;
        q.push(job);
    }

    /// 批量从队列安全取任务
    async fn next_jobs(&self, batch_size: usize) -> Vec<BackfillJob<F>> {
        let mut q = self.job_queue.lock().await;
        let mut jobs = Vec::new();
        for _ in 0..batch_size {
            if let Some(job) = q.pop() {
                jobs.push(job);
            } else {
                break;
            }
        }
        jobs
    }

    /// 入队任务并通知 worker
    async fn enqueue_job_notify(&self, job: BackfillJob<F>, notify: &Arc<Notify>) {
        {
            let mut q = self.job_queue.lock().await;
            q.push(job);
        }
        notify.notify_one();
    }

    /// 执行任务，批量提交 + 原子更新缺口和 last_filled
    async fn execute_job_atomic(&self, job: BackfillJob<F>) {
        let mut attempts = 0;
        while attempts < self.max_retries {
            attempts += 1;

            match job
                .scheduler
                .add_batch_tasks(job.ctx.clone(), job.data_type.clone(), job.step_millis, vec![])
                .await
            {
                Ok(_) => {
                    // 获取当前 meta
                    match job.db.get_meta(&job.key).await {
                        Ok(Some(meta)) => {
                            let new_last_filled = match Utc.timestamp_millis_opt(job.ctx.range.end) {
                                MappedLocalTime::Single(dt) => dt,
                                _ => {
                                    error!("Timestamp out-of-range for last_filled: {}", job.ctx.range.end);
                                    break;
                                }
                            };

                            // 计算新的 last_filled
                            let last_filled = Some(std::cmp::max(
                                meta.last_filled.unwrap_or(new_last_filled),
                                new_last_filled,
                            ));

                            // 清理缺口区间：删除被当前 job 覆盖的部分
                            let missing_ranges: Vec<(DateTime<Utc>, DateTime<Utc>)> = meta
                                .missing_ranges
                                .into_iter()
                                .filter(|&(s, e)| {
                                    e.timestamp_millis() <= job.ctx.range.start
                                        || s.timestamp_millis() >= job.ctx.range.end
                                })
                                .collect();

                            // 原子更新 meta
                            if let Err(e) = job.db.update_meta(&job.key, last_filled, Some(missing_ranges)).await {
                                error!("Failed to update meta after atomic batch: {:?}", e);
                            }
                        }
                        Ok(None) => {
                            // 如果 meta 不存在，初始化
                            let last_filled = match Utc.timestamp_millis_opt(job.ctx.range.end) {
                                MappedLocalTime::Single(dt) => Some(dt),
                                _ => {
                                    error!("Timestamp out-of-range for last_filled: {}", job.ctx.range.end);
                                    break;
                                }
                            };
                            if let Err(e) = job.db.update_meta(&job.key, last_filled, Some(vec![])).await {
                                error!("Failed to create meta for key: {:?}", e);
                            }
                        }
                        Err(e) => error!("Failed to get meta for key {:?}: {:?}", job.key, e),
                    }

                    break;
                }
                Err(e) => {
                    error!("Job failed (attempt {}/{}): {:?}", attempts, self.max_retries, e);
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// Worker 循环（缺口优先 + 回溯补齐）支持优雅退出
    pub async fn worker_loop(&self, mut shutdown_rx: broadcast::Receiver<()>, notify: Arc<Notify>) {
        loop {
            // 优先尝试批量取任务
            let jobs = self.next_jobs(5).await;

            if jobs.is_empty() {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Worker received shutdown signal, exiting");
                        break;
                    }
                    _ = notify.notified() => {
                        // 有新任务入队，继续循环取任务
                        continue;
                    }
                    else => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            } else {
                for job in jobs {
                    self.execute_job_atomic(job).await;
                }
            }
        }
    }

    /// 启动多个 worker 并返回 shutdown 通道
    pub fn start_workers(self: Arc<Self>, worker_count: usize) -> (broadcast::Sender<()>, Arc<Notify>) {
        let (shutdown_tx, _) = broadcast::channel::<()>(worker_count);
        let notify = Arc::new(Notify::new());

        for _ in 0..worker_count {
            let svc = self.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let notify_clone = notify.clone();

            tokio::spawn(async move {
                svc.worker_loop(shutdown_rx, notify_clone).await;
            });
        }

        (shutdown_tx, notify)
    }

    /// 调度任务到队列
    async fn schedule_backfill(
        &self,
        ctx: Arc<FetchContext>,
        data_type: BackfillDataType,
        key: MarketKey,
        priority: BackfillPriority,
        step_millis: i64,
    ) {
        let job = BackfillJob {
            ctx,
            data_type,
            priority,
            retries: 0,
            scheduler: self.scheduler.clone(),
            db: self.db.clone(),
            key,
            step_millis,
        };
        self.enqueue_job(job).await;
    }

    /// 初始化最近任务（优先缺口）
    pub async fn init_recent_tasks(
        &self,
        subscriptions: &SubscriptionMap,
        past_hours: i64,
        step_millis: i64,
        data_type: BackfillDataType,
    ) {
        for entry in subscriptions.iter() {
            let (_key, sub) = entry.pair();
            for period in &sub.periods {
                let key = Self::market_key(&sub.exchange, &sub.symbol, period);

                let last_filled = self
                    .db
                    .get_meta(&key)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|meta| meta.last_filled)
                    .unwrap_or_else(|| Utc::now() - Duration::hours(past_hours));

                let end_ts = Utc::now();
                let mut start_ts = last_filled;

                // 批量构建任务
                while start_ts < end_ts {
                    let batch_hours = self.compute_batch_hours(
                        &HistoricalSource {
                            name: "".to_string(),
                            exchange: sub.exchange.to_string(),
                            last_success_ts: 0,
                            last_fetch_ts: 0,
                            batch_size: 500,
                            supports_tick: true,
                            supports_trade: true,
                            supports_ohlcv: true,
                        },
                        period,
                    );

                    let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);

                    let ctx = Arc::new(FetchContext::new_with_range(
                        &sub.exchange,
                        &sub.symbol,
                        period,
                        start_ts,
                        batch_end,
                    ));

                    self.schedule_backfill(
                        ctx,
                        data_type.clone(),
                        key.clone(),
                        BackfillPriority::Lookback,
                        step_millis,
                    )
                    .await;

                    start_ts = batch_end;
                }
            }
        }
    }

    /// 后台回溯历史数据，缺口优先，原子更新
    pub async fn backfill_historical(
        &self,
        subscriptions: &SubscriptionMap,
        step_millis: i64,
        data_type: BackfillDataType,
        lookback_days: i64,
    ) {
        let now = Utc::now();
        let oldest_allowed = now - Duration::days(lookback_days);

        for entry in subscriptions.iter() {
            let (_key, sub) = entry.pair();
            for period in &sub.periods {
                let key = Self::market_key(&sub.exchange, &sub.symbol, period);

                let mut meta = self.db.get_meta(&key).await.unwrap_or(None).unwrap_or(BackfillMeta {
                    last_filled: None,
                    last_checked: None,
                    missing_ranges: vec![],
                });

                Self::merge_ranges(&mut meta.missing_ranges);

                // 1️⃣ 缺口任务
                for gap in meta.missing_ranges.iter() {
                    let mut start_ts = gap.0;
                    let end_ts = gap.1;
                    while start_ts < end_ts {
                        let batch_hours = self.compute_batch_hours(
                            &HistoricalSource {
                                name: "".to_string(),
                                exchange: sub.exchange.to_string(),
                                last_success_ts: 0,
                                last_fetch_ts: 0,
                                batch_size: 500,
                                supports_tick: true,
                                supports_trade: true,
                                supports_ohlcv: true,
                            },
                            period,
                        );

                        let batch_end = std::cmp::min(start_ts + Duration::hours(batch_hours), end_ts);

                        let ctx = Arc::new(FetchContext::new_with_range(
                            &sub.exchange,
                            &sub.symbol,
                            period,
                            start_ts,
                            batch_end,
                        ));

                        self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Gap, step_millis)
                            .await;

                        start_ts = batch_end;
                    }
                }

                // 2️⃣ 回溯任务
                let mut oldest = meta.last_filled.unwrap_or(now);
                while oldest > oldest_allowed {
                    let start = std::cmp::max(oldest - Duration::days(1), oldest_allowed);

                    let ctx = Arc::new(FetchContext::new_with_range(
                        &sub.exchange,
                        &sub.symbol,
                        period,
                        start,
                        oldest,
                    ));

                    self.schedule_backfill(
                        ctx,
                        data_type.clone(),
                        key.clone(),
                        BackfillPriority::Lookback,
                        step_millis,
                    )
                    .await;

                    oldest = start;
                }

                // 3️⃣ 更新 last_checked
                if let Err(e) = self.db.update_last_checked(&key, now).await {
                    error!("Failed to update last_checked: {:?}", e);
                }
            }
        }
    }
}

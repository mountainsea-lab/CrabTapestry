use crate::domain::model::AppResult;
use crate::domain::model::market_fill_range::{FillRangeStatus, HmdsMarketFillRange};
use crate::domain::service::{
    query_fill_range_list, query_latest_ranges, update_fill_range_info, update_fill_ranges_status,
};
use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::scheduler::service::back_fill_job::{BackfillJob, BackfillPriority};
use crate::ingestor::scheduler::service::{BackfillMeta, BackfillMetaStore, MarketKey};
use crate::ingestor::scheduler::{BackfillDataType, OutputSubscriber};
use crate::ingestor::types::FetchContext;
use chrono::{DateTime, Duration, Utc};
use crab_types::TimeRange;
use crab_types::crab_time::MillisToUtc;
use crab_types::time_frame::TimeFrame;
use ms_tracing::tracing_utils::internal::{debug, error, info, warn};
use std::collections::BinaryHeap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// HistoricalBackfillService
pub struct HistoricalBackfillService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    pub(crate) scheduler: Arc<BaseBackfillScheduler<F>>,
    db: Arc<dyn BackfillMetaStore>,
    max_retries: usize,
    job_queue: Arc<Mutex<BinaryHeap<BackfillJob<F>>>>,
    notify: Arc<Notify>, // 新增字段，用于 worker 通知
}

impl<F> HistoricalBackfillService<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 构造函数
    pub fn new(scheduler: Arc<BaseBackfillScheduler<F>>, db: Arc<dyn BackfillMetaStore>, max_retries: usize) -> Self {
        Self {
            scheduler,
            db,
            max_retries,
            job_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            notify: Arc::new(Notify::new()), // 初始化 notify
        }
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

    /// 执行任务，批量提交
    async fn execute_job_atomic(&self, job: BackfillJob<F>) {
        let mut attempts = 0;
        while attempts < self.max_retries {
            attempts += 1;

            let _ = job
                .scheduler
                .add_batch_tasks(job.ctx.clone(), job.data_type.clone(), job.step_millis, vec![])
                .await;
        }
    }

    /// Worker 循环（缺口优先 + 回溯补齐）支持优雅退出
    pub async fn worker_loop(&self, shutdown: Arc<Notify>, notify: Arc<Notify>) {
        loop {
            // 先尝试批量消费任务
            loop {
                let jobs = self.next_jobs(5).await;
                if jobs.is_empty() {
                    break;
                }
                for job in jobs {
                    self.execute_job_atomic(job).await;
                }
            }

            // 队列空了，等待新任务或 shutdown
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("Worker received shutdown signal, exiting");
                    break;
                }
                _ = notify.notified() => {
                    // 新任务入队，继续下一轮消费
                }
            }
        }
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
            key,
            step_millis,
        };
        // 使用 notify 通知 worker
        self.enqueue_job_notify(job, &self.notify).await;
    }

    /// 初始化缓存预生成区间维护任务最新时间
    pub async fn cache_latest_range_time(&self) {
        if let Ok(ranges) = query_latest_ranges().await {
            if ranges.is_empty() {
                debug!("latest ranges is empty");
                return;
            }

            // 遍历所有 range，生成 futures
            let futures_vec = ranges
                .iter()
                .filter_map(|r| {
                    let market_key = MarketKey::new(&r.exchange, &r.symbol, &r.period);

                    // 安全解析时间
                    let start_time = match r.start_time.to_utc() {
                        Some(ts) => ts,
                        None => {
                            error!("invalid start_time for market {:?}", market_key);
                            return None;
                        }
                    };

                    let end_time = match r.end_time.to_utc() {
                        Some(ts) => ts,
                        None => {
                            error!("invalid end_time for market {:?}", market_key);
                            return None;
                        }
                    };

                    // 构造 fill_meta
                    let fill_meta = BackfillMeta {
                        range_id: Some(r.id),
                        market_key: market_key.clone(),
                        start_time,
                        last_filled: Some(end_time),
                        batch_size: r.batch_size,
                        quote: r.quote.clone(),
                    };

                    // 返回异步 future
                    Some(async move {
                        if let Err(e) = self.db.add_last_filled(&market_key, fill_meta).await {
                            error!(?market_key, ?e, "failed to update last_filled");
                        }
                    })
                })
                .collect::<Vec<_>>();

            // 并发执行所有 futures
            futures::future::join_all(futures_vec).await;

            info!(count = ranges.len(), "cache_latest_range_time finished");
        } else {
            error!("query_latest_ranges failed");
        }
    }

    /// 初始化最近任务 - 根据缓存中的最新时间生成任务并调度
    pub async fn init_recent_tasks(&self, data_type: BackfillDataType) {
        let now = Utc::now();
        let map = self.db.get_all_metas().await;

        for (key, meta) in map.iter() {
            let start_time = Self::compute_start_time(meta, now);
            let mut batch_start = start_time;

            let period_millis = TimeFrame::from_str(&meta.market_key.period)
                .map(|tf| tf.to_millis())
                .unwrap_or(60_000);
            let max_span = period_millis * meta.batch_size as i64;

            while batch_start < now {
                let batch_end = std::cmp::min(batch_start + Duration::milliseconds(max_span), now);

                if Self::is_valid_batch(batch_start, batch_end, period_millis) {
                    let time_range = TimeRange::new(batch_start.timestamp_millis(), batch_end.timestamp_millis());
                    let ctx = Arc::new(FetchContext::new(
                        meta.range_id,
                        &key.exchange,
                        &key.symbol,
                        &meta.quote,
                        Some(&key.period),
                        time_range,
                        meta.batch_size,
                    ));

                    let step_millis = (batch_end - batch_start).num_milliseconds();
                    self.schedule_backfill(
                        ctx,
                        data_type.clone(),
                        key.clone(),
                        BackfillPriority::Lookback,
                        step_millis,
                    )
                    .await;
                }

                batch_start = batch_end;
            }
            // 更新缓存中最新时间
            if let Err(e) = self.db.update_last_filled(key, now).await {
                error!(?key, ?e, "failed to update last_filled");
            }
            // 判断这个区间是否完成
            let elapsed = (now - meta.start_time).num_milliseconds();
            if elapsed >= max_span {
                if let Some(fill_meta) = self.db.get_meta_by_key(key).await {
                    let update_fill_range = fill_meta.to_update_synced();
                    update_fill_range_info(update_fill_range).await.ok();

                    let new_fill_meta = fill_meta.new_for_next_range(now);
                    self.db.add_last_filled(key, new_fill_meta).await.ok();
                }
            }
        }

        info!("init_recent_tasks finished for {} markets", map.len());
    }

    /// 计算最新任务的开始时间
    fn compute_start_time(meta: &BackfillMeta, now: DateTime<Utc>) -> DateTime<Utc> {
        meta.last_filled
            .unwrap_or_else(|| now - Duration::hours(24))
            .max(meta.start_time)
    }
    /// 判断批次是否足够生成任务
    fn is_valid_batch(batch_start: DateTime<Utc>, batch_end: DateTime<Utc>, period_millis: i64) -> bool {
        (batch_end - batch_start).num_milliseconds() >= period_millis
    }

    /// 预生成的数据区间信息[HmdsMarketFillRange]--> 调度任务[BackfillJob]
    pub async fn backfill_historical(&self, data_type: BackfillDataType) {
        if let Ok(range_list) = query_fill_range_list().await {
            if range_list.is_empty() {
                debug!("Backfill range list is empty");
                return;
            }

            let ids = range_list.iter().map(|r| r.id).collect::<Vec<u64>>();

            for range in range_list {
                if let Err(e) = self.process_range(range, data_type.clone()).await {
                    warn!("Failed to process range: {:?}", e);
                }
            }

            // ✅ 批量更新状态为 Syncing
            if let Err(e) = update_fill_ranges_status(&ids, FillRangeStatus::Syncing).await {
                warn!("Failed to batch update status: {:?}", e);
            }
        } else {
            debug!("Backfill range list query failed");
        }
    }

    /// 处理单个区间任务
    pub async fn process_range(&self, range: HmdsMarketFillRange, data_type: BackfillDataType) -> AppResult<()> {
        // 构造上下文
        let ctx = Arc::new(FetchContext::new(
            Some(range.id),
            &range.exchange,
            &range.symbol,
            &range.quote,
            Some(&range.period),
            TimeRange::new(range.start_time, range.end_time),
            range.batch_size,
        ));

        // 计算步长：用 batch span 或最大区间长度
        let range_millis = range.end_time - range.start_time;

        // 任务调度 key 用 MarketKey
        let market_key = MarketKey::new(&range.exchange, &range.symbol, &range.period);

        self.schedule_backfill(
            ctx,
            data_type,
            market_key, // 例如 "binance:BTCUSDT:1m"
            BackfillPriority::Lookback,
            range_millis,
        )
        .await;

        Ok(())
    }

    /// 启动多个 worker，不返回 shutdown 通道，使用内部 notify 进行新任务通知
    pub async fn start_workers(self: Arc<Self>, worker_count: usize, shutdown: Arc<Notify>) {
        // 缓存维护区间最新时间
        self.cache_latest_range_time().await;

        for _ in 0..worker_count {
            let svc = self.clone();
            let notify_clone = svc.notify.clone(); // 内部任务通知
            let shutdown_clone = shutdown.clone(); // 统一 shutdown 控制

            tokio::spawn(async move {
                svc.worker_loop(shutdown_clone, notify_clone).await;
            });
        }
    }

    pub async fn loop_maintain_tasks_notify(
        &self,
        data_type: BackfillDataType,
        shutdown: &Arc<Notify>, // ✅ 替换 broadcast
    ) {
        let maintain_interval = std::time::Duration::from_secs(60);

        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("Maintain loop received shutdown signal, exiting");
                    break;
                }
                _ = tokio::time::sleep(maintain_interval) => {
                    info!("Running maintain loop iteration...");

                    // 1️⃣ 预生成维护区间->调度任务
                    self.backfill_historical(data_type.clone()).await;

                    // 2️⃣ 最新数据维护
                    self.init_recent_tasks(data_type.clone()).await;
                }
            }
        }

        info!("Maintain loop stopped");
    }

    /// 转发订阅数据
    pub fn subscribe(&self) -> OutputSubscriber {
        self.scheduler.subscribe()
    }
}

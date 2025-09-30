use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::scheduler::service::back_fill_job::{BackfillJob, BackfillPriority};
use crate::ingestor::scheduler::service::{BackfillMeta, BackfillMetaStore, MarketKey};
use crate::ingestor::scheduler::{BackfillDataType, OutputSubscriber};
use crate::ingestor::types::{FetchContext, HistoricalSource};
use chrono::{DateTime, Duration, MappedLocalTime, TimeZone, Utc};
use crab_infras::config::sub_config::SubscriptionMap;
use crab_types::time_frame::TimeFrame;
use futures_util::TryFutureExt;
use ms_tracing::tracing_utils::internal::{error, info, warn};
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
    pub fn new(
        scheduler: Arc<BaseBackfillScheduler<F>>,
        db: Arc<dyn BackfillMetaStore>,
        max_retries: usize,
    ) -> Self {
        Self {
            scheduler,
            db,
            max_retries,
            job_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            notify: Arc::new(Notify::new()), // 初始化 notify
        }
    }

    fn market_key(exchange: &str, symbol: &str, period: &str) -> MarketKey {
        MarketKey {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            interval: period.to_string(),
        }
    }


    /// 计算单次批量查询的最大跨度
    fn compute_batch_span(&self, source: &HistoricalSource, period: &Arc<str>) -> Duration {
        // Arc<str> → &str
        let period_str: &str = period.as_ref();

        // 尝试解析成 TimeFrame，如果失败就默认 1m
        let tf = match TimeFrame::from_str(period_str) {
            Ok(tf) => tf,
            Err(_) => {
                warn!("Invalid period string '{}', defaulting to 1m", period_str);
                TimeFrame::M1
            }
        };

        let period_ms = tf.to_millis();
        let batch_size = source.batch_size as i64;
        let max_batch_millis = period_ms * batch_size;

        Duration::milliseconds(max_batch_millis)
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
            db: self.db.clone(),
            key,
            step_millis,
        };
        // 使用 notify 通知 worker
        self.enqueue_job_notify(job, &self.notify).await;
    }

    /// 初始化最近任务（优先缺口）
    pub async fn init_recent_tasks(
        &self,
        data_type: BackfillDataType,
    ) {
        let now = Utc::now();

        for entry in subscriptions.iter() {
            let (_key, sub) = entry.pair();
            for period_arc in &sub.periods {
                let key = Self::market_key(&sub.exchange, &sub.symbol, period_arc);

                // 获取上次回溯时间
                let last_filled = self
                    .db
                    .get_meta(&key)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|meta| meta.last_filled)
                    .unwrap_or_else(|| now - Duration::hours(past_hours));

                let mut start_ts = last_filled;
                let end_ts = now;

                // 构造 HistoricalSource（交易所批量限制）
                let source = HistoricalSource {
                    name: "".to_string(),
                    exchange: sub.exchange.to_string(),
                    last_success_ts: 0,
                    last_fetch_ts: 0,
                    batch_size: 500, // 最大支持条数
                    supports_tick: false,
                    supports_trade: false,
                    supports_ohlcv: true,
                };
                let last_filled_str = last_filled.format("%Y-%m-%d %H:%M:%S").to_string();
                info!(
                    "Market={} Symbol={} Period={} last_filled={} now={}",
                    sub.exchange,
                    sub.symbol,
                    period_arc,
                    last_filled_str,
                    now.format("%Y-%m-%d %H:%M:%S")
                );

                while start_ts < end_ts {
                    // 计算批量跨度（毫秒） = batch_size * interval，自动控制
                    let batch_span = self.compute_batch_span(&source, period_arc);

                    // ⚡ 保持 batch_span 大小，不管剩余时间是否足够，循环处理直到 end_ts
                    let mut batch_start = start_ts;
                    while batch_start < end_ts {
                        let batch_end = batch_start + batch_span;
                        let batch_end = std::cmp::min(batch_end, end_ts); // 防止超过 now

                        // 构建 FetchContext
                        let ctx = Arc::new(FetchContext::new_with_range(
                            &sub.exchange,
                            &sub.symbol,
                            &sub.quote,
                            period_arc,
                            batch_start,
                            batch_end,
                        ));

                        // step_millis 记录为 batch_span 毫秒
                        let step_millis = batch_span.num_milliseconds();

                        // 调度 BackfillJob
                        self.schedule_backfill(
                            ctx,
                            data_type.clone(),
                            key.clone(),
                            BackfillPriority::Lookback,
                            step_millis,
                        )
                        .await;

                        batch_start = batch_end; // 下一批
                    }

                    // 更新 start_ts 为 end_ts，结束 while 循环
                    start_ts = end_ts;
                }
            }
        }
    }

    /// 后台回溯历史数据，缺口优先，原子更新
    pub async fn backfill_historical(
        &self,
        subscriptions: &SubscriptionMap,
        data_type: BackfillDataType,
        lookback_days: i64,
    ) {
        let now = Utc::now();
        let oldest_allowed = now - Duration::days(lookback_days);

        for entry in subscriptions.iter() {
            let (_key, sub) = entry.pair();

            for period_arc in &sub.periods {
                let period_str: &str = period_arc.as_ref();

                // 解析 TimeFrame，如果失败默认 1m
                let period_tf = TimeFrame::from_str(period_str).unwrap_or_else(|_| {
                    warn!("Invalid period '{}', defaulting to 1m", period_str);
                    TimeFrame::M1
                });

                let key = Self::market_key(&sub.exchange, &sub.symbol, period_arc);

                // 获取上次回溯时间
                let mut meta = self.db.get_meta(&key).await.unwrap_or(None).unwrap_or(BackfillMeta {
                    last_filled: None,
                    last_checked: None,
                    missing_ranges: vec![],
                });

                Self::merge_ranges(&mut meta.missing_ranges);

                // 构造 HistoricalSource（交易所批量限制）
                let source = HistoricalSource {
                    name: "".to_string(),
                    exchange: sub.exchange.to_string(),
                    last_success_ts: 0,
                    last_fetch_ts: 0,
                    batch_size: 1000, // 最大支持条数，可根据交易所调整
                    supports_tick: true,
                    supports_trade: true,
                    supports_ohlcv: true,
                };

                // 1️⃣ 缺口任务
                for gap in meta.missing_ranges.iter() {
                    let mut start_ts = gap.0;
                    let end_ts = gap.1;

                    while start_ts < end_ts {
                        // 计算单次批量跨度，最小为 1 条 K 线
                        let mut batch_span = self.compute_batch_span(&source, period_arc);
                        if batch_span < Duration::milliseconds(period_tf.to_millis()) {
                            batch_span = Duration::milliseconds(period_tf.to_millis());
                        }

                        let batch_end = std::cmp::min(start_ts + batch_span, end_ts);

                        let ctx = Arc::new(FetchContext::new_with_range(
                            &sub.exchange,
                            &sub.symbol,
                            &sub.quote,
                            period_arc,
                            start_ts,
                            batch_end,
                        ));

                        // step_millis 记录为 batch_span 毫秒
                        let step_millis = batch_span.num_milliseconds();

                        self.schedule_backfill(ctx, data_type.clone(), key.clone(), BackfillPriority::Gap, step_millis)
                            .await;

                        start_ts = batch_end;
                    }
                }

                // 2️⃣ 回溯任务
                let mut oldest = meta.last_filled.unwrap_or(now - Duration::days(lookback_days));
                while oldest > oldest_allowed {
                    let mut batch_span = self.compute_batch_span(&source, period_arc);
                    if batch_span < Duration::milliseconds(period_tf.to_millis()) {
                        batch_span = Duration::milliseconds(period_tf.to_millis());
                    }

                    let start = std::cmp::max(oldest - batch_span, oldest_allowed);

                    let ctx = Arc::new(FetchContext::new_with_range(
                        &sub.exchange,
                        &sub.symbol,
                        &sub.quote,
                        period_arc,
                        start,
                        oldest,
                    ));

                    let step_millis = batch_span.num_milliseconds();

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
                    error!("Failed to update last_checked for {:?}: {:?}", key, e);
                }
            }
        }
    }

    /// 启动多个 worker，不返回 shutdown 通道，使用内部 notify 进行新任务通知
    pub fn start_workers(self: Arc<Self>, worker_count: usize, shutdown: Arc<Notify>) {
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
        subscriptions: &SubscriptionMap,
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

                    // 1️⃣ 检查缺口并调度回溯任务
                    // self.backfill_historical(subscriptions, data_type.clone(), self.lookback_days).await;

                    // 2️⃣ 最近数据维护（过去 2 小时）
                    self.init_recent_tasks(subscriptions, 2, data_type.clone()).await;
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

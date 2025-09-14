use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::{
    BackfillDataType, BackfillNode, HistoricalBatchEnum, NodeMeta, NodeStatus, OutputSubscriber,
};
use crate::ingestor::types::FetchContext;
use anyhow::Result;
use crab_types::TimeRange;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{error, info, warn};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, broadcast};

/// BaseBackfillScheduler
pub struct BaseBackfillScheduler<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fetcher: Arc<F>,
    nodes: DashMap<usize, Arc<BackfillNode>>,
    ready_queue: Mutex<Vec<usize>>,
    notify: Notify,
    output_tx: broadcast::Sender<HistoricalBatchEnum>, // 改为广播发送器
    shutdown: Arc<AtomicBool>,
    retry_limit: usize,
    next_id: AtomicUsize,
}

impl<F> BaseBackfillScheduler<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 创建调度器
    pub fn new(fetcher: Arc<F>, retry_limit: usize) -> Arc<Self> {
        let (output_tx, _) = broadcast::channel(100); // 创建广播通道‘

        Arc::new(Self {
            fetcher,
            nodes: DashMap::new(),
            ready_queue: Mutex::new(Vec::new()),
            notify: Notify::new(),
            output_tx,
            shutdown: Arc::new(AtomicBool::new(false)),
            retry_limit,
            next_id: AtomicUsize::new(1),
        })
    }

    /// 添加单个任务节点
    pub async fn add_task(
        self: &Arc<Self>,
        ctx: Arc<FetchContext>,
        data_type: BackfillDataType,
        depends_on: Vec<usize>,
    ) -> usize {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let node = Arc::new(BackfillNode {
            id,
            ctx,
            data_type,
            meta: Mutex::new(NodeMeta {
                status: NodeStatus::Pending,
                retry_count: 0,
                started_at: None,
                finished_at: None,
                last_error: None,
            }),
            dependencies: depends_on.iter().copied().collect(),
            dependents: Mutex::new(HashSet::new()),
        });

        // 更新上游 dependents
        for &dep_id in &depends_on {
            if let Some(dep_node) = self.nodes.get(&dep_id) {
                let mut deps = dep_node.dependents.lock().await;
                deps.insert(id);
            }
        }

        // 无依赖直接入就绪队列
        if depends_on.is_empty() {
            let mut q = self.ready_queue.lock().await;
            q.push(id);
            self.notify.notify_one();
        }

        self.nodes.insert(id, node);
        id
    }

    /// 批量任务（按时间切片）
    pub async fn add_batch_tasks(
        self: &Arc<Self>,
        base_ctx: Arc<FetchContext>,
        data_type: BackfillDataType,
        step_millis: i64,
        depends_on: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut ids = Vec::new();
        let depends_on = Arc::new(depends_on); // clone 更轻量
        let mut cur = base_ctx.range.start;

        while cur < base_ctx.range.end {
            let next = std::cmp::min(cur + step_millis, base_ctx.range.end);

            let new_ctx = Arc::new(FetchContext {
                range: TimeRange { start: cur, end: next },
                ..(*base_ctx).clone()
            });

            let id = self.add_task(new_ctx, data_type.clone(), (*depends_on).clone()).await;

            ids.push(id);
            cur = next;
        }

        Ok(ids)
    }

    /// 停止调度器
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
        info!("BackfillScheduler stopping...");
    }

    /// 入就绪队列
    async fn enqueue_ready(&self, node_id: usize) {
        let mut q = self.ready_queue.lock().await;
        q.push(node_id);
        self.notify.notify_one();
    }

    /// worker 循环
    async fn worker_loop(self: Arc<Self>) {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let node_id_opt = { self.ready_queue.lock().await.pop() };
            let node_id = match node_id_opt {
                Some(id) => id,
                None => {
                    self.notify.notified().await;
                    continue;
                }
            };

            if let Err(e) = self.execute_node(node_id).await {
                warn!(error=?e, "execute_node error for node {}", node_id);
            }
        }
    }

    /// 执行单个节点
    async fn execute_node(self: &Arc<Self>, node_id: usize) -> Result<()> {
        let node_arc = match self.nodes.get(&node_id) {
            Some(n) => n.clone(),
            None => return Ok(()),
        };

        // 依赖检查
        for &dep_id in &node_arc.dependencies {
            if let Some(dep_node) = self.nodes.get(&dep_id) {
                let dep_meta = dep_node.meta.lock().await;
                match dep_meta.status {
                    NodeStatus::Completed => {}
                    NodeStatus::Failed => {
                        let mut meta = node_arc.meta.lock().await;
                        meta.status = NodeStatus::Skipped;
                        meta.finished_at = Some(Instant::now());
                        info!("Node {} skipped due to failed dependency", node_id);
                        // 推进下游
                        let dependents = node_arc.dependents.lock().await.clone();
                        for dep_id in dependents {
                            self.enqueue_ready(dep_id).await;
                        }
                        return Ok(());
                    }
                    _ => {
                        // 依赖未完成，重新入队
                        self.enqueue_ready(node_id).await;
                        return Ok(());
                    }
                }
            }
        }

        // 标记 Running
        {
            let mut meta = node_arc.meta.lock().await;
            meta.status = NodeStatus::Running;
            meta.started_at = Some(Instant::now());
            info!("Node {} running", node_id);
        }

        // 重试循环
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutdown detected, aborting node {}", node_id);
                break;
            }

            // 读取并增加 retry_count
            let attempt = {
                let mut meta = node_arc.meta.lock().await;
                meta.retry_count = meta.retry_count.saturating_add(1);
                meta.retry_count
            };

            // fetch
            let fetch_res: Result<HistoricalBatchEnum> = match node_arc.data_type {
                BackfillDataType::OHLCV => self
                    .fetcher
                    .fetch_ohlcv(node_arc.ctx.clone())
                    .await
                    .map(HistoricalBatchEnum::OHLCV),
                BackfillDataType::Tick => self
                    .fetcher
                    .fetch_ticks(node_arc.ctx.clone())
                    .await
                    .map(HistoricalBatchEnum::Tick),
                BackfillDataType::Trade => Err(anyhow::anyhow!("Trade fetch not implemented")),
            };

            match fetch_res {
                Ok(batch) => {
                    // 发送到 output channel
                    if let Err(e) = self.output_tx.send(batch) {
                        error!("Node {} output send failed: {}", node_id, e);
                    }

                    let mut meta = node_arc.meta.lock().await;
                    meta.status = NodeStatus::Completed;
                    meta.finished_at = Some(Instant::now());
                    info!("Node {} completed", node_id);

                    // 推进下游
                    let dependents = node_arc.dependents.lock().await.clone();
                    for dep_id in dependents {
                        self.enqueue_ready(dep_id).await;
                    }

                    break; // 完成退出 retry
                }
                Err(err) => {
                    let err_str = format!("{:?}", err);
                    warn!("Node {} fetch attempt {} failed: {}", node_id, attempt, err_str);
                    {
                        let mut meta = node_arc.meta.lock().await;
                        meta.last_error = Some(err_str);
                    }

                    if attempt >= self.retry_limit {
                        let mut meta = node_arc.meta.lock().await;
                        meta.status = NodeStatus::Failed;
                        meta.finished_at = Some(Instant::now());
                        error!("Node {} failed after {} attempts", node_id, attempt);

                        let dependents = node_arc.dependents.lock().await.clone();
                        for dep_id in dependents {
                            self.enqueue_ready(dep_id).await;
                        }
                        break;
                    }

                    // 指数退避 + jitter
                    let exp = attempt.min(10) as u32;
                    let base = 2u64.saturating_pow(exp);
                    let jitter: u64 = rand::random_range(0..5);
                    let sleep_secs = std::cmp::min(base, 60) + jitter;
                    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
                }
            }
        }

        Ok(())
    }

    /// 启动 worker_count 个 worker
    pub async fn run(self: Arc<Self>, worker_count: usize) -> Result<()> {
        info!("BackfillScheduler starting with {} workers", worker_count);

        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let sched = self.clone();
            handles.push(tokio::spawn(async move {
                sched.worker_loop().await;
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        info!("BackfillScheduler stopped");
        Ok(())
    }

    /// 获取节点状态
    pub async fn get_node_meta(&self, node_id: usize) -> Option<NodeMeta> {
        let node = self.nodes.get(&node_id)?;
        let guard = node.meta.lock().await;
        Some(guard.clone())
    }

    /// 获取 pending 任务数量（就绪队列 + 未完成节点）
    pub async fn pending_tasks(&self) -> usize {
        let queue_len = self.ready_queue.lock().await.len();
        let incomplete_nodes = self
            .nodes
            .iter()
            .filter(|n| {
                let meta = futures::executor::block_on(n.meta.lock());
                matches!(meta.status, NodeStatus::Pending | NodeStatus::Running)
            })
            .count();

        queue_len + incomplete_nodes
    }

    /// 订阅输出
    pub fn subscribe(&self) -> OutputSubscriber {
        OutputSubscriber::new(self.output_tx.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
    use crate::ingestor::types::HistoricalSource;
    use anyhow::Result;
    use futures_util::{join, try_join};
    use std::time::Duration;
    use tokio::time::timeout;

    fn make_source() -> HistoricalSource {
        HistoricalSource {
            name: "Binance API".to_string(),
            exchange: "binance".to_string(),
            last_success_ts: 0,
            last_fetch_ts: 0,
            batch_size: 0,
            supports_tick: false,
            supports_trade: false,
            supports_ohlcv: true,
        }
    }

    fn make_ctx(period: &str, past_hours: i64) -> Arc<FetchContext> {
        FetchContext::new_with_past(
            make_source(),
            "binance",
            "BTCUSDT",
            Some(period),
            Some(past_hours),
            None,
        )
    }

    #[tokio::test]
    async fn test_single_task_with_binance_fetcher() -> Result<()> {
        let fetcher = Arc::new(BinanceFetcher::new());
        let scheduler = BaseBackfillScheduler::new(fetcher, 2);

        let ctx = make_ctx("1h", 3);
        let task_id = scheduler.add_task(ctx, BackfillDataType::OHLCV, vec![]).await;

        // 启动 worker
        let sched = scheduler.clone();
        tokio::spawn(async move { sched.run(1).await.unwrap() });

        // 订阅输出
        let mut sub = scheduler.subscribe();
        // 带超时接收（3 秒）
        if let Some(batch_enum) = sub.recv_timeout(Duration::from_secs(3)).await {
            match batch_enum {
                HistoricalBatchEnum::OHLCV(batch) => {
                    println!("Fetched OHLCV (single task): {:?} records", batch.data);
                    assert!(!batch.data.is_empty());
                }
                _ => panic!("Expected OHLCV batch"),
            }
        } else {
            warn!("No message within 3 seconds");
        }

        let meta = scheduler.get_node_meta(task_id).await.unwrap();
        assert!(matches!(meta.status, NodeStatus::Completed));

        scheduler.stop();
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_tasks_with_binance_fetcher() -> Result<()> {
        let fetcher = Arc::new(BinanceFetcher::new());
        let scheduler = BaseBackfillScheduler::new(fetcher, 2);

        let base_ctx = make_ctx("1h", 6); // 6 小时数据
        let task_ids = scheduler
            .add_batch_tasks(base_ctx, BackfillDataType::OHLCV, 2 * 60 * 60 * 1000, vec![]) // 每 2 小时切片
            .await?;

        assert!(task_ids.len() >= 3, "batch task count >= 3");

        let sched = scheduler.clone();
        tokio::spawn(async move { sched.run(2).await.unwrap() });

        let mut sub = scheduler.subscribe();
        // 带超时接收（3 秒）
        let mut received = 0;
        while received < task_ids.len() {
            if let Some(batch_enum) = sub.recv_timeout(Duration::from_secs(3)).await {
                match batch_enum {
                    HistoricalBatchEnum::OHLCV(batch) => {
                        println!(
                            "Fetched OHLCV batch slice: {} records (range {:?})",
                            batch.data.len(),
                            batch.range
                        );
                        assert!(!batch.data.is_empty());
                        received += 1;
                    }
                    _ => panic!("Expected OHLCV batch"),
                }
            } else {
                warn!("No message within 3 seconds");
            }
        }

        // 检查所有任务完成
        for id in task_ids {
            let meta = scheduler.get_node_meta(id).await.unwrap();
            assert!(matches!(meta.status, NodeStatus::Completed));
        }

        scheduler.stop();
        Ok(())
    }

    #[tokio::test]
    async fn test_dependency_with_binance_fetcher() -> Result<()> {
        let fetcher = Arc::new(BinanceFetcher::new());
        let scheduler = BaseBackfillScheduler::new(fetcher, 2);

        let parent_id = scheduler.add_task(make_ctx("1h", 1), BackfillDataType::OHLCV, vec![]).await;
        let child_id = scheduler
            .add_task(make_ctx("1h", 1), BackfillDataType::OHLCV, vec![parent_id])
            .await;

        let sched = scheduler.clone();
        tokio::spawn(async move { sched.run(2).await.unwrap() });

        let mut sub = scheduler.subscribe();

        let mut completed_ids = Vec::new();
        while completed_ids.len() < 2 {
            if let Some(batch_enum) = sub.recv_timeout(Duration::from_secs(3)).await {
                match batch_enum {
                    HistoricalBatchEnum::OHLCV(batch) => {
                        println!("Task completed for range {:?}", batch.range);
                        completed_ids.push(batch.range);
                    }
                    _ => panic!("Expected OHLCV batch"),
                }
            } else {
                warn!("No message within 3 seconds");
            }
        }

        let parent_meta = scheduler.get_node_meta(parent_id).await.unwrap();
        let child_meta = scheduler.get_node_meta(child_id).await.unwrap();
        assert!(matches!(parent_meta.status, NodeStatus::Completed));
        assert!(matches!(child_meta.status, NodeStatus::Completed));

        scheduler.stop();
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_subscribers() -> Result<()> {
        let fetcher = Arc::new(BinanceFetcher::new());
        let scheduler = BaseBackfillScheduler::new(fetcher, 2);

        let ctx = make_ctx("1h", 1);
        scheduler.add_task(ctx, BackfillDataType::OHLCV, vec![]).await;

        let sched = scheduler.clone();
        let handle = tokio::spawn(async move { sched.run(1).await.unwrap() });

        // 两个订阅者
        let mut rx1 = scheduler.subscribe();
        let mut rx2 = scheduler.subscribe();

        // 并行等待两个订阅者

        let (b1, b2) = join!(
            rx1.recv_timeout(Duration::from_secs(10)),
            rx2.recv_timeout(Duration::from_secs(10))
        );

        // 解包 Option 并匹配 OHLCV
        if let (Some(HistoricalBatchEnum::OHLCV(batch1)), Some(HistoricalBatchEnum::OHLCV(batch2))) = (b1, b2) {
            println!(
                "Subscriber1 got {} records, Subscriber2 got {} records",
                batch1.data.len(),
                batch2.data.len()
            );
            assert_eq!(batch1.data.len(), batch2.data.len());
            assert!(batch1.data.len() > 0);
        } else {
            panic!("Expected OHLCV batch for both subscribers");
        }

        scheduler.stop();
        handle.await.unwrap(); // 确保 worker 退出
        Ok(())
    }
}

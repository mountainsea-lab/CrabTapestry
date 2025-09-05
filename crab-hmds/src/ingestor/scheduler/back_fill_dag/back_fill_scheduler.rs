use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::{BackfillDataType, BackfillNode, HistoricalBatchEnum, NodeMeta, NodeStatus};
use crate::ingestor::types::FetchContext;
use anyhow::Result;
use dashmap::DashMap;
use ms_tracing::tracing_utils::internal::{error, info, warn};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify, mpsc};

/// BaseBackfillScheduler
pub struct BaseBackfillScheduler<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fetcher: Arc<F>,
    nodes: DashMap<usize, Arc<BackfillNode>>,
    ready_queue: Mutex<Vec<usize>>,
    notify: Notify,
    output_tx: mpsc::Sender<HistoricalBatchEnum>,
    shutdown: Arc<AtomicBool>,
    retry_limit: usize,
    next_id: AtomicUsize,
}

impl<F> BaseBackfillScheduler<F>
where
    F: HistoricalFetcherExt + 'static,
{
    /// 创建调度器
    pub fn new(fetcher: Arc<F>, output_tx: mpsc::Sender<HistoricalBatchEnum>, retry_limit: usize) -> Arc<Self> {
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

    /// 添加 DAG 节点
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
            }),
            dependencies: depends_on.iter().copied().collect(),
            dependents: Mutex::new(HashSet::new()), // ✅ 加锁，支持后续插入
        });

        // 更新上游节点 dependents
        for &dep_id in &depends_on {
            if let Some(dep_node) = self.nodes.get(&dep_id) {
                let mut dependents = dep_node.dependents.lock().await;
                dependents.insert(id);
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

    /// 停止调度器
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// 将节点入就绪队列
    async fn enqueue_ready(&self, node_id: usize) {
        let mut queue = self.ready_queue.lock().await;
        queue.push(node_id);
        self.notify.notify_one();
    }

    /// 调度器主循环
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("BackfillScheduler started");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutdown detected, exiting scheduler loop");
                break;
            }

            let node_id = {
                let mut q = self.ready_queue.lock().await;
                q.pop()
            };

            let node_id = match node_id {
                Some(id) => id,
                None => {
                    self.notify.notified().await;
                    continue;
                }
            };

            let node = match self.nodes.get(&node_id) {
                Some(n) => n.clone(),
                None => continue,
            };

            // 检查依赖完成
            let deps_done = node.dependencies.iter().all(|dep_id| {
                if let Some(dep_node) = self.nodes.get(dep_id) {
                    matches!(dep_node.meta.blocking_lock().status, NodeStatus::Completed)
                } else {
                    false
                }
            });

            if !deps_done {
                self.enqueue_ready(node_id).await;
                continue;
            }

            // 节点状态置为 Running
            {
                let mut meta = node.meta.lock().await;
                meta.status = NodeStatus::Running;
            }

            let fetcher = self.fetcher.clone();
            let output_tx = self.output_tx.clone();
            let shutdown = self.shutdown.clone();
            let scheduler = self.clone();
            let node_clone = node.clone();
            let retry_limit = self.retry_limit;

            // 节点异步执行
            tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        info!("Node {} stopped due to shutdown", node_clone.id);
                        break;
                    }

                    let mut meta = node_clone.meta.lock().await;
                    meta.retry_count += 1;
                    let attempt = meta.retry_count;
                    drop(meta);

                    let fetch_res = match node_clone.data_type {
                        BackfillDataType::OHLCV => fetcher
                            .fetch_ohlcv(node_clone.ctx.clone())
                            .await
                            .map(HistoricalBatchEnum::OHLCV),
                        BackfillDataType::Tick => {
                            fetcher.fetch_ticks(node_clone.ctx.clone()).await.map(HistoricalBatchEnum::Tick)
                        }
                        BackfillDataType::Trade => Err(anyhow::anyhow!("Trade fetch not implemented")),
                    };

                    match fetch_res {
                        Ok(batch) => {
                            // 非阻塞发送
                            if output_tx.try_send(batch).is_err() {
                                warn!("Output channel full, node {} batch dropped", node_clone.id);
                            }
                            let mut meta = node_clone.meta.lock().await;
                            meta.status = NodeStatus::Completed;
                            info!("Node {} completed", node_clone.id);
                            break;
                        }
                        Err(err) => {
                            warn!(error=?err, "Node {} fetch failed attempt {}", node_clone.id, attempt);
                            if attempt >= retry_limit {
                                let mut meta = node_clone.meta.lock().await;
                                meta.status = NodeStatus::Failed;
                                error!("Node {} failed after {} retries", node_clone.id, attempt);
                                break;
                            }
                            tokio::time::sleep(tokio::time::Duration::from_secs(2_u64.pow(attempt as u32))).await;
                        }
                    }
                }

                // 获取 dependents 的克隆
                let dependents = node_clone.dependents.lock().await;
                let dependents_clone: Vec<usize> = dependents.iter().copied().collect();
                drop(dependents); // 释放锁

                // 遍历下游节点
                for dep_id in dependents_clone {
                    scheduler.enqueue_ready(dep_id).await;
                }
            });
        }

        info!("BackfillScheduler stopped");
        Ok(())
    }
}

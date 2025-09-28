use crate::ingestor::types::{FetchContext, HistoricalBatch, OHLCVRecord, TickRecord, TradeRecord};
use futures_util::{Stream, stream};
use ms_tracing::tracing_utils::internal::{error, warn};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tokio::time::timeout;

pub mod back_fill_dag;
pub mod service;

/// 数据类型枚举
#[derive(Debug, Clone, Copy)]
pub enum BackfillDataType {
    OHLCV,
    Tick,
    Trade,
}

/// 泛型历史数据枚举，用于统一输出
#[derive(Debug, Clone)]
pub enum HistoricalBatchEnum {
    OHLCV(HistoricalBatch<OHLCVRecord>),
    Tick(HistoricalBatch<TickRecord>),
    Trade(HistoricalBatch<TradeRecord>),
}

/// Node execution status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

/// Node meta for monitoring
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub status: NodeStatus,
    pub retry_count: usize,
    pub last_error: Option<String>,
    pub started_at: Option<Instant>,
    pub finished_at: Option<Instant>,
}

/// DAG 节点
pub struct BackfillNode {
    pub id: usize,
    pub ctx: Arc<FetchContext>,
    pub data_type: BackfillDataType,
    meta: Mutex<NodeMeta>, // 合并 status + retry + 依赖
    dependencies: HashSet<usize>,
    pub dependents: Mutex<HashSet<usize>>, // ✅ 线程安全可变
}

/// 包装后的订阅者
pub struct OutputSubscriber {
    inner: broadcast::Receiver<HistoricalBatchEnum>,
}

impl OutputSubscriber {
    pub fn new(inner: broadcast::Receiver<HistoricalBatchEnum>) -> Self {
        Self { inner }
    }

    /// 安全 recv：自动处理 Lagged 和 Closed
    pub async fn recv(&mut self) -> Option<HistoricalBatchEnum> {
        loop {
            match self.inner.recv().await {
                Ok(batch) => return Some(batch),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("⚠️ Subscriber lagged and missed {n} messages");
                    // 跳过，继续等下一条
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    error!("❌ Channel closed");
                    return None;
                }
            }
        }
    }
    /// 带超时的异步接收
    pub async fn recv_timeout(&mut self, dur: Duration) -> Option<HistoricalBatchEnum> {
        match timeout(dur, self.recv()).await {
            Ok(res) => res,
            Err(_) => {
                // warn!("⏰ recv timed out after {:?}", dur);
                None
            }
        }
    }

    /// 非阻塞接收（立即返回）
    pub fn try_recv(&mut self) -> Option<HistoricalBatchEnum> {
        match self.inner.try_recv() {
            Ok(batch) => Some(batch),
            Err(broadcast::error::TryRecvError::Empty) => None,
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                warn!("⚠️ Subscriber lagged and missed {n} messages");
                None
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                error!("❌ Channel closed");
                None
            }
        }
    }

    /// 将 Subscriber 转换为 Stream
    pub fn into_stream(self) -> impl Stream<Item = HistoricalBatchEnum> {
        stream::unfold(self, |mut sub| async move {
            match sub.recv().await {
                Some(batch) => Some((batch, sub)),
                None => None,
            }
        })
    }
}

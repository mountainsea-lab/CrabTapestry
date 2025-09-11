use crate::ingestor::types::{FetchContext, HistoricalBatch, OHLCVRecord, TickRecord, TradeRecord};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, broadcast};

pub mod back_fill_dag;

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
                    eprintln!("⚠️ Subscriber lagged and missed {n} messages");
                    // 跳过，继续等下一条
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    eprintln!("❌ Channel closed");
                    return None;
                }
            }
        }
    }
}

use crate::ingestor::types::{FetchContext, HistoricalBatch, OHLCVRecord, TickRecord, TradeRecord};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

mod back_fill_dag;

/// 数据类型枚举
#[derive(Debug, Clone, Copy)]
pub enum BackfillDataType {
    OHLCV,
    Tick,
    Trade,
}

/// 泛型历史数据枚举，用于统一输出
#[derive(Debug)]
pub enum HistoricalBatchEnum {
    OHLCV(HistoricalBatch<OHLCVRecord>),
    Tick(HistoricalBatch<TickRecord>),
    Trade(HistoricalBatch<TradeRecord>),
}

/// 节点状态
#[derive(Debug, Clone, Copy)]
pub enum NodeStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug)]
struct NodeMeta {
    status: NodeStatus,
    retry_count: usize,
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

// /// 节点依赖信息
// #[derive(Debug, Clone)]
// pub struct BackfillTask {
//     pub ctx: Arc<FetchContext>,      // 拉取上下文
//     pub data_type: BackfillDataType, // 数据类型
//     pub depends_on: Vec<usize>,      // DAG 前置节点 ID
// }

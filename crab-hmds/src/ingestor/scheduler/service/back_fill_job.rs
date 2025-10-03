use crate::ingestor::historical::HistoricalFetcherExt;
use crate::ingestor::scheduler::BackfillDataType;
use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crate::ingestor::scheduler::service::MarketKey;
use crate::ingestor::types::FetchContext;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillPriority {
    Gap = 1,
    Lookback = 2,
}

pub struct BackfillJob<F>
where
    F: HistoricalFetcherExt + 'static,
{
    pub(crate) ctx: Arc<FetchContext>,
    pub(crate) data_type: BackfillDataType,
    pub(crate) priority: BackfillPriority,
    pub(crate) retries: usize,
    pub(crate) scheduler: Arc<BaseBackfillScheduler<F>>,
    pub(crate) key: MarketKey,
    pub(crate) step_millis: i64,
}

// BinaryHeap 优先级排序
impl<F> Ord for BackfillJob<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap 默认是最大堆，优先级小的先执行需要反转
        (other.priority as usize).cmp(&(self.priority as usize))
    }
}

impl<F> PartialOrd for BackfillJob<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<F> PartialEq for BackfillJob<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<F> Eq for BackfillJob<F> where F: HistoricalFetcherExt + 'static {}

use std::fmt;

impl<F> fmt::Debug for BackfillJob<F>
where
    F: HistoricalFetcherExt + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackfillJob")
            .field("ctx", &self.ctx)
            .field("data_type", &self.data_type)
            .field("priority", &self.priority)
            .field("retries", &self.retries)
            .field("key", &self.key)
            .field("step_millis", &self.step_millis)
            .finish()
    }
}

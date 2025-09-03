// ---------- Deduplicator 接口 ----------

use crate::ingestor::types::OHLCVRecord;

pub struct Deduplicator;

impl Deduplicator {
    /// 批量去重，去掉历史和实时数据重叠部分
    /// batch remove duplicate historical and realtime data
    pub fn remove_overlap(events: Vec<OHLCVRecord>) -> Vec<OHLCVRecord> {
        // 这里先留接口，不实现具体逻辑
        events
    }
}

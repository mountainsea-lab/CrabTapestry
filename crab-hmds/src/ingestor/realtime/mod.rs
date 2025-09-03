
// ---------- 实时数据订阅器接口 ----------
// ----------------- RealtimeSubscriber Interface  -----------------

use crate::ingestor::types::OHLCVRecord;
use anyhow::{ Result};
#[async_trait::async_trait]
pub trait RealtimeSubscriber: Send + Sync {
    /// 订阅实时事件流
    async fn subscribe(&self, symbol: &str) -> Result<tokio::sync::mpsc::Receiver<OHLCVRecord>>;
}
#[async_trait::async_trait]
pub trait Storage {
    async fn last_time(&self) -> Option<i64>;
    async fn query_range(&self, start: i64, end: i64) -> Vec<OHLCVRecord>;
    async fn write_batch(&self, events: Vec<OHLCVRecord>);
    async fn update_last_time(&self, ts: i64);
}

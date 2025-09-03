#[async_trait::async_trait]
pub trait HistoricalFetcher {
    async fn fetch_historical(&self, start: i64, end: i64) -> Vec<DataEvent>;
}
use crate::ingestor::deduplicator::Deduplicator;

#[async_trait::async_trait]
pub trait HistoricalFetcher {
    async fn fetch_range(&self, start: i64, end: i64) -> Vec<Deduplicator>;
}

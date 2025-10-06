use crate::data::cache::bar_key::BarKey;
use crate::data::cache::series_entry::SeriesEntry;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct BarCacheManager {
    caches: Arc<DashMap<BarKey, Arc<SeriesEntry>>>,
    default_capacity: usize,
}

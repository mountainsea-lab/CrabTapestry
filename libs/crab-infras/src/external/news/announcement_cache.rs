use crate::external::news::AnnouncementEvent;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AnnouncementCache {
    cache: Arc<Mutex<VecDeque<AnnouncementEvent>>>,
    capacity: usize,
}

impl AnnouncementCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    /// 添加新公告
    pub async fn push(&self, event: AnnouncementEvent) {
        let mut cache = self.cache.lock().await;
        if cache.len() == self.capacity {
            cache.pop_front(); // 移除最早的
        }
        cache.push_back(event);
    }

    /// 获取当前缓存所有公告
    pub async fn get_all(&self) -> Vec<AnnouncementEvent> {
        let cache = self.cache.lock().await;
        cache.iter().cloned().collect()
    }
}

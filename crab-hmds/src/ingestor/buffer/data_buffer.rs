use async_stream::stream;
use crossbeam::queue::SegQueue;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use ms_tracing::tracing_utils::internal::warn;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, Semaphore};

/// 最大容量策略
#[derive(Debug, Clone, Copy)]
pub enum CapacityStrategy {
    /// 超出容量直接丢弃新数据
    DropNewest,
    /// 超出容量丢弃最老数据
    DropOldest,
    /// 阻塞等待直到有空间（基于 Semaphore）
    Block,
}

/// 高性能异步 DataBuffer
/// 支持批量处理、容量限制、高并发消费
#[derive(Debug)]
pub struct DataBuffer<T>
where
    T: Send + Sync + 'static,
{
    queue: Arc<SegQueue<Arc<T>>>,
    notify: Arc<Notify>,
    max_capacity: Option<usize>,
    batch_notify_size: usize,
    batch_counter: AtomicUsize,
    capacity_strategy: CapacityStrategy,
    permits: Option<Arc<Semaphore>>, // Block 策略专用
}

impl<T> DataBuffer<T>
where
    T: Send + Sync + 'static,
{
    /// 创建新 DataBuffer
    pub fn new(
        max_capacity: Option<usize>,
        batch_notify_size: usize,
        capacity_strategy: CapacityStrategy,
    ) -> Arc<Self> {
        let permits = match (capacity_strategy, max_capacity) {
            (CapacityStrategy::Block, Some(max)) => Some(Arc::new(Semaphore::new(max))),
            _ => None,
        };

        Arc::new(Self {
            queue: Arc::new(SegQueue::new()),
            notify: Arc::new(Notify::new()),
            max_capacity,
            batch_notify_size,
            batch_counter: AtomicUsize::new(0),
            capacity_strategy,
            permits,
        })
    }

    /// 入队单条数据（Block 策略下为异步）
    pub async fn push(self: &Arc<Self>, item: T) {
        if let Some(max) = self.max_capacity {
            if self.queue.len() >= max {
                match self.capacity_strategy {
                    CapacityStrategy::DropNewest => {
                        warn!("DataBuffer max_capacity {} reached, dropping newest item", max);
                        return;
                    }
                    CapacityStrategy::DropOldest => {
                        let _ = self.queue.pop();
                    }
                    CapacityStrategy::Block => {
                        if let Some(permits) = &self.permits {
                            // 等待一个 permit
                            let _ = permits.acquire().await.unwrap();
                        }
                    }
                }
            }
        }

        self.queue.push(Arc::new(item));
        let count = self.batch_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.batch_notify_size {
            self.batch_counter.store(0, Ordering::Relaxed);
            self.notify.notify_waiters();
        }
    }

    /// 批量入队
    pub async fn push_batch(self: &Arc<Self>, items: Vec<T>) {
        let mut added = 0;
        for item in items {
            if let Some(max) = self.max_capacity {
                if self.queue.len() >= max {
                    match self.capacity_strategy {
                        CapacityStrategy::DropNewest => break,
                        CapacityStrategy::DropOldest => {
                            let _ = self.queue.pop();
                        }
                        CapacityStrategy::Block => {
                            if let Some(permits) = &self.permits {
                                let _ = permits.acquire().await.unwrap();
                            }
                        }
                    }
                }
            }
            self.queue.push(Arc::new(item));
            added += 1;
        }

        if added > 0 {
            let count = self.batch_counter.fetch_add(added, Ordering::Relaxed) + added;
            if count >= self.batch_notify_size {
                self.batch_counter.store(0, Ordering::Relaxed);
                self.notify.notify_waiters();
            }
        }
    }

    /// 异步流式消费（批量 yield）
    pub fn consume_stream(self: &Arc<Self>, batch_size: usize) -> BoxStream<'static, Vec<Arc<T>>> {
        let queue = Arc::clone(&self.queue);
        let notify = Arc::clone(&self.notify);
        let permits = self.permits.clone();

        let s = stream! {
            loop {
                let mut batch = Vec::with_capacity(batch_size);

                while batch.len() < batch_size {
                    if let Some(item) = queue.pop() {
                        batch.push(item);
                        if let Some(p) = &permits {
                            p.add_permits(1); // 消费后释放 permit
                        }
                    } else {
                        break;
                    }
                }

                if !batch.is_empty() {
                    yield batch;
                    continue;
                }

                notify.notified().await;
            }
        };

        s.boxed()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn clear(&self) {
        while let Some(_) = self.queue.pop() {}
        self.batch_counter.store(0, Ordering::Relaxed);
        if let Some(p) = &self.permits {
            if let Some(max) = self.max_capacity {
                p.add_permits(max.saturating_sub(self.queue.len()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio::task;

    #[tokio::test]
    async fn test_drop_newest_strategy() {
        let buf = DataBuffer::new(Some(5), 1, CapacityStrategy::DropNewest);
        for i in 0..10 {
            buf.push(i).await;
        }
        assert!(buf.len() <= 5);
    }

    #[tokio::test]
    async fn test_drop_oldest_strategy() {
        let buf = DataBuffer::new(Some(5), 1, CapacityStrategy::DropOldest);
        for i in 0..10 {
            buf.push(i).await;
        }
        assert_eq!(buf.len(), 5);
    }

    #[tokio::test]
    async fn test_block_strategy_no_loss() {
        let buf = DataBuffer::new(Some(5), 1, CapacityStrategy::Block);

        // consumer 在后台跑，持续消费
        let consumer = {
            let buf = Arc::clone(&buf);
            task::spawn(async move {
                let mut stream = buf.consume_stream(2);
                let mut consumed = 0;
                while let Some(batch) = stream.next().await {
                    consumed += batch.len();
                    if consumed >= 10 {
                        break;
                    }
                }
                consumed
            })
        };

        // producer 并发写入
        let producer = {
            let buf = Arc::clone(&buf);
            task::spawn(async move {
                for i in 0..10 {
                    buf.push(i).await;
                }
            })
        };

        producer.await.unwrap();
        let consumed = consumer.await.unwrap();
        assert_eq!(consumed, 10, "Block strategy should not drop data");
    }

    #[tokio::test]
    async fn test_high_concurrency() {
        let buf = DataBuffer::new(Some(1000), 10, CapacityStrategy::Block);

        let consumers = {
            let buf = Arc::clone(&buf);
            task::spawn(async move {
                let mut stream = buf.consume_stream(50);
                let mut consumed = 0;
                while let Some(batch) = stream.next().await {
                    consumed += batch.len();
                    if consumed >= 100_000 {
                        break;
                    }
                }
                consumed
            })
        };

        let mut producers = Vec::new();
        for _ in 0..4 {
            let buf = Arc::clone(&buf);
            producers.push(task::spawn(async move {
                for i in 0..25_000 {
                    buf.push(i).await;
                }
            }));
        }

        for p in producers {
            p.await.unwrap();
        }

        let consumed = consumers.await.unwrap();
        assert_eq!(consumed, 100_000);
    }
}

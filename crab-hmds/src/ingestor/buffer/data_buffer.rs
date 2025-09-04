use async_stream::stream;
use crossbeam_queue::SegQueue;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use log::warn;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

/// 最大容量策略
#[derive(Debug, Clone, Copy)]
pub enum CapacityStrategy {
    /// 超出容量直接丢弃新数据
    DropNewest,
    /// 超出容量丢弃最老数据
    DropOldest,
    /// 阻塞等待直到有空间
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
}

impl<T> DataBuffer<T>
where
    T: Send + Sync + 'static,
{
    /// 创建新 DataBuffer
    pub fn new(max_capacity: Option<usize>, batch_notify_size: usize, capacity_strategy: CapacityStrategy) -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            notify: Arc::new(Notify::new()),
            max_capacity,
            batch_notify_size,
            batch_counter: AtomicUsize::new(0),
            capacity_strategy,
        }
    }

    /// 入队单条数据
    pub fn push(&self, item: T) {
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
                        // 简单 busy-wait，可用 async-aware semaphore 改进
                        while self.queue.len() >= max {
                            std::thread::yield_now();
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
    pub fn push_batch(&self, items: Vec<T>) {
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
                            while self.queue.len() >= max {
                                std::thread::yield_now();
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
    pub fn consume_stream(&self, batch_size: usize) -> BoxStream<'static, Vec<Arc<T>>> {
        let queue = Arc::clone(&self.queue);
        let notify = Arc::clone(&self.notify);

        let s = stream! {
            loop {
                // 新建 batch
                let mut batch = Vec::with_capacity(batch_size);

                // 拉取数据填充 batch
                while batch.len() < batch_size {
                    if let Some(item) = queue.pop() {
                        batch.push(item);
                    } else {
                        break;
                    }
                }

                // 如果有数据就 yield
                if !batch.is_empty() {
                    yield batch;
                    continue; // 重新开始循环，batch 已经 move，不会再访问
                }

                // 队列空了，等待 notify
                notify.notified().await;
            }
        };

        s.boxed()
    }

    /// 当前队列长度
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// 清空队列
    pub fn clear(&self) {
        while let Some(_) = self.queue.pop() {}
        self.batch_counter.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

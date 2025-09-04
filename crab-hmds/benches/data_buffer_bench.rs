use crab_hmds::ingestor::buffer::data_buffer::{CapacityStrategy, DataBuffer};
use criterion::async_executor::AsyncExecutor;
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::{StreamExt, join};
use std::sync::Arc;
use tokio::runtime::Runtime;

// 包装 Arc<Runtime> 支持 AsyncExecutor
#[derive(Clone)]
struct TokioExecutorArc(Arc<Runtime>);

impl AsyncExecutor for TokioExecutorArc {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.0.block_on(future)
    }
}

fn bench_data_buffer(c: &mut Criterion) {
    let num_items = 1_000_000;
    let batch_size = 10_000; // 批量大小

    // 每个 benchmark 使用独立 Runtime
    let mut group = c.benchmark_group("DataBuffer Throughput");
    group.sample_size(10);

    // 用 Arc 包裹 Runtime，可以多次 clone
    let rt = TokioExecutorArc(Arc::new(Runtime::new().unwrap()));

    group.bench_function("1M_push_consume_block_batch", |b| {
        let rt_clone = rt.clone(); // 每次使用 clone
        b.to_async(rt_clone).iter(|| async {
            let buf = DataBuffer::new(Some(200_000), 100, CapacityStrategy::Block);

            let buf_arc = Arc::new(buf);
            let producer_buf = buf_arc.clone();
            let consumer_buf = buf_arc.clone();

            // 生产者：批量 push
            let producer = async move {
                let mut current = 0;
                let mut batch = Vec::with_capacity(batch_size);

                while current < num_items {
                    batch.clear();
                    let end = (current + batch_size).min(num_items);
                    batch.extend(current..end);
                    producer_buf.push_batch(batch.clone()).await;
                    current = end;
                }
            };

            // 消费者：批量消费
            let consumer = async move {
                let mut stream = consumer_buf.consume_stream(batch_size);
                let mut consumed = 0;
                while consumed < num_items {
                    if let Some(batch) = stream.next().await {
                        consumed += batch.len();
                    }
                }
                consumed
            };

            // 并发执行生产者和消费者
            let (_prod_res, consumed) = join!(producer, consumer);
            assert_eq!(consumed, num_items);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_data_buffer);
criterion_main!(benches);

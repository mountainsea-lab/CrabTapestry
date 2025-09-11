use crab_hmds::ingestor::historical::fetcher::binance_fetcher::BinanceFetcher;
use crab_hmds::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
use crab_hmds::ingestor::scheduler::{BackfillDataType, HistoricalBatchEnum, OutputSubscriber};
use crab_hmds::ingestor::types::{FetchContext, HistoricalSource};
use std::sync::Arc;
use std::time::Duration;

/// 配置交易对和周期
struct MarketConfig {
    exchange: String,
    symbols: Vec<String>,
    periods: Vec<String>,
    past_hours: i64, // 初始历史回溯
}

/// 构建 HistoricalSource
fn make_source_for_exchange(exchange: &str) -> HistoricalSource {
    HistoricalSource {
        name: format!("{} API", exchange),
        exchange: exchange.to_string(),
        last_success_ts: 0,
        last_fetch_ts: 0,
        batch_size: 0,
        supports_tick: false,
        supports_trade: false,
        supports_ohlcv: true,
    }
}

/// 构建 FetchContext
fn make_ctx(exchange: &str, symbol: &str, period: &str, past_hours: i64) -> Arc<FetchContext> {
    FetchContext::new_with_past(
        make_source_for_exchange(exchange),
        exchange,
        symbol,
        Some(period),
        Some(past_hours),
        None,
    )
}

/// 启动自动拉取调度器
pub async fn start_auto_scheduler(configs: Vec<MarketConfig>) -> Arc<BaseBackfillScheduler<BinanceFetcher>> {
    let fetcher = Arc::new(BinanceFetcher::new());
    let scheduler = BaseBackfillScheduler::new(fetcher.clone(), 3); // retry_limit=3

    let sched_clone = scheduler.clone();
    tokio::spawn(async move {
        // 启动 worker pool
        sched_clone.run(4).await.unwrap(); // 4 workers
    });

    // 批量历史任务初始化
    for cfg in &configs {
        for symbol in &cfg.symbols {
            for period in &cfg.periods {
                let ctx = make_ctx(&cfg.exchange, symbol, period, cfg.past_hours);
                // 可用 batch_tasks 切片历史数据，例如每 2 小时一片
                let step_millis = 2 * 60 * 60 * 1000;
                let _task_ids = scheduler
                    .add_batch_tasks(ctx, BackfillDataType::OHLCV, step_millis, vec![])
                    .await;
            }
        }
    }

    // 增量拉取：每隔 period 对应时间生成新任务
    let scheduler_clone = scheduler.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60)); // 每小时
        loop {
            interval.tick().await;
            for cfg in &configs {
                for symbol in &cfg.symbols {
                    for period in &cfg.periods {
                        // 这里可以根据 last_fetch_ts 或数据库记录生成增量 ctx
                        let ctx = make_ctx(&cfg.exchange, symbol, period, 0); // 0 表示最新增量
                        let _ = scheduler_clone.add_task(ctx, BackfillDataType::OHLCV, vec![]).await;
                    }
                }
            }
        }
    });

    scheduler
}

/// 示例订阅消费历史数据
pub async fn consume_historical_data(mut subscriber: OutputSubscriber) {
    while let Some(batch) = subscriber.recv().await {
        match batch {
            HistoricalBatchEnum::OHLCV(batch) => {
                println!("Got OHLCV batch: {} records, range {:?}", batch.data.len(), batch.range);
            }
            _ => {}
        }
    }
}

/// 服务启动示例
#[tokio::main]
async fn main() {
    let configs = vec![MarketConfig {
        exchange: "binance".to_string(),
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        periods: vec!["1h".to_string(), "4h".to_string()],
        past_hours: 24,
    }];

    let scheduler = start_auto_scheduler(configs).await;

    // 创建订阅者消费数据
    let subscriber = scheduler.subscribe();
    tokio::spawn(async move {
        consume_historical_data(subscriber).await;
    });

    // 服务可继续执行其他逻辑
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

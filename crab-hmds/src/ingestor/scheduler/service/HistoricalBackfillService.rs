// use std::sync::Arc;
// use tokio::task;
// use chrono::{Utc, Duration};
// use crate::ingestor::historical::HistoricalFetcherExt;
// use crate::ingestor::scheduler::back_fill_dag::back_fill_scheduler::BaseBackfillScheduler;
// use crate::ingestor::scheduler::BackfillDataType;
// use crate::ingestor::scheduler::service::BackfillMetaStore;
// use crate::ingestor::types::FetchContext;
//
// pub struct HistoricalBackfillService<F>
// where
//     F: HistoricalFetcherExt + 'static,
// {
//     scheduler: Arc<BaseBackfillScheduler<F>>,
//     db: Arc<dyn BackfillMetaStore>, // 数据库抽象接口
// }
//
// impl<F> HistoricalBackfillService<F>
// where
//     F: HistoricalFetcherExt + 'static,
// {
//     pub fn new(scheduler: Arc<BaseBackfillScheduler<F>>, db: Arc<dyn BackfillMetaStore>) -> Self {
//         Self { scheduler, db }
//     }
//
//     /// 启动时初始化：维护最近一段数据
//     pub async fn init_recent_tasks(
//         &self,
//         exchanges: &[&str],
//         symbols: &[&str],
//         periods: &[&str],
//         past_hours: i64,
//         step_millis: i64,
//         data_type: BackfillDataType,
//     ) {
//         for &exchange in exchanges {
//             for &symbol in symbols {
//                 for &period in periods {
//                     let ctx = Arc::new(FetchContext::new_with_past_for_symbol(
//                         exchange, symbol, period, past_hours,
//                     ));
//
//                     // 生成任务
//                     self.scheduler
//                         .add_batch_tasks(ctx.clone(), data_type.clone(), step_millis, vec![])
//                         .await;
//
//                     // 更新 db 中的 last_synced_end_time
//                     self.db.update_last_synced(exchange, symbol, period, Utc::now()).await;
//                 }
//             }
//         }
//     }
//
//     /// 后台任务：持续回溯更久远的数据
//     pub async fn backfill_historical(
//         &self,
//         exchanges: &[&str],
//         symbols: &[&str],
//         periods: &[&str],
//         step_millis: i64,
//         data_type: BackfillDataType,
//         lookback_days: i64, // 往过去多少天
//     ) {
//         let now = Utc::now();
//         let oldest_allowed = now - Duration::days(lookback_days);
//
//         for &exchange in exchanges {
//             for &symbol in symbols {
//                 for &period in periods {
//                     let oldest = self.db.get_oldest_synced(exchange, symbol, period).await;
//
//                     if oldest > oldest_allowed {
//                         let ctx = Arc::new(FetchContext::new_with_range(
//                             exchange,
//                             symbol,
//                             period,
//                             oldest - Duration::days(7), // 每次往前补 7 天
//                             oldest,
//                         ));
//
//                         self.scheduler
//                             .add_batch_tasks(ctx.clone(), data_type.clone(), step_millis, vec![])
//                             .await;
//
//                         self.db.update_oldest_synced(exchange, symbol, period, oldest - Duration::days(7)).await;
//                     }
//                 }
//             }
//         }
//     }
//
//     /// 启动调度器
//     pub async fn run(&self, worker_count: usize) {
//         let sched = self.scheduler.clone();
//         task::spawn(async move {
//             sched.run(worker_count).await.unwrap();
//         });
//     }
// }
//
// //
// // let scheduler = Arc::new(BaseBackfillScheduler::new(fetcher, 3));
// // let db = Arc::new(PostgresMetaStore::new(pool));
// // let service = HistoricalBackfillService::new(scheduler.clone(), db.clone());
// //
// // // 启动调度器
// // service.run(4).await;
// //
// // // 初始化最近 48 小时的数据
// // service
// // .init_recent_tasks(
// // &["binance"],
// // &["BTCUSDT", "ETHUSDT"],
// // &["1m", "1h"],
// // 48,
// // 60 * 60 * 1000,
// // BackfillDataType::OHLCV,
// // )
// // .await;
// //
// // // 后台回溯半年数据
// // tokio::spawn({
// // let svc = service.clone();
// // async move {
// // loop {
// // svc.backfill_historical(
// // &["binance"],
// // &["BTCUSDT", "ETHUSDT"],
// // &["1m", "1h"],
// // 60 * 60 * 1000,
// // BackfillDataType::OHLCV,
// // 180, // 180 天
// // ).await;
// //
// // tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
// // }
// // }
// // });

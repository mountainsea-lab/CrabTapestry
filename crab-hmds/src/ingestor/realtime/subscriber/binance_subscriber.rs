// use crate::ingestor::realtime::SubscriberStatus;
// use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
// use crate::ingestor::types::PublicTradeEvent;
// use async_trait::async_trait;
// use barter_data::barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
// use barter_data::exchange::binance::futures::BinanceFuturesUsd;
// use barter_data::streams::Streams;
// use barter_data::streams::reconnect::stream::ReconnectingStream;
// use barter_data::subscription::trade::PublicTrades;
// use futures_util::StreamExt;
// use std::sync::Arc;
// use tokio::sync::mpsc::{Receiver, Sender};
// use tokio::time::Duration;
//
// pub struct BinanceSubscriber {
//     pub exchange: String,
//     sender: Sender<PublicTradeEvent>,
//     status: Arc<tokio::sync::Mutex<SubscriberStatus>>,
// }
//
// impl BinanceSubscriber {
//     pub fn new(sender: Sender<PublicTradeEvent>) -> Self {
//         Self {
//             exchange: "binance".into(),
//             sender,
//             status: Arc::new(tokio::sync::Mutex::new(SubscriberStatus::Disconnected)),
//         }
//     }
//
//     // 内部订阅逻辑，接收 'static symbols
//     pub async fn run_subscribe(&self, symbols: Vec<Arc<str>>) {
//         let max_retries = 5;
//         let mut retry_count = 0;
//
//         loop {
//             // 批量订阅多个币种的市场数据流
//             let streams_builder = Streams::<PublicTrades>::builder().subscribe(
//                 symbols
//                     .iter()
//                     .map(|symbol| {
//                         (
//                             BinanceFuturesUsd::default(),
//                             Arc::clone(symbol),
//                             "usdt",
//                             MarketDataInstrumentKind::Perpetual,
//                             PublicTrades,
//                         )
//                     })
//                     .collect::<Vec<_>>(),
//             );
//
//             let streams = match streams_builder.init().await {
//                 Ok(s) => s,
//                 Err(err) => {
//                     eprintln!("Failed to init streams: {err:?}");
//                     if retry_count < max_retries {
//                         retry_count += 1;
//                         tokio::time::sleep(Duration::from_secs(2u64.pow(retry_count as u32))).await;
//                         continue;
//                     } else {
//                         eprintln!("Max retries reached, aborting subscription");
//                         break;
//                     }
//                 }
//             };
//
//             // 连接多个 symbol stream
//             let mut joined_stream = streams
//                 .select_all()
//                 .with_error_handler(|error| eprintln!("Stream error: {:?}", error));
//
//             // 更新状态
//             *self.status.lock().await = SubscriberStatus::Connected;
//
//             // 监听事件
//             while let Some(event) = joined_stream.next().await {
//                 // barter-data -> PublicTradeEvent
//                 let public_trade_event: PublicTradeEvent = event.into();
//
//                 // 发送到 pipeline channel
//                 if let Err(err) = self.sender.send(public_trade_event).await {
//                     eprintln!("Failed to send event: {:?}", err);
//                 }
//             }
//
//             // Stream 结束或错误
//             eprintln!("Stream ended or errored, retrying...");
//             if retry_count < max_retries {
//                 retry_count += 1;
//                 tokio::time::sleep(Duration::from_secs(2u64.pow(retry_count as u32))).await;
//             } else {
//                 eprintln!("Max retries reached, aborting.");
//                 break;
//             }
//         }
//
//         // 订阅结束，更新状态
//         *self.status.lock().await = SubscriberStatus::Disconnected;
//     }
// }
//
// #[async_trait]
// impl RealtimeSubscriber for BinanceSubscriber {
//     fn exchange(&self) -> &str {
//         &self.exchange
//     }
//
//     async fn subscribe_symbols(&self, symbols: &[&str])
//                                -> Result<Receiver<PublicTradeEvent>, anyhow::Error>
//     {
//         let (tx, rx) = tokio::sync::mpsc::channel(1024);
//         let subscriber = Self::new(tx.clone());
//
//         // 转成 Arc<str>，确保 'static
//         let symbols_vec: Vec<Arc<str>> = symbols.iter().map(|s| Arc::from(*s)).collect();
//
//         tokio::spawn(async move {
//             subscriber.run_subscribe(symbols_vec).await;
//         });
//
//         Ok(rx)
//     }
//
//     async fn unsubscribe_symbols(&self, _symbols: &[&str]) -> Result<(), anyhow::Error> {
//         // TODO: barter-data 暂未提供动态取消订阅，可后续封装
//         Ok(())
//     }
//
//     fn stop(&mut self) -> Result<(), anyhow::Error> {
//         // TODO: barter-data 暂未提供优雅 stop，可后续实现
//         Ok(())
//     }
//
//     fn status(&self) -> SubscriberStatus {
//         futures::executor::block_on(self.status.lock()).clone()
//     }
// }

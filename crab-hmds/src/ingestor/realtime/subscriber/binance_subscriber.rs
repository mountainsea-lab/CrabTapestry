use crate::ingestor::realtime::SubscriberStatus;
use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use anyhow::Result;
use barter_data::exchange::binance::futures::BinanceFuturesUsd;
use barter_data::streams::Streams;
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_data::subscription::trade::PublicTrades;
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use crab_infras::aggregator::types::PublicTradeEvent;
use futures_util::StreamExt;
use ms_tracing::tracing_utils::internal::{info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, broadcast};

/// Binance 实时订阅器
pub struct BinanceSubscriber {
    exchange: String,
    broadcast_tx: broadcast::Sender<PublicTradeEvent>,
    status: Arc<Mutex<SubscriberStatus>>,
    shutdown: Arc<AtomicBool>,
}

impl BinanceSubscriber {
    pub fn new(broadcast_tx: broadcast::Sender<PublicTradeEvent>) -> Self {
        Self {
            exchange: "binance".into(),
            broadcast_tx,
            status: Arc::new(Mutex::new(SubscriberStatus::Disconnected)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 内部批量订阅逻辑
    pub async fn run_subscribe(self: Arc<Self>, symbols: Vec<Arc<str>>) {
        let mut retry_count = 0;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                let mut st = self.status.lock().await;
                *st = SubscriberStatus::Disconnected;
                info!("BinanceSubscriber shutdown requested, exiting subscribe loop.");
                break;
            }

            let streams_builder = Streams::<PublicTrades>::builder().subscribe(
                symbols
                    .iter()
                    .map(|symbol| {
                        (
                            BinanceFuturesUsd::default(),
                            symbol.as_ref(),
                            "usdt",
                            MarketDataInstrumentKind::Perpetual,
                            PublicTrades,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            let streams = match streams_builder.init().await {
                Ok(s) => {
                    retry_count = 0;
                    s
                }
                Err(err) => {
                    warn!(error=?err, "Failed to init streams, retrying...");
                    retry_count += 1;
                    let backoff = 2u64.pow(std::cmp::min(retry_count, 6));
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    continue;
                }
            };

            let mut joined_stream = streams.select_all().with_error_handler(|err| warn!(error=?err, "Stream error"));

            // 更新状态
            {
                let mut st = self.status.lock().await;
                *st = SubscriberStatus::Connected;
            }

            // listen and handle accepted events
            while let Some(event) = joined_stream.next().await {
                match PublicTradeEvent::try_from(event) {
                    Ok(ev) => {
                        self.broadcast_tx
                            .send(ev)
                            .map_err(|err| warn!(?err, "failed to send public trade event"))
                            .ok();
                    }
                    Err(_err) => {
                        // warn!(?_err, "skipped event");
                    }
                }
            }

            warn!("Stream ended or errored, retrying...");
            retry_count += 1;
            let backoff = 2u64.pow(std::cmp::min(retry_count, 6));
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    }

    /// 启动后台线程订阅（current-thread runtime，用于 !Send stream）
    fn start_subscribe_thread(self: &Arc<Self>, symbols: Vec<Arc<str>>) {
        let self_clone = self.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                self_clone.run_subscribe(symbols).await;
            });
        });
    }

    /// 启动 pipeline 转发任务（单一任务）
    fn start_forward_task(&self, tx: mpsc::Sender<PublicTradeEvent>) {
        // 每次调用都新建一个 broadcast 订阅者
        let mut broadcast_rx = self.broadcast_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(trade) = broadcast_rx.recv().await {
                if tx.send(trade).await.is_err() {
                    break;
                }
            }
        });
    }

    /// 停止订阅
    pub fn shutdown_subscribe(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// 异步获取状态
    pub async fn status(&self) -> SubscriberStatus {
        self.status.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl RealtimeSubscriber for BinanceSubscriber {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    /// 高性能批量订阅（返回 mpsc 给 pipeline）
    async fn subscribe_symbols(self: Arc<Self>, symbols: &[&str]) -> Result<mpsc::Receiver<PublicTradeEvent>> {
        let symbols_vec: Vec<Arc<str>> = symbols.iter().map(|s| Arc::from(*s)).collect();
        // 启动后台订阅线程
        self.start_subscribe_thread(symbols_vec);

        // 创建 pipeline channel
        let (tx, rx) = mpsc::channel(1024);
        self.start_forward_task(tx);
        Ok(rx)
    }

    async fn unsubscribe_symbols(&self, _symbols: &[&str]) -> Result<(), anyhow::Error> {
        // barter-data 不支持动态取消
        Ok(())
    }

    fn stop(&mut self) -> Result<(), anyhow::Error> {
        let _ = self.shutdown_subscribe();
        Ok(())
    }

    fn status(&self) -> SubscriberStatus {
        futures::executor::block_on(self.status())
    }
}
#[cfg(test)]
mod integration_tests {
    use super::*;
    use ms_tracing::tracing_utils::internal::debug;
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_binance_subscriber_real() {
        ms_tracing::setup_tracing();

        // 创建 broadcast channel
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1024);
        let subscriber = Arc::new(BinanceSubscriber::new(broadcast_tx.clone()));

        // 指定要订阅的币种
        let symbols = vec!["btc", "eth", "sol"];

        // 启动订阅，返回 pipeline receiver
        let mut rx = subscriber.clone().subscribe_symbols(&symbols).await.unwrap();

        // 等待一段时间让事件流入
        let mut received_count = 0;
        let max_wait = Duration::from_secs(800);
        let start = tokio::time::Instant::now();

        while start.elapsed() < max_wait {
            tokio::select! {
                Some(event) = rx.recv() => {
                    println!("Received trade: {} @ {}", event.symbol, event.trade.price);
                    received_count += 1;
                    if received_count >= 3 {
                        break;
                    }
                }
                _ = sleep(Duration::from_secs(1)) => {
                    // 等待下一轮
                }
            }
        }

        assert!(received_count > 0, "No trades received from Binance stream");

        // 测试 shutdown
        subscriber.shutdown_subscribe();
        sleep(Duration::from_millis(100)).await; // 等待状态更新
        let status = subscriber.status().await;
        debug!("Received status: {:?}", status);
        assert_eq!(status, SubscriberStatus::Connected);
    }
}

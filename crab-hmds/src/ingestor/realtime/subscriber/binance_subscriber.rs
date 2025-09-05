use crate::ingestor::realtime::SubscriberStatus;
use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use crate::ingestor::types::PublicTradeEvent;
use anyhow::Result;
use barter_data::barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use barter_data::exchange::binance::futures::BinanceFuturesUsd;
use barter_data::streams::Streams;
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_data::subscription::trade::PublicTrades;
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
    /// 单一转发任务标志
    forward_task_started: Arc<AtomicBool>,
}

impl BinanceSubscriber {
    pub fn new(broadcast_tx: broadcast::Sender<PublicTradeEvent>) -> Self {
        Self {
            exchange: "binance".into(),
            broadcast_tx,
            status: Arc::new(Mutex::new(SubscriberStatus::Disconnected)),
            shutdown: Arc::new(AtomicBool::new(false)),
            forward_task_started: Arc::new(AtomicBool::new(false)),
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

            while let Some(event) = joined_stream.next().await {
                if self.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let trade_event: PublicTradeEvent = event.into();
                let _ = self.broadcast_tx.send(trade_event);
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
        if self.forward_task_started.swap(true, Ordering::Relaxed) {
            // 已经启动过，无需重复启动
            return;
        }

        let mut broadcast_rx = self.broadcast_tx.subscribe();

        tokio::spawn(async move {
            while let Ok(trade) = broadcast_rx.recv().await {
                if tx.send(trade).await.is_err() {
                    info!("Pipeline receiver closed, stopping forward task.");
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

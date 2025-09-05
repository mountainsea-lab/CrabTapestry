use crate::ingestor::realtime::subscriber::RealtimeSubscriber;
use crate::ingestor::realtime::SubscriberStatus;
use crate::ingestor::types::PublicTradeEvent;
use barter_data::barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use barter_data::exchange::binance::futures::BinanceFuturesUsd;
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_data::streams::Streams;
use barter_data::subscription::trade::PublicTrades;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use anyhow::Result;
use tokio::sync::mpsc;

/// Binance 实时订阅器（精简版）
pub struct BinanceSubscriber {
    exchange: String,
    /// broadcast channel，用于 pipeline 订阅事件
    broadcast_tx: broadcast::Sender<PublicTradeEvent>,
    /// 订阅器状态
    status: Arc<Mutex<SubscriberStatus>>,
}

impl BinanceSubscriber {
    pub fn new(broadcast_tx: broadcast::Sender<PublicTradeEvent>) -> Self {
        Self {
            exchange: "binance".into(),
            broadcast_tx,
            status: Arc::new(Mutex::new(SubscriberStatus::Disconnected)),
        }
    }


    /// 内部批量订阅逻辑
    /// symbols: 'static Arc<str>，避免生命周期问题
    pub async fn run_subscribe(self: Arc<Self>, symbols: Vec<Arc<str>>) {
        let max_retries = 5;
        let mut retry_count = 0;

        loop {
            // 构建批量订阅流
            let streams_builder = Streams::<PublicTrades>::builder().subscribe(
                symbols.iter().map(|symbol| {
                    (
                        BinanceFuturesUsd::default(),
                        symbol.as_ref(),
                        "usdt",
                        MarketDataInstrumentKind::Perpetual,
                        PublicTrades,
                    )
                }).collect::<Vec<_>>(),
            );

            let streams = match streams_builder.init().await {
                Ok(s) => s,
                Err(err) => {
                    eprintln!("Failed to init streams: {:?}", err);
                    if retry_count < max_retries {
                        retry_count += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(retry_count as u32))).await;
                        continue;
                    } else {
                        eprintln!("Max retries reached, aborting subscription");
                        break;
                    }
                }
            };

            // 合并所有 symbol stream
            let mut joined_stream = streams
                .select_all()
                .with_error_handler(|err| eprintln!("Stream error: {:?}", err));

            // 更新状态
            {
                let mut st = self.status.lock().await;
                *st = SubscriberStatus::Connected;
            }

            while let Some(event) = joined_stream.next().await {
                let trade_event: PublicTradeEvent = event.into();
                // 发给 pipeline 的 broadcast channel
                let _ = self.broadcast_tx.send(trade_event);
            }

            eprintln!("Stream ended or errored, retrying...");
            if retry_count < max_retries {
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(retry_count as u32))).await;
            } else {
                eprintln!("Max retries reached, aborting.");
                break;
            }
        }

        // 更新状态
        {
            let mut st = self.status.lock().await;
            *st = SubscriberStatus::Disconnected;
        }
    }
}

#[async_trait::async_trait]
impl RealtimeSubscriber for BinanceSubscriber {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    /// 高性能批量订阅
    async fn subscribe_symbols(
        self: Arc<Self>,
        symbols: &[&str],
    ) -> anyhow::Result<mpsc::Receiver<PublicTradeEvent>> {
        // 每个 symbol 任务直接使用 broadcast channel 订阅
        let mut rx = self.broadcast_tx.subscribe();

        // 转成 Arc<str>，保证 'static
        let symbols_vec: Vec<Arc<str>> = symbols.iter().map(|s| Arc::from(*s)).collect();
        let self_clone = self.clone();

        // 后台任务启动订阅
        tokio::spawn(async move {
            self_clone.run_subscribe(symbols_vec).await;
        });

        // 包装成独立 mpsc channel 给 pipeline 使用
        let (tx, rx_wrapper) = mpsc::channel(1024);

        tokio::spawn(async move {
            while let Ok(trade) = rx.recv().await {
                // 将 broadcast 消息转发到 pipeline mpsc
                if tx.send(trade).await.is_err() {
                    // pipeline 已关闭，退出任务
                    break;
                }
            }
        });

        Ok(rx_wrapper)
    }

    async fn unsubscribe_symbols(&self, _symbols: &[&str]) -> Result<(), anyhow::Error> {
        // barter-data 暂不支持动态取消，symbol 级取消交给 pipeline
        Ok(())
    }

    fn stop(&mut self) -> Result<(), anyhow::Error> {
        // barter-data 暂不支持 stop，pipeline 可通过 cancel 终止任务
        Ok(())
    }

    fn status(&self) -> SubscriberStatus {
        futures::executor::block_on(self.status.lock()).clone()
    }
}
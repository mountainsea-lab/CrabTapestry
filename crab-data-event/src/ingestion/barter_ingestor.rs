use std::sync::Arc;
use std::time::Duration;
use barter_data::barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use barter_data::exchange::binance::futures::BinanceFuturesUsd;
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_data::streams::Streams;
use barter_data::subscription::trade::PublicTrades;
use crate::ingestion::types::PublicTradeEvent;
use crossbeam::channel::Sender;
use futures_util::StreamExt;
use ms_tracing::tracing_utils::internal::warn;

/// BarterIngestor acquires real-time trade data events from the exchange
pub struct BarterIngestor {
    sender: Sender<PublicTradeEvent>, // trade data transfer channel
}

impl BarterIngestor {
    pub fn new(sender: Sender<PublicTradeEvent>) -> Self {
        BarterIngestor { sender }
    }

    async fn subscribe(&self) {
        let sender = self.sender.clone();
        // get symbols（examples）
        let symbols = vec![
            "btc", "eth", "xrp", "sol", "avax", "ltc",
        ];

        let mut retry_count = 0;
        let max_retries = 5; // 最大重试次数

        loop {
            // 批量订阅多个币种的市场数据流
            let streams = Streams::<PublicTrades>::builder().subscribe(
                symbols
                    .clone()
                    .into_iter()
                    .map(|symbol| {
                        (
                            BinanceFuturesUsd::default(),
                            symbol,
                            "usdt",
                            MarketDataInstrumentKind::Perpetual,
                            PublicTrades,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            // 初始化并启动所有订阅
            let streams = match streams.init().await {
                Ok(s) => s,
                Err(err) => {
                    warn!(?err, "Failed to initialize streams. Retrying...");
                    if retry_count < max_retries {
                        retry_count += 1;
                        let backoff_duration = Duration::from_secs(2u64.pow(retry_count as u32)); // 指数回退
                        tokio::time::sleep(backoff_duration).await;
                        continue;
                    } else {
                        warn!("Maximum retries reached. Aborting subscription.");
                        return; // 达到最大重试次数，放弃
                    }
                }
            };

            // use `select_all` join multi data streams
            let mut joined_stream = streams
                .select_all()
                .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

            // 监听和处理接收到的事件 listen and handle accepted events
            while let Some(event) = joined_stream.next().await {
                // info!("{:?}", event);
                let public_trade_event: PublicTradeEvent = event.into();
                // send public_trade_event to the channel
                if let Err(err) = sender.send(public_trade_event) {
                    warn!(?err, "Failed to send public trade event");
                }
            }

            // retry
            warn!("Stream has ended or encountered an error. Retrying...");
            if retry_count < max_retries {
                retry_count += 1;
                let backoff_duration = Duration::from_secs(2u64.pow(retry_count as u32)); // 指数回退
                tokio::time::sleep(backoff_duration).await;
            } else {
                warn!("Maximum retries reached. Aborting.");
                return; // retry max times , stop
            }
        }
    }

    fn start_non_send(self: Arc<Self>) {
        std::thread::spawn(move || {
            // current thread runtime
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                self.subscribe().await;
            });
        });
    }
}

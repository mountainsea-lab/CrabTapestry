use crate::ingestion::Ingestor;
use barter_data::barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use barter_data::exchange::binance::futures::BinanceFuturesUsd;
use barter_data::streams::Streams;
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_data::subscription::trade::PublicTrades;
use crab_infras::aggregator::types::PublicTradeEvent;
use crab_infras::config::sub_config::Subscription;
use crossbeam::channel::Sender;
use dashmap::DashMap;
use futures_util::StreamExt;
use ms_tracing::tracing_utils::internal::warn;
use std::sync::Arc;
use std::time::Duration;

/// BarterIngestor acquires real-time trade data events from the exchange
pub struct BarterIngestor {
    sender: Sender<PublicTradeEvent>, // trade data transfer channel
    /// 已订阅交易对 (exchange, symbol) -> Subscription
    subscribed: Arc<DashMap<(String, String), Subscription>>,
}

impl BarterIngestor {
    pub fn new(sender: Sender<PublicTradeEvent>, subscribed: Arc<DashMap<(String, String), Subscription>>) -> Self {
        BarterIngestor { sender, subscribed }
    }

    async fn subscribe(&self) {
        let sender = self.sender.clone();
        let symbols = Subscription::get_symbols_for_exchange(&self.subscribed, "BinanceFuturesUsd");

        let mut retry_count = 0;
        let max_retries = 5; // 最大重试次数

        loop {
            // 批量订阅多个币种的市场数据流
            let streams = Streams::<PublicTrades>::builder().subscribe(
                symbols
                    .iter()
                    .map(|symbol_arc| {
                        (
                            BinanceFuturesUsd::default(),
                            symbol_arc.as_ref(), // &str
                            "usdt",
                            MarketDataInstrumentKind::Perpetual,
                            PublicTrades,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            // init and start subscribe
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
                        return; // retry max times , stop
                    }
                }
            };

            // use `select_all` join multi data streams
            let mut joined_stream = streams
                .select_all()
                .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

            // listen and handle accepted events
            while let Some(event) = joined_stream.next().await {
                match PublicTradeEvent::try_from(event) {
                    Ok(ev) => {
                        sender
                            .send(ev)
                            .map_err(|err| warn!(?err, "failed to send public trade event"))
                            .ok();
                    }
                    Err(_err) => {
                        // warn!(?err, "skipped event");
                    }
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
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

            rt.block_on(async {
                self.subscribe().await;
            });
        });
    }
}

impl Ingestor<PublicTradeEvent> for BarterIngestor {
    fn start(self: Arc<Self>) {
        self.start_non_send();
    }
}

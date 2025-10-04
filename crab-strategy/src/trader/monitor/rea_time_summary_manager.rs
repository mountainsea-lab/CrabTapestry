use barter::statistic::summary::TradingSummary;
use barter::statistic::time::TimeInterval;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

/// 实时统计管理器 - 基于现有的 TradingSummaryGenerator
pub struct RealTimeSummaryManager<Interval>
where
    Interval: TimeInterval,
{
    /// 当前的交易总结
    current_summary: Arc<RwLock<Option<TradingSummary<Interval>>>>,

    /// 总结更新广播
    summary_tx: broadcast::Sender<TradingSummary<Interval>>,

    /// 最后更新时间
    last_update: Arc<RwLock<DateTime<Utc>>>,
}

impl<Interval> RealTimeSummaryManager<Interval>
where
    Interval: TimeInterval + Send + Sync + 'static,
    TradingSummary<Interval>: Send + Sync,
{
    pub fn new() -> Self {
        let (summary_tx, _) = broadcast::channel(100);

        Self {
            current_summary: Arc::new(RwLock::new(None)),
            summary_tx,
            last_update: Arc::new(RwLock::new(Utc::now())),
        }
    }

    /// 更新当前总结
    pub async fn update_summary(&self, summary: TradingSummary<Interval>) {
        // 更新当前总结
        {
            let mut current_guard = self.current_summary.write().await;
            *current_guard = Some(summary.clone());
        }

        // 更新时间戳
        {
            let mut last_update_guard = self.last_update.write().await;
            *last_update_guard = Utc::now();
        }

        // 广播更新
        let _ = self.summary_tx.send(summary);
    }

    /// 获取当前总结
    pub async fn get_current_summary(&self) -> Option<TradingSummary<Interval>> {
        let current_guard = self.current_summary.read().await;
        current_guard.clone()
    }

    /// 订阅总结更新
    pub fn subscribe(&self) -> broadcast::Receiver<TradingSummary<Interval>> {
        self.summary_tx.subscribe()
    }

    /// 获取最后更新时间
    pub async fn last_update(&self) -> DateTime<Utc> {
        let last_update_guard = self.last_update.read().await;
        *last_update_guard
    }
}

/// 实时统计显示器
pub struct RealTimeSummaryDisplay<Interval>
where
    Interval: TimeInterval,
{
    summary_manager: Arc<RealTimeSummaryManager<Interval>>,
    display_interval: std::time::Duration,
}

impl<Interval> RealTimeSummaryDisplay<Interval>
where
    Interval: TimeInterval + Send + Sync + 'static,
    TradingSummary<Interval>: Send + Sync,
{
    pub fn new(summary_manager: Arc<RealTimeSummaryManager<Interval>>, display_interval: std::time::Duration) -> Self {
        Self { summary_manager, display_interval }
    }

    /// 启动实时显示
    pub async fn start_display(&self) {
        let mut interval = tokio::time::interval(self.display_interval);
        let summary_manager = Arc::clone(&self.summary_manager);

        tokio::spawn(async move {
            loop {
                interval.tick().await;

                if let Some(summary) = summary_manager.get_current_summary().await {
                    Self::display_realtime_summary(&summary).await;
                }
            }
        });
    }

    /// 显示实时总结
    async fn display_realtime_summary(summary: &TradingSummary<Interval>) {
        // 清屏（可选）
        print!("\x1B[2J\x1B[1;1H");

        println!("🔄 实时交易统计 - 最后更新: {}", Utc::now().format("%H:%M:%S"));
        println!("⏱️  运行时间: {}", format_duration(summary.trading_duration()));

        // 使用原有的打印方法，但添加实时标记
        summary.print_summary();
    }

    /// 启动自动更新订阅
    pub async fn start_auto_update(&self) {
        let mut summary_rx = self.summary_manager.subscribe();
        let summary_manager = Arc::clone(&self.summary_manager);

        tokio::spawn(async move {
            while let Ok(summary) = summary_rx.recv().await {
                // 有新总结时立即显示
                Self::display_realtime_summary(&summary).await;
            }
        });
    }
}

/// 格式化持续时间
fn format_duration(duration: chrono::TimeDelta) -> String {
    let seconds = duration.num_seconds();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, secs)
}

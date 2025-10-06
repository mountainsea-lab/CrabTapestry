use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use ta4r::bar::base_bar_series::BaseBarSeries;
use ta4r::bar::base_bar_series_builder::BaseBarSeriesBuilder;
use ta4r::bar::types::BarSeriesBuilder;
use ta4r::num::decimal_num::DecimalNum;
use tokio::sync::{Notify, RwLock};

pub const STATE_UNINIT: u8 = 0;
pub const STATE_LOADING: u8 = 1;
pub const STATE_READY: u8 = 2;

/// 每个序列的条目
pub struct SeriesEntry {
    pub series: Arc<RwLock<BaseBarSeries<DecimalNum>>>,
    pub(crate) state: AtomicU8,     // 0=Uninit,1=Loading,2=Ready
    pub(crate) notify: Arc<Notify>, // notify waiters when ready
}
impl SeriesEntry {
    pub fn new_empty() -> Self {
        // create empty series with name placeholder
        let builder = BaseBarSeriesBuilder::<DecimalNum>::new().with_name("empty");
        let series = builder.build().expect("empty builder build");
        Self {
            series: Arc::new(RwLock::new(series)),
            state: AtomicU8::new(STATE_UNINIT),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn new_with_series(series: BaseBarSeries<DecimalNum>) -> Self {
        Self {
            series: Arc::new(RwLock::new(series)),
            state: AtomicU8::new(STATE_READY),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::SeqCst) == STATE_READY
    }
}

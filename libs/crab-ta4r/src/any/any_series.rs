use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use ta4r::bar::types::{Bar, BarSeries};
use ta4r::num::TrNum;

// ====================================================
// AnyBar 类型擦除
// ====================================================
pub trait AnyBar: Send + Sync {
    fn open(&self) -> f64;
    fn high(&self) -> f64;
    fn low(&self) -> f64;
    fn close(&self) -> f64;
    fn volume(&self) -> f64;
    fn as_any(&self) -> &dyn Any;
}

// Value-based BarWrapper，线程安全
pub struct BarWrapper {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl AnyBar for BarWrapper {
    fn open(&self) -> f64 {
        self.open
    }
    fn high(&self) -> f64 {
        self.high
    }
    fn low(&self) -> f64 {
        self.low
    }
    fn close(&self) -> f64 {
        self.close
    }
    fn volume(&self) -> f64 {
        self.volume
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ====================================================
// AnySeries 类型擦除
// ====================================================
pub trait AnySeries: Send + Sync {
    fn len(&self) -> usize;
    fn get_bar(&self, index: usize) -> Option<Box<dyn AnyBar>>;
    fn last_bar(&self) -> Option<Box<dyn AnyBar>> {
        if self.len() == 0 {
            None
        } else {
            self.get_bar(self.len() - 1)
        }
    }
    fn as_any(&self) -> &dyn Any;
}

// 内部包装 trait
pub trait BarSeriesWrapper: Send + Sync {
    fn len(&self) -> usize;
    fn get_bar_box(&self, index: usize) -> Option<Box<dyn AnyBar>>;
    fn as_any(&self) -> &dyn Any;
}

// AnySeries 实现
pub struct AnySeriesImpl {
    inner: Arc<dyn BarSeriesWrapper>,
}

impl AnySeriesImpl {
    pub fn new(inner: Arc<dyn BarSeriesWrapper>) -> Self {
        Self { inner }
    }
}

impl AnySeries for AnySeriesImpl {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn get_bar(&self, index: usize) -> Option<Box<dyn AnyBar>> {
        self.inner.get_bar_box(index)
    }
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }
}

// ====================================================
// Blanket 封装：将任意具体 BarSeries<T> 封装为 AnySeries
// ====================================================
pub struct BarSeriesWrapperImpl<T, S>
where
    T: TrNum + 'static,
    S: BarSeries<T> + Send + Sync + 'static,
{
    series: Arc<S>,
    _marker: PhantomData<T>,
}

impl<T, S> BarSeriesWrapperImpl<T, S>
where
    T: TrNum + 'static,
    S: BarSeries<T> + Send + Sync + 'static,
{
    pub fn new(series: Arc<S>) -> Self {
        Self { series, _marker: PhantomData }
    }
}

impl<T, S> BarSeriesWrapper for BarSeriesWrapperImpl<T, S>
where
    T: TrNum + 'static,
    S: BarSeries<T> + Send + Sync + 'static,
{
    fn len(&self) -> usize {
        self.series.get_bar_count()
    }

    fn get_bar_box(&self, index: usize) -> Option<Box<dyn AnyBar>> {
        self.series.get_bar(index).map(|b| {
            let b = b; // &S::Bar
            // 需要 Bar<T> trait
            Box::new(BarWrapper {
                open: b.get_open_price().map(|v| v.to_f64().unwrap_or(f64::NAN)).unwrap_or(f64::NAN),
                high: b.get_high_price().map(|v| v.to_f64().unwrap_or(f64::NAN)).unwrap_or(f64::NAN),
                low: b.get_low_price().map(|v| v.to_f64().unwrap_or(f64::NAN)).unwrap_or(f64::NAN),
                close: b.get_close_price().map(|v| v.to_f64().unwrap_or(f64::NAN)).unwrap_or(f64::NAN),
                volume: b.get_volume().to_f64().unwrap_or(f64::NAN),
            }) as Box<dyn AnyBar>
        })
    }

    fn as_any(&self) -> &dyn Any {
        &*self.series
    }
}

// ====================================================
// wrap_series 快捷方法
// ====================================================
pub fn wrap_series<T, S>(series: Arc<S>) -> AnySeriesImpl
where
    T: TrNum + 'static,
    S: BarSeries<T> + Send + Sync + 'static,
{
    let wrapper = Arc::new(BarSeriesWrapperImpl::<T, S>::new(series));
    AnySeriesImpl::new(wrapper)
}

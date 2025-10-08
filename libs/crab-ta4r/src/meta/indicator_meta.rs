use crate::indicator::CrabIndicatorAny;
use crate::meta::param::{ParamSpec, ParamValue};
use crate::meta::view::VisualizationConfig;
use crate::types::IndicatorCategory;
use crab_types::bar_cache::bar_key::BarKey;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use ta4r::bar::builder::types::BarSeriesRef;
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;

/// 构建指标实例的上下文
pub struct IndicatorInitContext<N: TrNum + 'static, S: BarSeries<N>> {
    /// 指标绑定的数据序列
    pub series: BarSeriesRef<S>,

    /// 构造参数：name -> value
    pub params: HashMap<String, ParamValue>,

    /// 可选运行上下文信息
    pub binding: Option<BarKey>,

    /// 每个指标实例在运行时的唯一 key （如策略实例维度） BarKey+具体指标名称
    pub id: String,

    /// 可选附加信息（如策略 id、数据版本等）
    pub metadata: HashMap<String, String>,
    pub _marker: PhantomData<N>,
}

// --------------------------- 指标元信息 ---------------------------
#[derive(Clone)]
pub struct IndicatorMeta {
    pub name: String,
    pub display_name: String,
    pub description: Option<String>,
    pub category: IndicatorCategory,
    pub params: HashMap<String, ParamSpec>,
    pub visualization: VisualizationConfig,
    pub factory: Arc<dyn Fn(Box<dyn std::any::Any>) -> Arc<dyn CrabIndicatorAny> + Send + Sync>,
    pub unstable_bars: usize,
    pub version: Option<String>,
    pub author: Option<String>,
}

impl fmt::Debug for IndicatorMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndicatorMeta")
            .field("name", &self.name)
            .field("display_name", &self.display_name)
            .field("description", &self.description)
            .field("category", &self.category)
            .field("params", &self.params)
            .field("visualization", &self.visualization)
            .field("unstable_bars", &self.unstable_bars)
            .field("version", &self.version)
            .field("author", &self.author)
            .finish_non_exhaustive() // 👈 表示略去 factory
    }
}
//
// // Factory 示例对接
// pub fn sma_factory<N: TrNum, S: BarSeries<N>>(ctx: IndicatorInitContext<N, S>) -> Arc<dyn CrabIndicatorAny> {
//     // 构建静态泛型指标
//     let close = Arc::new(ClosePriceIndicator::from_shared(ctx.series.clone()));
//     let sma = Arc::new(SmaIndicator::new(close.clone(), ctx.params["period"].as_int()));
//
//     // 包装成 CrabIndicatorAny 对外返回
//     Arc::new(CrabIndicatorWrapper { inner: sma }) as Arc<dyn CrabIndicatorAny>
// }
//
// // Wrapper
// struct CrabIndicatorWrapper<N: TrNum, S: BarSeries<N>> {
//     inner: Arc<dyn CrabIndicator<Num = N, Output = N, Series = S>>,
// }
//
// impl<N: TrNum, S: BarSeries<N>> CrabIndicatorAny for CrabIndicatorWrapper<N, S> {
//     fn name(&self) -> &str { "SMA" }
//     fn get_value_as_f64(&self, index: usize) -> f64 {
//         self.inner.get_value(index).unwrap().to_f64().unwrap()
//     }
// }

use crate::indicator::{CrabIndicator, CrabIndicatorAny};
use crate::meta::CrabIndicatorAny;
use crate::meta::param::{ParamSpec, ParamValue};
use crate::meta::view::VisualizationConfig;
use crate::types::IndicatorCategory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use ta4r::bar::builder::types::BarSeriesRef;
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;

/// 构建指标实例的上下文
pub struct IndicatorInitContext<N: TrNum, S: BarSeries<N>> {
    /// 指标绑定的数据序列
    pub series: BarSeriesRef<S>,

    /// 构造参数：name -> value
    pub params: HashMap<String, ParamValue>,

    /// 可选运行上下文信息
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub period: Option<String>,

    /// 可选附加信息（如策略 id、数据版本等）
    pub metadata: HashMap<String, String>,
}

// --------------------------- 指标元信息 ---------------------------
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorMeta {
    pub name: String,
    pub display_name: String,
    pub description: Option<String>,
    pub category: String,
    pub params: HashMap<String, ParamSpec>,
    pub factory: Arc<dyn Fn(Box<dyn std::any::Any>) -> Arc<dyn CrabIndicatorAny> + Send + Sync>,
    pub unstable_bars: usize,
    pub version: Option<String>,
    pub author: Option<String>,
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
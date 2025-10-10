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

// ====================== 上下文：构建指标时的输入环境 ======================
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

/// 类型擦除版本 —— 用于动态指标构建工厂
#[derive(Clone)]
pub struct IndicatorInitContextAny {
    /// 可选的通用 series 引用（类型擦除）
    pub series: Option<Arc<dyn std::any::Any + Send + Sync>>,

    /// 构造参数
    pub params: HashMap<String, ParamValue>,

    /// 所属 BarKey
    pub binding: Option<BarKey>,

    /// 每个实例唯一标识
    pub id: String,

    /// 附加元数据
    pub metadata: HashMap<String, String>,
}

// ====================== 元信息层：指标定义与工厂 ======================

/// 指标元信息结构 ——
/// 用于描述指标的属性、可视化配置及动态构建方式。
#[derive(Clone)]
pub struct IndicatorMeta {
    /// 内部名称（如 "sma"）
    pub name: String,

    /// 显示名称（如 "Simple Moving Average"）
    pub display_name: String,

    /// 描述信息
    pub description: Option<String>,

    /// 分类（趋势类、震荡类等）
    pub category: IndicatorCategory,

    /// 参数规范（如 "period" -> int 14）
    pub params: HashMap<String, ParamSpec>,

    /// 可视化配置（颜色、图层、线型等）
    pub visualization: VisualizationConfig,

    /// 指标实例构造工厂 ——
    ///   类型擦除闭包：由 `IndicatorInitContextAny` 构建具体实例
    pub factory: Arc<dyn Fn(IndicatorInitContextAny) -> Arc<dyn CrabIndicatorAny> + Send + Sync>,

    /// 不稳定 bar 数量
    pub unstable_bars: usize,

    /// 可选版本号
    pub version: Option<String>,

    /// 可选作者信息
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

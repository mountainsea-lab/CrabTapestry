pub mod registry;

use crate::meta::indicator_meta::IndicatorMeta;
use num_traits::ToPrimitive;
use std::sync::Arc;
use ta4r::bar::builder::types::BarSeriesRef;
use ta4r::bar::types::BarSeries;
use ta4r::indicators::types::IndicatorError;
use ta4r::num::TrNum;
// ====================== 顶层：泛型指标接口 ======================

/// Crab 顶层指标 trait ——
/// 面向策略引擎和可视化层的通用封装，
/// 以泛型方式包装 ta4r::Indicator。
///
/// 泛型参数：
/// - `Num`: 数值类型（实现 TrNum，可为 f64、Decimal、Dashu 等）
/// - `Output`: 指标输出类型（一般与 Num 相同）
/// - `Series`: 数据序列类型（实现 BarSeries<Num>）
pub trait CrabIndicator: Send + Sync {
    /// 指标使用的数值类型
    type Num: TrNum + 'static;

    /// 指标输出类型（例如 RSI 为 Num，MACD 可能为结构体）
    type Output: Clone + 'static;

    /// 绑定的数据序列类型
    type Series: BarSeries<Self::Num> + 'static;

    /// 指标元信息（包括名称、参数说明、可视化配置等）
    fn meta(&self) -> &IndicatorMeta;

    /// 获取指定索引处的指标值（对齐 ta4r::Indicator 接口）
    fn get_value(&self, index: usize) -> Result<Self::Output, IndicatorError>;

    /// 获取绑定的 BarSeries 引用（支持共享或独占）
    fn bar_series(&self) -> BarSeriesRef<Self::Series>;

    /// 指标中“不稳定 bar”的数量（例如 MA 前几项无效）
    fn count_of_unstable_bars(&self) -> usize {
        0
    }

    /// 判断某个索引处是否已经稳定
    fn is_stable_at(&self, index: usize) -> bool {
        index >= self.count_of_unstable_bars()
    }

    /// 返回底层 ta4r 指标的引用（可选）
    /// - 默认返回 None
    /// - 若想支持类型反射或调试，可返回 `Some(&dyn Any)`
    fn as_ta4r(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// 批量导出所有指标值（通常用于可视化或导出）
    fn values(&self) -> Vec<(usize, Self::Output)> {
        let series_ref = self.bar_series();
        let len = series_ref.with_ref_or(0, |s| s.get_bar_count());
        (0..len).filter_map(|i| self.get_value(i).ok().map(|v| (i, v))).collect()
    }
}

// ====================== 动态层：类型擦除接口 ======================

/// 类型擦除后的指标接口 ——
/// 用于动态构建、策略运行时与可视化系统中。
///
/// - 适用于所有指标统一管理（无需关心具体数值类型）
/// - 便于 JSON/前端展示、策略参数传递
pub trait CrabIndicatorAny: Send + Sync {
    /// 指标内部名称（如 "sma", "rsi"）
    fn name(&self) -> &str;

    /// 指标元信息（用于展示和反射）
    fn meta(&self) -> &IndicatorMeta;

    /// 获取数值型指标值（以 f64 形式返回）
    /// - 仅用于展示或简单计算，不保证精度
    fn get_value_as_f64(&self, index: usize) -> Result<f64, IndicatorError>;

    /// 判断该索引处的值是否稳定（默认 true）
    fn is_stable_at(&self, index: usize) -> bool {
        true
    }

    /// 批量导出所有指标值（用于图表绘制）
    fn values(&self) -> Vec<(usize, f64)>;
}

// ====================== 桥接层：泛型 → 类型擦除包装器 ======================

/// 将泛型指标包装为类型擦除指标，
/// 以便动态注册与统一调度。
pub struct CrabIndicatorWrapper<I: CrabIndicator> {
    inner: Arc<I>,
}

impl<I: CrabIndicator> CrabIndicatorWrapper<I>
where
    I::Output: ToPrimitive,
{
    pub fn new(inner: Arc<I>) -> Self {
        Self { inner }
    }
}

impl<I: CrabIndicator> CrabIndicatorAny for CrabIndicatorWrapper<I>
where
    I::Output: ToPrimitive,
{
    fn name(&self) -> &str {
        &self.inner.meta().name
    }

    fn meta(&self) -> &IndicatorMeta {
        self.inner.meta()
    }

    fn get_value_as_f64(&self, index: usize) -> Result<f64, IndicatorError> {
        self.inner.get_value(index).map(|v| v.to_f64().unwrap_or(f64::NAN))
    }

    fn is_stable_at(&self, index: usize) -> bool {
        self.inner.is_stable_at(index)
    }

    fn values(&self) -> Vec<(usize, f64)> {
        self.inner
            .values()
            .into_iter()
            .filter_map(|(i, v)| v.to_f64().map(|f| (i, f)))
            .collect()
    }
}

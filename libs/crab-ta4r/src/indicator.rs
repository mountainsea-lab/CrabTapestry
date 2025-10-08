pub mod registry;

use crate::types::IndicatorMeta;
use std::sync::Arc;
use ta4r::bar::builder::types::BarSeriesRef;
use ta4r::bar::types::BarSeries;
use ta4r::indicators::Indicator;
use ta4r::indicators::types::IndicatorError;
use ta4r::num::TrNum;
/// 顶层指标 trait —— 面向策略和可视化封装 ta4r::Indicator
pub trait CrabIndicator: Send + Sync {
    type Num: TrNum + 'static;
    type Output: Clone + 'static;
    type Series: BarSeries<Self::Num> + 'static;

    /// 元信息
    fn meta(&self) -> &IndicatorMeta;

    /// 获取值（与 ta4r 对齐）
    fn get_value(&self, index: usize) -> Result<Self::Output, IndicatorError>;

    /// 获取绑定的 BarSeries 引用
    fn bar_series(&self) -> BarSeriesRef<Self::Series>;

    /// 不稳定 bar 数量
    fn count_of_unstable_bars(&self) -> usize {
        0
    }

    /// 是否稳定
    fn is_stable_at(&self, index: usize) -> bool {
        index >= self.count_of_unstable_bars()
    }

    /// 返回底层 ta4r 指标引用（可选）
    fn as_ta4r(
        &self,
    ) -> Option<Arc<dyn Indicator<Num = Self::Num, Output = Self::Output, Series = Self::Series> + Send + Sync>> {
        None
    }

    /// 批量导出（用于可视化）
    fn values(&self) -> Vec<(usize, Self::Output)> {
        let series_ref = self.bar_series();
        let len = series_ref.with_ref_or(0, |s| s.get_bar_count());
        (0..len).filter_map(|i| self.get_value(i).ok().map(|v| (i, v))).collect()
    }
}

use num_traits::ToPrimitive;
use std::any::Any;
use ta4r::bar::types::BarSeries;
use ta4r::indicators::Indicator;
use ta4r::num::TrNum;

/// 类型擦除后的指标
pub trait IndicatorAny: Send + Sync {
    /// 获取指标值，返回 f64 方便可视化
    fn get_value(&self, index: usize) -> Option<f64>;

    /// 指标名称
    fn name(&self) -> &str;

    /// downcast 回原始指标类型
    fn as_any(&self) -> &dyn Any;
}

// Blanket impl: 对任何具体指标实现 IndicatorAny
impl<N, S, I> IndicatorAny for I
where
    I: Indicator<Num = N, Series = S> + Send + Sync + 'static,
    N: TrNum + ToPrimitive + 'static,
    S: BarSeries<N>,
    I::Output: ToPrimitive,
{
    fn get_value(&self, index: usize) -> Option<f64> {
        match self.get_value(index) {
            Ok(v) => Some(v.to_f64().unwrap_or(f64::NAN)),
            Err(_) => None,
        }
    }

    fn name(&self) -> &str {
        std::any::type_name::<I>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

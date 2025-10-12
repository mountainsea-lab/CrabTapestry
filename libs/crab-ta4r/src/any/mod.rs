use crate::any::any_indicator::IndicatorAny;
use crate::meta::view::IndicatorLine;

pub mod any_indicator;
pub mod any_series;

/// -------------------------------------------
/// 类型擦除指标 -> IndicatorLine 可视化
/// -------------------------------------------
pub fn sample_indicator_any(indicator: &dyn IndicatorAny, len: usize, name: &str) -> IndicatorLine {
    let values = (0..len)
        .map(|i| (i, indicator.get_value(i).unwrap_or(f64::NAN)))
        .collect::<Vec<_>>();

    IndicatorLine {
        name: name.into(),
        color: None,
        values,
        visible: true,
    }
}

use serde::{Deserialize, Serialize};

/// 指标分类（趋势/动量/波动率/自定义）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IndicatorCategory {
    Trend,
    Momentum,
    Volume,
    Volatility,
    Custom(String),
}

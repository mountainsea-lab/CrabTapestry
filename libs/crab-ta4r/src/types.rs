use crate::meta::indicator_meta::IndicatorMeta;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 指标分类（趋势/动量/波动率/自定义）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IndicatorCategory {
    Trend,
    Momentum,
    Volume,
    Volatility,
    Custom(String),
}

/// 指标事件
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorEvent {
    pub name: String,
    pub meta: IndicatorMeta,
    pub data: Vec<crate::meta::view::IndicatorPoint>,
}

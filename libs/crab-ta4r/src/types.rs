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

/// 指标元信息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorMeta {
    pub name: String,                 // 英文标识符
    pub display_name: String,         // 展示名称
    pub category: IndicatorCategory,  // 分类
    pub params: HashMap<String, f64>, // 参数（period, threshold 等）
    pub visualization: VisualizationType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VisualizationType {
    /// 主图叠加（如 SMA, EMA）
    Overlay,
    /// 副图折线（如 RSI, MACD）
    Line,
    /// 柱状图（如成交量）
    Histogram,
    /// 信号点（如买入/卖出）
    Signal,
}

/// 绘图样式（未来可扩展）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisualizationStyle {
    pub color: Option<String>,
    pub line_width: Option<f64>,
    pub show_points: bool,
}

/// 指标单点结果
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorPoint {
    pub index: usize,
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

/// 指标事件
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorEvent {
    pub name: String,
    pub meta: IndicatorMeta,
    pub data: Vec<IndicatorPoint>,
}

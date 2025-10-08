use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct VisualizationConfig {
    pub chart_type: VisualizationType, // Overlay, Line, Histogram, Signal
    pub color: Option<String>,
    pub line_width: Option<f64>,
    pub show_points: bool,
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

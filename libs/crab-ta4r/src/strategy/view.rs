use serde::{Deserialize, Serialize};

/// 策略输出的可视化数据（统一接口）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyVisualization {
    /// 指标名称 -> 曲线值
    pub indicators: Vec<IndicatorLine>,

    /// 规则信号（买入/卖出点等）
    pub signals: Vec<SignalEvent>,

    /// 可选的策略层信息（盈亏、持仓、交易点）
    pub metrics: Option<StrategyMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorLine {
    pub name: String,
    pub color: Option<String>,
    pub values: Vec<(usize, f64)>, // index, value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvent {
    pub index: usize,
    pub signal_type: String, // "BUY" / "SELL" / "ALERT"
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub total_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub equity_curve: Vec<(usize, f64)>,
}

use serde::{Deserialize, Serialize};

/// 策略可视化总结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyVisualization {
    /// 策略名称
    pub name: String,

    /// 指标数据（价格、均线、RSI 等）
    pub indicators: Vec<IndicatorLine>,

    /// 信号事件（买入 / 卖出 / 警报）
    pub signals: Vec<SignalEvent>,

    /// 规则评估结果（entry/exit 逻辑）
    pub rules: Vec<RuleResult>,

    /// 策略整体绩效指标（累计收益、胜率、净值曲线等）
    pub metrics: Option<StrategyMetrics>,
}

/// 指标曲线数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorLine {
    pub name: String,
    pub color: Option<String>,     // 可选颜色（前端使用）
    pub values: Vec<(usize, f64)>, // (index, value)
    pub visible: bool,             // 是否默认显示
}

/// 信号事件（买卖点、提醒点）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvent {
    pub index: usize, // 对应 bar index
    pub signal_type: SignalType,
    pub description: Option<String>,
}

/// 信号类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SignalType {
    Buy,
    Sell,
    Alert,
    Entry,
    Exit,
}

/// 规则评估结果（用于调试 / 可视化 entry/exit 条件）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleResult {
    pub name: String,                // 规则名称
    pub results: Vec<(usize, bool)>, // 每个 bar 的布尔结果
}

/// 策略绩效指标 metrics 盈亏曲线、胜率等
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub total_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub equity_curve: Vec<(usize, f64)>,           // 净值曲线
    pub drawdown_curve: Option<Vec<(usize, f64)>>, // 可选：最大回撤曲线
    pub total_return: Option<f64>,
}

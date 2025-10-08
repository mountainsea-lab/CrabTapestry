use serde::{Deserialize, Serialize};

/// 规则信号类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuleSignal {
    Buy,
    Sell,
    Hold,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuleParam {
    /// 参数名称，例如 "period"
    pub name: String,

    /// 参数值
    pub value: f64,

    /// 可选最小值
    pub min: Option<f64>,

    /// 可选最大值
    pub max: Option<f64>,

    /// 参数描述 / 注释
    pub description: Option<String>,

    /// 默认值（可用于配置表单或策略模板）
    pub default: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuleMeta {
    pub name: String,
    pub description: String,
    pub category: String,
    pub tags: Vec<String>,

    /// 参数模板（带注释、默认值、约束）
    pub params: Vec<RuleParam>,
}

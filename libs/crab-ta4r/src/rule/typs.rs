use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 规则信号类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuleSignal {
    Buy,
    Sell,
    Hold,
    Custom(String),
}

/// 规则元信息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuleMeta {
    pub name: String,
    pub description: String,
    pub params: HashMap<String, f64>,
}

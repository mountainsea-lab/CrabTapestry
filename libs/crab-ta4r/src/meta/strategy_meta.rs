use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyMeta {
    pub name: String,
    pub display_name: String,
    pub description: Option<String>,

    /// 使用到的指标及其参数
    pub indicators: Vec<String>,

    /// 使用到的规则及其组合逻辑（entry/exit）
    pub rules: Vec<String>,

    /// 可选参数（如止盈止损阈值）
    pub params: HashMap<String, f64>,

    /// 可选版本/作者等元信息
    pub version: Option<String>,
    pub author: Option<String>,
}

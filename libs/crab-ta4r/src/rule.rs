pub mod typs;

use crate::rule::typs::{RuleMeta, RuleSignal};
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;

/// 顶层规则接口 —— 与 ta4r 规则解耦，只提供统一封装能力
///
/// ✅ 不再要求 BarSeries 泛型
/// ✅ 只提供基于指标的 evaluate 接口
pub trait CrabRule: Send + Sync {
    /// 返回规则元信息
    fn meta(&self) -> &RuleMeta;

    /// 执行规则评估（如：OverIndicatorRule 比较两个指标）
    fn evaluate(&self, index: usize) -> RuleSignal;

    /// 获取规则名称（便于注册中心管理）
    fn name(&self) -> &str {
        &self.meta().name
    }
}

pub trait CrabRuleAny: Send + Sync {
    fn name(&self) -> &str;
    fn meta(&self) -> &RuleMeta;
    fn evaluate(&self, index: usize) -> RuleSignal;
}

pub mod typs;

use crate::rule::typs::{RuleMeta, RuleSignal};
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;

/// 顶层规则 Trait
pub trait CrabRule<N: TrNum, S: BarSeries<N>>: Send + Sync {
    /// 返回规则元信息
    fn meta(&self) -> &RuleMeta;

    /// 在给定序列与索引下评估信号
    fn evaluate(&self, series: &S, index: usize) -> RuleSignal;
}

use std::sync::Arc;
use ta4r::TradingRecord;
use ta4r::analysis::CostModel;
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;
use ta4r::rule::Rule;
use ta4r::strategy::Strategy;
use ta4r::strategy::base_strategy::BaseStrategy;

use crate::meta::view::{IndicatorLine, RuleResult, StrategyVisualization};

// =============================================================
// 🧩 Step 1. StrategyBundleTypes: 提供关联类型定义
// =============================================================
pub trait StrategyBundleTypes {
    type Num: TrNum + 'static;
    type CostBuy: CostModel<Self::Num> + Clone + Send + Sync + 'static;
    type CostSell: CostModel<Self::Num> + Clone + Send + Sync + 'static;
    type Series: BarSeries<Self::Num> + Send + Sync + 'static;
    type TradingRec: TradingRecord<Self::Num, Self::CostBuy, Self::CostSell, Self::Series> + Send + Sync + 'static;
}

// =============================================================
// 🧠 Step 2. StrategyBundle Trait: 策略封装体
// =============================================================
pub trait StrategyBundle: StrategyBundleTypes {
    type EntryRule: Rule<
            Num = Self::Num,
            CostBuy = Self::CostBuy,
            CostSell = Self::CostSell,
            Series = Self::Series,
            TradingRec = Self::TradingRec,
        > + Send
        + Sync
        + 'static;

    type ExitRule: Rule<
            Num = Self::Num,
            CostBuy = Self::CostBuy,
            CostSell = Self::CostSell,
            Series = Self::Series,
            TradingRec = Self::TradingRec,
        > + Send
        + Sync
        + 'static;

    /// 返回可视化指标（例如 SMA, RSI 等线）
    fn indicators_for_viz(&self) -> Vec<IndicatorLine>;

    /// 返回规则可视化数据（默认空实现）
    fn rules_for_viz(&self, _len: usize) -> Vec<RuleResult> {
        Vec::new()
    }

    /// 返回已构建好的基础策略
    fn strategy_arc(
        &self,
    ) -> Arc<
        BaseStrategy<
            Self::Num,
            Self::CostBuy,
            Self::CostSell,
            Self::Series,
            Self::TradingRec,
            Self::EntryRule,
            Self::ExitRule,
        >,
    >;

    /// 策略名称
    fn name(&self) -> &'static str;
}

// =============================================================
// 🎭 Step 3. 类型擦除 Trait: CrabStrategyAny
// =============================================================
pub trait CrabStrategyAny: Send + Sync {
    fn name(&self) -> &str;
    fn should_enter(&self, index: usize) -> bool;
    fn should_exit(&self, index: usize) -> bool;
    fn get_visualization_data(&self) -> Option<StrategyVisualization>;
}

// =============================================================
// ⚙️ Step 4. Blanket impl: 自动为任意 StrategyBundle 实现 CrabStrategyAny
// =============================================================
impl<T> CrabStrategyAny for T
where
    T: StrategyBundle + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.name()
    }

    fn should_enter(&self, index: usize) -> bool {
        self.strategy_arc().as_ref().should_enter(index, None)
    }

    fn should_exit(&self, index: usize) -> bool {
        self.strategy_arc().as_ref().should_exit(index, None)
    }

    fn get_visualization_data(&self) -> Option<StrategyVisualization> {
        let indicators = self.indicators_for_viz();
        let rules = self.rules_for_viz(indicators.first().map(|l| l.values.len()).unwrap_or(0));
        Some(StrategyVisualization {
            name: self.name().to_string(),
            indicators,
            signals: Vec::new(), // 应用层可额外注入交易信号
            rules,
            metrics: None,
        })
    }
}

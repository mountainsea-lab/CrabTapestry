mod factory;
mod registry;

use crate::any::any_indicator::IndicatorAny;
use crate::meta::view::{IndicatorLine, RuleResult, StrategyVisualization};
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use ta4r::TradingRecord;
use ta4r::analysis::CostModel;
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;
use ta4r::rule::Rule;
use ta4r::strategy::Strategy;
use ta4r::strategy::base_strategy::BaseStrategy;

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

    /// 返回可视化指标（Arc<dyn IndicatorAny>）
    fn indicators_for_viz(&self) -> Vec<Arc<dyn IndicatorAny>>;

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

    /// 可选：策略参数集合（可用于可视化 / 调试）
    fn params(&self) -> Option<HashMap<String, String>> {
        None
    }

    /// 可选：获取原始指标对象
    fn raw_indicators(&self) -> Option<Vec<Arc<dyn Any>>> {
        None
    }

    // /// 可选：获取交易记录 todo!(暂时延后)
    // fn trading_record(&self) -> Option<Arc<dyn Any>> {
    //     None
    // }

    /// 返回 series（必须由具体 bundle 提供）
    fn series(&self) -> Arc<RwLock<Self::Series>>;

    /// 返回 series 长度
    fn series_len(&self) -> usize {
        // 获取 RwLock 中的数据并调用 get_bar_count 方法
        self.series().read().get_bar_count()
    }
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

/// 可选扩展接口
pub trait CrabStrategyAnyEx: CrabStrategyAny {
    fn get_strategy_params(&self) -> Option<HashMap<String, String>> {
        None
    }
    fn get_raw_indicators(&self) -> Option<Vec<Arc<dyn IndicatorAny>>> {
        None
    }
    // todo!(暂时延后)
    // fn get_trading_record(&self) -> Option<Arc<dyn TradingRecordAny>>;
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
        let indicators_any = self.indicators_for_viz();
        let indicators_line: Vec<IndicatorLine> = indicators_any
            .iter()
            .map(|i| {
                let len = self.series_len();
                let values: Vec<(usize, f64)> =
                    (0..len).map(|idx| (idx, i.get_value(idx).unwrap_or(f64::NAN))).collect();
                IndicatorLine {
                    name: i.name().to_string(),
                    color: None,
                    values,
                    visible: true,
                }
            })
            .collect();

        let rules = self.rules_for_viz(self.series_len());

        Some(StrategyVisualization {
            name: self.name().to_string(),
            indicators: indicators_line,
            signals: Vec::new(), // 应用层可注入交易信号
            rules,
            metrics: None,
        })
    }
}

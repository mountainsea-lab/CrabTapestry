use crate::strategy::{StrategyBundle, StrategyBundleTypes};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ta4r::strategy::base_strategy::BaseStrategy;

/// 指标分类（趋势/动量/波动率/自定义）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IndicatorCategory {
    Trend,
    Momentum,
    Volume,
    Volatility,
    Custom(String),
}

/// 泛型别名：把 BaseStrategy 包装成 Arc
pub type GenericStrategy<T> = Arc<
    BaseStrategy<
        <T as StrategyBundleTypes>::Num,
        <T as StrategyBundleTypes>::CostBuy,
        <T as StrategyBundleTypes>::CostSell,
        <T as StrategyBundleTypes>::Series,
        <T as StrategyBundleTypes>::TradingRec,
        <T as StrategyBundle>::EntryRule,
        <T as StrategyBundle>::ExitRule,
    >,
>;

use crate::rule::CrabRuleAny;
use crate::strategy::view::StrategyVisualization;
use parking_lot::RwLock;
use std::sync::Arc;
use ta4r::analysis::CostModel;
use ta4r::bar::types::{Bar, BarSeries};
use ta4r::indicators::Indicator;
use ta4r::num::TrNum;
use ta4r::rule::Rule;
use ta4r::strategy::base_strategy::BaseStrategy;
use ta4r::strategy::Strategy;
use ta4r::TradingRecord;

pub mod registry;
pub mod strategy_meta;
pub mod view;

/// 类型擦除后的统一策略接口
pub trait CrabStrategyAny: Send + Sync {
    /// 策略名称
    fn name(&self) -> &str;

    /// 判断是否应进场
    fn should_enter(&self, index: usize) -> bool;

    /// 判断是否应出场
    fn should_exit(&self, index: usize) -> bool;

    /// ✅ 输出可视化信息（指标 + 信号）
    fn get_visualization_data(&self) -> Option<StrategyVisualization>;
}

pub struct CrabStrategyWrapper<BS, S> {
    pub name: String,
    pub inner: Arc<BS>,
    pub series: Arc<RwLock<S>>, // ✅ 直接存储应用层传入的 Series
}

impl<N, Cb, Cs, Ser, R, E, X> CrabStrategyAny for CrabStrategyWrapper<BaseStrategy<N, Cb, Cs, Ser, R, E, X>, Ser>
where
    N: TrNum + Send + Sync + 'static,
    Cb: CostModel<N> + Clone + Send + Sync + 'static,
    Cs: CostModel<N> + Clone + Send + Sync + 'static,
    Ser: BarSeries<N> + Send + Sync + 'static, // 👈 核心：Ser 需要 Send + Sync
    R: TradingRecord<N, Cb, Cs, Ser> + Send + Sync + 'static,
    E: Rule<Num = N, CostBuy = Cb, CostSell = Cs, Series = Ser, TradingRec = R> + Send + Sync + 'static,
    X: Rule<Num = N, CostBuy = Cb, CostSell = Cs, Series = Ser, TradingRec = R> + Send + Sync + 'static,
{
    fn should_enter(&self, index: usize) -> bool {
        self.inner.as_ref().should_enter(index, None)
    }

    fn should_exit(&self, index: usize) -> bool {
        self.inner.as_ref().should_exit(index, None)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get_visualization_data(&self) -> Option<StrategyVisualization> {
        todo!()
    }
}

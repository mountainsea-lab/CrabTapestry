use crate::any::any_series::AnySeries;
use crate::strategy::CrabStrategyAny;
use parking_lot::RwLock;
use std::sync::Arc;
/// 动态插件策略，或者配置文件驱动的策略列表，需要根据运行时配置生成策略
///策略与 series 解耦，然后统一通过接口构建策略
pub trait StrategyFactory: Send + Sync {
    fn build(&self, series: Arc<RwLock<dyn AnySeries>>) -> Arc<dyn CrabStrategyAny>;
}

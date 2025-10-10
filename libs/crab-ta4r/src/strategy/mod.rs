pub mod registry;
pub mod strategy_meta;
mod view;

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

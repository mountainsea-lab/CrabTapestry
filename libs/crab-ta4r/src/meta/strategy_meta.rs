use crate::meta::param::{ParamSpec, ParamValue};
use crate::strategy::CrabStrategyAny;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct StrategyInitContextAny {
    pub series: Arc<dyn Any + Send + Sync>, // BarSeries 类型擦除
    pub params: HashMap<String, ParamValue>,
}

#[derive(Clone)]
pub struct StrategyMeta {
    pub name: String,
    pub display_name: String,
    pub description: Option<String>,
    pub params: HashMap<String, ParamSpec>,
    pub factory: Arc<dyn Fn(StrategyInitContextAny) -> Arc<dyn CrabStrategyAny> + Send + Sync>,
}

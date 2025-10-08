use crate::indicator::CrabIndicator;
use crate::meta::CrabIndicatorAny;
use crate::meta::param::{ParamSpec, ParamValue};
use crate::meta::view::VisualizationConfig;
use crate::types::IndicatorCategory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use ta4r::bar::builder::types::BarSeriesRef;
use ta4r::bar::types::BarSeries;
use ta4r::num::TrNum;

/// 构建指标实例的上下文
pub struct IndicatorInitContext<N: TrNum, S: BarSeries<N>> {
    /// 指标绑定的数据序列
    pub series: BarSeriesRef<S>,

    /// 构造参数：name -> value
    pub params: HashMap<String, ParamValue>,

    /// 可选运行上下文信息
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub period: Option<String>,

    /// 可选附加信息（如策略 id、数据版本等）
    pub metadata: HashMap<String, String>,
}

// --------------------------- 指标元信息 ---------------------------
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndicatorMeta {
    pub name: String,
    pub display_name: String,
    pub description: Option<String>,
    pub category: String,
    pub params: HashMap<String, ParamSpec>,
    pub factory: Arc<dyn Fn(Box<dyn std::any::Any>) -> Arc<dyn CrabIndicatorAny> + Send + Sync>,
    pub unstable_bars: usize,
    pub version: Option<String>,
    pub author: Option<String>,
}

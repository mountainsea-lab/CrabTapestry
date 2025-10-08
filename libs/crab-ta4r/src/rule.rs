use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use ta4r::num::TrNum;
use ta4r::rule::Rule;

/// --------------------------- 规则元信息 ---------------------------
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuleMeta {
    pub name: String,
    pub description: Option<String>,
    pub category: Option<String>,
    /// 参数及注释（可选）
    pub params: HashMap<String, (f64, Option<String>)>,
}

/// --------------------------- 规则信号 ---------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleSignal {
    Buy,
    Sell,
    None,
}

/// --------------------------- 顶层规则 Trait ---------------------------
/// 自定义规则实现时可以实现此 trait
pub trait CrabRule: Send + Sync {
    /// 返回规则元信息
    fn meta(&self) -> &RuleMeta;

    /// 执行规则评估
    fn evaluate(&self, index: usize, ctx: Option<&dyn Any>) -> RuleSignal;

    /// 默认名称取自 meta
    fn name(&self) -> &str {
        &self.meta().name
    }
}

/// --------------------------- 类型擦除接口 ---------------------------
/// 便于注册中心存储和策略运行时统一调用
pub trait CrabRuleAny: Send + Sync {
    fn name(&self) -> &str;
    fn meta(&self) -> &RuleMeta;
    fn evaluate(&self, index: usize, ctx: Option<&dyn Any>) -> RuleSignal;
}

/// 自动类型擦除适配：自定义规则
impl<T> CrabRuleAny for T
where
    T: CrabRule + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        CrabRule::name(self)
    }

    fn meta(&self) -> &RuleMeta {
        CrabRule::meta(self)
    }

    fn evaluate(&self, index: usize, ctx: Option<&dyn Any>) -> RuleSignal {
        CrabRule::evaluate(self, index, ctx)
    }
}

/// --------------------------- Ta4r Rule 适配器 ---------------------------
/// 将 ta4r::Rule 包装为 CrabRuleAny
pub struct Ta4rRuleAdapter<R> {
    pub meta: RuleMeta,
    pub inner: Arc<R>,
}

impl<R, N> Ta4rRuleAdapter<R>
where
    N: TrNum + 'static,
    R: Rule<Num = N> + Send + Sync + 'static,
{
    pub fn new(meta: RuleMeta, inner: Arc<R>) -> Self {
        Self { meta, inner }
    }
}

impl<R, N> CrabRuleAny for Ta4rRuleAdapter<R>
where
    N: TrNum + 'static,
    R: Rule<Num = N> + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.meta.name
    }

    fn meta(&self) -> &RuleMeta {
        &self.meta
    }

    fn evaluate(&self, index: usize, _ctx: Option<&dyn Any>) -> RuleSignal {
        if self.inner.is_satisfied(index) {
            RuleSignal::Buy
        } else {
            RuleSignal::Sell
        }
    }
}

/// --------------------------- 示例自定义规则 ---------------------------
pub struct MyCustomRule {
    meta: RuleMeta,
}

impl MyCustomRule {
    pub fn new(name: &str) -> Self {
        Self {
            meta: RuleMeta {
                name: name.to_string(),
                description: Some("自定义规则示例".to_string()),
                category: Some("Custom".to_string()),
                params: HashMap::new(),
            },
        }
    }
}

impl CrabRule for MyCustomRule {
    fn meta(&self) -> &RuleMeta {
        &self.meta
    }

    fn evaluate(&self, index: usize, _ctx: Option<&dyn Any>) -> RuleSignal {
        if index % 2 == 0 {
            RuleSignal::Buy
        } else {
            RuleSignal::Sell
        }
    }
}

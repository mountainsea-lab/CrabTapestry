#[derive(Clone, Debug)]
pub enum ParamType {
    Int,
    Float,
    Bool,
    String,
    Enum(Vec<String>),
}
#[derive(Clone, Debug)]
pub enum ParamValue {
    Int(usize),
    Float(f64),
    Bool(bool),
    String(String),
}
#[derive(Clone, Debug)]
pub struct ParamSpec {
    pub name: String,
    pub param_type: ParamType,
    pub default: Option<ParamValue>,
}

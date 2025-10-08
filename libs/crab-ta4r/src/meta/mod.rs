pub mod indicator_meta;
pub mod param;
pub mod view;

// --------------------------- 类型擦除 trait ---------------------------
pub trait CrabIndicatorAny: Send + Sync {
    fn name(&self) -> &str;
    fn get_value_as_f64(&self, index: usize) -> f64;
}

use std::sync::Once;

/// 全局 Once，确保 .env 只加载一次
static INIT: Once = Once::new();

pub fn is_local() -> bool {
    std::env::var("LOCAL").is_ok()
}

pub fn must_get_env(key: &str) -> String {
    match std::env::var(key) {
        Ok(val) => val,
        Err(_) => panic!("{} must be set", key),
    }
}

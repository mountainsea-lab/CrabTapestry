pub fn is_local() -> bool {
    std::env::var("LOCAL").is_ok()
}

pub fn must_get_env(key: &str) -> String {
    match std::env::var(key) {
        Ok(val) => val,
        Err(_) => panic!("{} must be set", key),
    }
}

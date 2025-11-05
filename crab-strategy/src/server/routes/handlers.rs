pub mod log_handlers;
pub mod trader_handlers;
pub mod tradingview_handlers;

pub fn index() -> &'static str {
    "Welcome to crab strategy server!"
}

pub fn ping() -> &'static str {
    "ping pong!"
}

pub fn version() -> &'static str {
    "crab strategy version 0.1.0"
}

pub fn sysinfo() -> &'static str {
    "sysinfo info: hello , I am a crab strategy server"
}

pub fn health() -> &'static str {
    "if you ask: hao are you,oh I am ok"
}

use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Eq)]
pub struct BarKey {
    pub exchange: String,
    pub symbol: String,
    pub period: String, // e.g. "1m", "5m", "1h"
}

impl BarKey {
    pub fn new(exchange: &str, symbol: &str, period: &str) -> Self {
        Self {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            period: period.to_string(),
        }
    }

    pub fn id(&self) -> String {
        format!("{}:{}:{}", self.exchange, self.symbol, self.period)
    }
}

impl PartialEq for BarKey {
    fn eq(&self, other: &Self) -> bool {
        self.exchange == other.exchange
            && self.symbol == other.symbol
            && self.period == other.period
    }
}

impl Hash for BarKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.exchange.hash(state);
        self.symbol.hash(state);
        self.period.hash(state);
    }
}

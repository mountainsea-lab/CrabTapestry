use crab_infras::config::sub_config::SubscriptionConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub app: AppDetails,
    pub subscriptions: Vec<SubscriptionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppDetails {
    pub lookback_days: i64,
}

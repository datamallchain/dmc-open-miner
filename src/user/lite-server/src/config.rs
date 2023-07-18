use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
pub struct LiteServerConfig {
    pub account_name: String,
    pub account_private_key: String,
    pub spv_url: String,
    #[serde(default="default_challenge_atomic_interval")]
    pub challenge_atomic_interval: Duration,
    #[serde(default="default_challenge_interval")]
    pub challenge_interval: Duration,
    #[serde(default="default_auto_onchain_challenge")]
    pub auto_onchain_challenge: bool
}

fn default_challenge_atomic_interval() -> Duration {
    Duration::from_secs(24*60*60)
}

fn default_challenge_interval() -> Duration {
    Duration::from_secs(60*60)
}

fn default_auto_onchain_challenge() -> bool {
    true
}

pub const JOURNAL_ADDR: &str = "127.0.0.1:5452";
pub const NODE_ADDR: &str = "127.0.0.1:5451";
pub const CONTRACT_ADDR: &str = "127.0.0.1:5450";
pub const ACCOUNT_ADDR: &str = "127.0.0.1:5448";
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HeartbeatOptions {
    pub interval_millis: u64,
    pub retry_interval_millis: u64,
}

impl Default for HeartbeatOptions {
    fn default() -> Self {
        Self {
            interval_millis: 5000,
            retry_interval_millis: 5000,
        }
    }
}

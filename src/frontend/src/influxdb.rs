use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InfluxdbOptions {
    pub enable: bool,
}

impl Default for InfluxdbOptions {
    fn default() -> Self {
        Self { enable: true }
    }
}

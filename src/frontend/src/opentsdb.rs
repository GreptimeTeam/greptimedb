use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpentsdbOptions {
    pub addr: String,
    pub runtime_size: u32,
}

impl Default for OpentsdbOptions {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:4242".to_string(),
            runtime_size: 2,
        }
    }
}

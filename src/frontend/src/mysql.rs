use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlOptions {
    pub addr: String,
    pub runtime_size: usize,
}

impl Default for MysqlOptions {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:4002".to_string(),
            runtime_size: 2,
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresOptions {
    pub addr: String,
    pub runtime_size: u32,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:4003".to_string(),
            runtime_size: 2,
        }
    }
}

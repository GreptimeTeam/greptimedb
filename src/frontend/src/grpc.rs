use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcOptions {
    pub addr: String,
    pub runtime_size: usize,
}

impl Default for GrpcOptions {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:4001".to_string(),
            runtime_size: 8,
        }
    }
}

use serde::Deserialize;
use serde::Serialize;

pub mod client;
pub mod error;
#[cfg(test)]
mod mocks;
pub mod rpc;

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaClientOpts {
    pub metasrv_addr: String,
    pub timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

impl Default for MetaClientOpts {
    fn default() -> Self {
        Self {
            metasrv_addr: "127.0.0.1:3002".to_string(),
            timeout_millis: 3_000u64,
            connect_timeout_millis: 5_000u64,
            tcp_nodelay: true,
        }
    }
}

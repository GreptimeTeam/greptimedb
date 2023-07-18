// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_grpc::channel_manager;
use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::LoggingOptions;
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;

use crate::error::{self, Result};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
    pub datanode_lease_secs: i64,
    pub selector: SelectorType,
    pub use_memory_store: bool,
    pub disable_region_failover: bool,
    pub http_opts: HttpOptions,
    pub logging: LoggingOptions,
    pub procedure: ProcedureConfig,
    pub datanode: DatanodeOptions,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3002".to_string(),
            server_addr: "127.0.0.1:3002".to_string(),
            store_addr: "127.0.0.1:2379".to_string(),
            datanode_lease_secs: 15,
            selector: SelectorType::default(),
            use_memory_store: false,
            disable_region_failover: false,
            http_opts: HttpOptions::default(),
            logging: LoggingOptions::default(),
            procedure: ProcedureConfig::default(),
            datanode: DatanodeOptions::default(),
        }
    }
}

// Options for datanode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DatanodeOptions {
    pub client_options: DatanodeClientOptions,
}

// Options for datanode client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatanodeClientOptions {
    pub timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

impl Default for DatanodeClientOptions {
    fn default() -> Self {
        Self {
            timeout_millis: channel_manager::DEFAULT_GRPC_REQUEST_TIMEOUT_SECS * 1000,
            connect_timeout_millis: channel_manager::DEFAULT_GRPC_CONNECT_TIMEOUT_SECS * 1000,
            tcp_nodelay: true,
        }
    }
}

impl MetaSrvOptions {
    pub fn to_toml_string(&self) -> String {
        toml::to_string(&self).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SelectorType {
    #[default]
    LoadBased,
    LeaseBased,
}

impl TryFrom<&str> for SelectorType {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "LoadBased" => Ok(SelectorType::LoadBased),
            "LeaseBased" => Ok(SelectorType::LeaseBased),
            other => error::UnsupportedSelectorTypeSnafu {
                selector_type: other,
            }
            .fail(),
        }
    }
}

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaClientOptions {
    pub metasrv_addrs: Vec<String>,
    pub timeout_millis: u64,
    #[serde(default = "default_ddl_timeout_millis")]
    pub ddl_timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

fn default_ddl_timeout_millis() -> u64 {
    10_000u64
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout_millis: 3_000u64,
            ddl_timeout_millis: default_ddl_timeout_millis(),
            connect_timeout_millis: 5_000u64,
            tcp_nodelay: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SelectorType;
    use crate::error::Result;

    #[test]
    fn test_default_selector_type() {
        assert_eq!(SelectorType::LoadBased, SelectorType::default());
    }

    #[test]
    fn test_convert_str_to_selector_type() {
        let leasebased = "LeaseBased";
        let selector_type = leasebased.try_into().unwrap();
        assert_eq!(SelectorType::LeaseBased, selector_type);

        let loadbased = "LoadBased";
        let selector_type = loadbased.try_into().unwrap();
        assert_eq!(SelectorType::LoadBased, selector_type);

        let unknown = "unknown";
        let selector_type: Result<SelectorType> = unknown.try_into();
        assert!(selector_type.is_err());
    }
}

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

use common_telemetry::logging::LoggingOptions;
use meta_client::MetaClientOptions;
use serde::{Deserialize, Serialize};
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use servers::Mode;

use crate::service_config::{
    DatanodeOptions, GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, OtlpOptions,
    PostgresOptions, PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FrontendOptions {
    pub mode: Mode,
    pub node_id: Option<String>,
    pub heartbeat: HeartbeatOptions,
    pub http_options: HttpOptions,
    pub grpc_options: GrpcOptions,
    pub mysql_options: MysqlOptions,
    pub postgres_options: PostgresOptions,
    pub opentsdb_options: OpentsdbOptions,
    pub influxdb_options: InfluxdbOptions,
    pub prom_store_options: PromStoreOptions,
    pub otlp_options: OtlpOptions,
    pub meta_client_options: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub datanode: DatanodeOptions,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            node_id: None,
            heartbeat: HeartbeatOptions::default(),
            http_options: HttpOptions::default(),
            grpc_options: GrpcOptions::default(),
            mysql_options: MysqlOptions::default(),
            postgres_options: PostgresOptions::default(),
            opentsdb_options: OpentsdbOptions::default(),
            influxdb_options: InfluxdbOptions::default(),
            prom_store_options: PromStoreOptions::default(),
            otlp_options: OtlpOptions::default(),
            meta_client_options: None,
            logging: LoggingOptions::default(),
            datanode: DatanodeOptions::default(),
        }
    }
}

impl FrontendOptions {
    pub fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client_options.metasrv_addrs"])
    }

    pub fn to_toml_string(&self) -> String {
        toml::to_string(&self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml() {
        let opts = FrontendOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: FrontendOptions = toml::from_str(&toml_string).unwrap();
    }
}

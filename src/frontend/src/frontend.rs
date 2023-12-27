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
use servers::export_metrics::ExportMetricsOption;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use servers::Mode;
use snafu::prelude::*;

use crate::error::{Result, TomlFormatSnafu};
use crate::service_config::{
    DatanodeOptions, GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, OtlpOptions,
    PostgresOptions, PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FrontendOptions {
    pub mode: Mode,
    pub node_id: Option<String>,
    pub default_timezone: Option<String>,
    pub heartbeat: HeartbeatOptions,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub prom_store: PromStoreOptions,
    pub otlp: OtlpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub datanode: DatanodeOptions,
    pub user_provider: Option<String>,
    pub export_metrics: ExportMetricsOption,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            node_id: None,
            default_timezone: None,
            heartbeat: HeartbeatOptions::frontend_default(),
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            prom_store: PromStoreOptions::default(),
            otlp: OtlpOptions::default(),
            meta_client: None,
            logging: LoggingOptions::default(),
            datanode: DatanodeOptions::default(),
            user_provider: None,
            export_metrics: ExportMetricsOption::default(),
        }
    }
}

impl FrontendOptions {
    pub fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client.metasrv_addrs"])
    }

    pub fn to_toml_string(&self) -> String {
        toml::to_string(&self).unwrap()
    }
}

pub trait TomlSerializable {
    fn to_toml(&self) -> Result<String>;
}

impl TomlSerializable for FrontendOptions {
    fn to_toml(&self) -> Result<String> {
        toml::to_string(&self).context(TomlFormatSnafu)
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

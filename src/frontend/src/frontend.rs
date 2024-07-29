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

use common_config::config::Configurable;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use meta_client::MetaClientOptions;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;

use crate::service_config::{
    DatanodeOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, OtlpOptions, PostgresOptions,
    PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FrontendOptions {
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
    pub tracing: TracingOptions,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
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
            tracing: TracingOptions::default(),
        }
    }
}

impl Configurable for FrontendOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client.metasrv_addrs"])
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

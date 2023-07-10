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
use servers::http::HttpOptions;
use servers::Mode;

use crate::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PrometheusOptions,
    PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FrontendOptions {
    pub mode: Mode,
    pub heartbeat_interval_millis: u64,
    pub retry_interval_millis: u64,
    pub http_options: Option<HttpOptions>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prom_store_options: Option<PromStoreOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub meta_client_options: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            heartbeat_interval_millis: 5000,
            retry_interval_millis: 5000,
            http_options: Some(HttpOptions::default()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prom_store_options: Some(PromStoreOptions::default()),
            prometheus_options: Some(PrometheusOptions::default()),
            meta_client_options: None,
            logging: LoggingOptions::default(),
        }
    }
}

impl FrontendOptions {
    pub fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client_options.metasrv_addrs"])
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

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

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_config::config::Configurable;
use common_options::datanode::DatanodeClientOptions;
use common_telemetry::logging::{LoggingOptions, SlowQueryOptions, TracingOptions};
use meta_client::MetaClientOptions;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::export_metrics::{ExportMetricsOption, ExportMetricsTask};
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use servers::server::ServerHandlers;
use snafu::ResultExt;

use crate::error;
use crate::error::Result;
use crate::heartbeat::HeartbeatTask;
use crate::instance::prom_store::ExportMetricHandler;
use crate::instance::Instance;
use crate::service_config::{
    InfluxdbOptions, JaegerOptions, MysqlOptions, OpentsdbOptions, OtlpOptions, PostgresOptions,
    PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
    pub jaeger: JaegerOptions,
    pub otlp: OtlpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub datanode: DatanodeClientOptions,
    pub user_provider: Option<String>,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub max_in_flight_write_bytes: Option<ReadableSize>,
    pub slow_query: Option<SlowQueryOptions>,
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
            jaeger: JaegerOptions::default(),
            prom_store: PromStoreOptions::default(),
            otlp: OtlpOptions::default(),
            meta_client: None,
            logging: LoggingOptions::default(),
            datanode: DatanodeClientOptions::default(),
            user_provider: None,
            export_metrics: ExportMetricsOption::default(),
            tracing: TracingOptions::default(),
            query: QueryOptions::default(),
            max_in_flight_write_bytes: None,
            slow_query: Some(SlowQueryOptions::default()),
        }
    }
}

impl Configurable for FrontendOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client.metasrv_addrs"])
    }
}

/// The [`Frontend`] struct is the main entry point for the frontend service
/// which contains server handlers, frontend instance and some background tasks.
pub struct Frontend {
    pub instance: Arc<Instance>,
    pub servers: ServerHandlers,
    pub heartbeat_task: Option<HeartbeatTask>,
    pub export_metrics_task: Option<ExportMetricsTask>,
}

impl Frontend {
    pub async fn start(&mut self) -> Result<()> {
        if let Some(t) = &self.heartbeat_task {
            t.start().await?;
        }

        if let Some(t) = self.export_metrics_task.as_ref() {
            if t.send_by_handler {
                let inserter = self.instance.inserter().clone();
                let statement_executor = self.instance.statement_executor().clone();
                let handler = ExportMetricHandler::new_handler(inserter, statement_executor);
                t.start(Some(handler)).context(error::StartServerSnafu)?
            } else {
                t.start(None).context(error::StartServerSnafu)?;
            }
        }

        self.servers
            .start_all()
            .await
            .context(error::StartServerSnafu)
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.servers
            .shutdown_all()
            .await
            .context(error::ShutdownServerSnafu)
    }

    pub fn server_handlers(&self) -> &ServerHandlers {
        &self.servers
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

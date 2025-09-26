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

use common_base::readable_size::ReadableSize;
use common_config::{Configurable, KvBackendConfig};
use common_options::memory::MemoryOptions;
use common_telemetry::logging::{LoggingOptions, SlowQueryOptions, TracingOptions};
use common_wal::config::DatanodeWalConfig;
use datanode::config::{DatanodeOptions, ProcedureConfig, RegionEngineConfig, StorageConfig};
use file_engine::config::EngineConfig as FileEngineConfig;
use flow::FlowConfig;
use frontend::frontend::FrontendOptions;
use frontend::service_config::{
    InfluxdbOptions, JaegerOptions, MysqlOptions, OpentsdbOptions, PostgresOptions,
    PromStoreOptions,
};
use mito2::config::MitoConfig;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StandaloneOptions {
    pub enable_telemetry: bool,
    pub default_timezone: Option<String>,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub jaeger: JaegerOptions,
    pub prom_store: PromStoreOptions,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub metadata_store: KvBackendConfig,
    pub procedure: ProcedureConfig,
    pub flow: FlowConfig,
    pub logging: LoggingOptions,
    pub user_provider: Option<String>,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub max_in_flight_write_bytes: Option<ReadableSize>,
    pub slow_query: SlowQueryOptions,
    pub query: QueryOptions,
    pub memory: MemoryOptions,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            enable_telemetry: true,
            default_timezone: None,
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            jaeger: JaegerOptions::default(),
            prom_store: PromStoreOptions::default(),
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            metadata_store: KvBackendConfig::default(),
            procedure: ProcedureConfig::default(),
            flow: FlowConfig::default(),
            logging: LoggingOptions::default(),
            export_metrics: ExportMetricsOption::default(),
            user_provider: None,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            tracing: TracingOptions::default(),
            init_regions_in_background: false,
            init_regions_parallelism: 16,
            max_in_flight_write_bytes: None,
            slow_query: SlowQueryOptions::default(),
            query: QueryOptions::default(),
            memory: MemoryOptions::default(),
        }
    }
}

impl Configurable for StandaloneOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints"])
    }
}

/// The [`StandaloneOptions`] is only defined in `standalone` crate,
/// we don't want to make `frontend` depends on it, so impl [`Into`]
/// rather than [`From`].
#[allow(clippy::from_over_into)]
impl Into<FrontendOptions> for StandaloneOptions {
    fn into(self) -> FrontendOptions {
        self.frontend_options()
    }
}

impl StandaloneOptions {
    pub fn frontend_options(&self) -> FrontendOptions {
        let cloned_opts = self.clone();
        FrontendOptions {
            default_timezone: cloned_opts.default_timezone,
            http: cloned_opts.http,
            grpc: cloned_opts.grpc,
            mysql: cloned_opts.mysql,
            postgres: cloned_opts.postgres,
            opentsdb: cloned_opts.opentsdb,
            influxdb: cloned_opts.influxdb,
            jaeger: cloned_opts.jaeger,
            prom_store: cloned_opts.prom_store,
            meta_client: None,
            logging: cloned_opts.logging,
            user_provider: cloned_opts.user_provider,
            // Handle the export metrics task run by standalone to frontend for execution
            export_metrics: cloned_opts.export_metrics,
            max_in_flight_write_bytes: cloned_opts.max_in_flight_write_bytes,
            slow_query: cloned_opts.slow_query,
            ..Default::default()
        }
    }

    pub fn datanode_options(&self) -> DatanodeOptions {
        let cloned_opts = self.clone();
        DatanodeOptions {
            node_id: Some(0),
            enable_telemetry: cloned_opts.enable_telemetry,
            wal: cloned_opts.wal,
            storage: cloned_opts.storage,
            region_engine: cloned_opts.region_engine,
            grpc: cloned_opts.grpc,
            init_regions_in_background: cloned_opts.init_regions_in_background,
            init_regions_parallelism: cloned_opts.init_regions_parallelism,
            query: cloned_opts.query,
            ..Default::default()
        }
    }

    /// Sanitize the `StandaloneOptions` to ensure the config is valid.
    pub fn sanitize(&mut self) {
        if self.storage.is_object_storage() {
            self.storage
                .store
                .cache_config_mut()
                .unwrap()
                .sanitize(&self.storage.data_home);
        }
    }
}

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

//! Datanode configurations

use common_base::readable_size::ReadableSize;
use common_config::{Configurable, DEFAULT_DATA_HOME};
use common_options::memory::MemoryOptions;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_wal::config::DatanodeWalConfig;
use common_workload::{DatanodeWorkloadType, sanitize_workload_types};
use file_engine::config::EngineConfig as FileEngineConfig;
use meta_client::MetaClientOptions;
use metric_engine::config::EngineConfig as MetricEngineConfig;
use mito2::config::MitoConfig;
pub(crate) use object_store::config::ObjectStoreConfig;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StorageConfig {
    /// The working directory of database
    pub data_home: String,
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
    /// Object storage providers
    pub providers: Vec<ObjectStoreConfig>,
}

impl StorageConfig {
    /// Returns true when the default storage config is a remote object storage service such as AWS S3, etc.
    pub fn is_object_storage(&self) -> bool {
        self.store.is_object_storage()
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_home: DEFAULT_DATA_HOME.to_string(),
            store: ObjectStoreConfig::default(),
            providers: vec![],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DatanodeOptions {
    pub node_id: Option<u64>,
    pub workload_types: Vec<DatanodeWorkloadType>,
    pub require_lease_before_startup: bool,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub grpc: GrpcOptions,
    pub heartbeat: HeartbeatOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub max_concurrent_queries: usize,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub logging: LoggingOptions,
    pub enable_telemetry: bool,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub memory: MemoryOptions,

    /// Deprecated options, please use the new options instead.
    #[deprecated(note = "Please use `grpc.addr` instead.")]
    pub rpc_addr: Option<String>,
    #[deprecated(note = "Please use `grpc.hostname` instead.")]
    pub rpc_hostname: Option<String>,
    #[deprecated(note = "Please use `grpc.runtime_size` instead.")]
    pub rpc_runtime_size: Option<usize>,
    #[deprecated(note = "Please use `grpc.max_recv_message_size` instead.")]
    pub rpc_max_recv_message_size: Option<ReadableSize>,
    #[deprecated(note = "Please use `grpc.max_send_message_size` instead.")]
    pub rpc_max_send_message_size: Option<ReadableSize>,
}

impl DatanodeOptions {
    /// Sanitize the `DatanodeOptions` to ensure the config is valid.
    pub fn sanitize(&mut self) {
        sanitize_workload_types(&mut self.workload_types);

        if self.storage.is_object_storage() {
            self.storage
                .store
                .cache_config_mut()
                .unwrap()
                .sanitize(&self.storage.data_home);
        }
    }
}

impl Default for DatanodeOptions {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            node_id: None,
            workload_types: vec![DatanodeWorkloadType::Hybrid],
            require_lease_before_startup: false,
            init_regions_in_background: false,
            init_regions_parallelism: 16,
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:3001"),
            http: HttpOptions::default(),
            meta_client: None,
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            max_concurrent_queries: 0,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            logging: LoggingOptions::default(),
            heartbeat: HeartbeatOptions::datanode_default(),
            enable_telemetry: true,
            export_metrics: ExportMetricsOption::default(),
            tracing: TracingOptions::default(),
            query: QueryOptions::default(),
            memory: MemoryOptions::default(),

            // Deprecated options
            rpc_addr: None,
            rpc_hostname: None,
            rpc_runtime_size: None,
            rpc_max_recv_message_size: None,
            rpc_max_send_message_size: None,
        }
    }
}

impl Configurable for DatanodeOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client.metasrv_addrs", "wal.broker_endpoints"])
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RegionEngineConfig {
    #[serde(rename = "mito")]
    Mito(MitoConfig),
    #[serde(rename = "file")]
    File(FileEngineConfig),
    #[serde(rename = "metric")]
    Metric(MetricEngineConfig),
}

#[cfg(test)]
mod tests {
    use common_base::secrets::ExposeSecret;

    use super::*;

    #[test]
    fn test_toml() {
        let opts = DatanodeOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: DatanodeOptions = toml::from_str(&toml_string).unwrap();
    }

    #[test]
    fn test_secstr() {
        let toml_str = r#"
            [storage]
            type = "S3"
            access_key_id = "access_key_id"
            secret_access_key = "secret_access_key"
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert_eq!(
                    "SecretBox<alloc::string::String>([REDACTED])".to_string(),
                    format!("{:?}", cfg.connection.access_key_id)
                );
                assert_eq!(
                    "access_key_id",
                    cfg.connection.access_key_id.expose_secret()
                );
            }
            _ => unreachable!(),
        }
    }
    #[test]
    fn test_skip_ssl_validation_config() {
        // Test with skip_ssl_validation = true
        let toml_str_true = r#"
            [storage]
            type = "S3"
            [storage.http_client]
            skip_ssl_validation = true
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_true).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }

        // Test with skip_ssl_validation = false
        let toml_str_false = r#"
            [storage]
            type = "S3"
            [storage.http_client]
            skip_ssl_validation = false
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_false).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(!cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }
        // Test default value (should be false)
        let toml_str_default = r#"
            [storage]
            type = "S3"
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_default).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(!cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_cache_config() {
        let toml_str = r#"
            [storage]
            data_home = "test_data_home"
            type = "S3"
            [storage.cache_config]
            enable_read_cache = true
        "#;
        let mut opts: DatanodeOptions = toml::from_str(toml_str).unwrap();
        opts.sanitize();
        assert!(opts.storage.store.cache_config().unwrap().enable_read_cache);
        assert_eq!(
            opts.storage.store.cache_config().unwrap().cache_path,
            "test_data_home"
        );
    }
}

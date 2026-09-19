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

use std::time::Duration;

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
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StorageConfig {
    /// The working directory of database
    pub data_home: String,
    /// Root directory for standalone SQL access to local files.
    ///
    /// Defaults to `<data_home>/copy` when `data_home` is a local path.
    /// Distributed deployments always disable SQL access to local files.
    pub copy_root: Option<String>,
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
            copy_root: None,
            store: ObjectStoreConfig::default(),
            providers: vec![],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DatanodeOptions {
    pub node_id: Option<u64>,
    pub default_column_prefix: Option<String>,
    pub workload_types: Vec<DatanodeWorkloadType>,
    pub require_lease_before_startup: bool,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub grpc: GrpcOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub max_concurrent_queries: usize,
    /// Timeout to acquire a permit from the concurrent query limiter when
    /// `max_concurrent_queries` is reached. Only effective when the limiter is enabled.
    #[serde(with = "humantime_serde")]
    pub concurrent_query_limiter_timeout: Duration,
    /// Options for different store engines.
    ///
    /// Note: no field-level `#[serde(default)]` here — the struct-level
    /// `#[serde(default)]` above already fills a missing field from
    /// `DatanodeOptions::default().region_engine` (mito + file). Adding a
    /// field-level `default` with no path would instead fall back to
    /// `Vec::default()` (an empty list), silently dropping both default
    /// engines whenever `region_engine` is absent from the input.
    #[serde(deserialize_with = "deserialize_region_engine_options")]
    pub region_engine: Vec<RegionEngineConfig>,
    pub logging: LoggingOptions,
    pub enable_telemetry: bool,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub memory: MemoryOptions,

    /// Environment variable keys to read and report in heartbeat messages.
    /// The values of these env vars at startup will be sent to metasrv.
    pub heartbeat_env_vars: Vec<String>,

    /// Deprecated options, please use the new options instead.
    #[deprecated(note = "Please use `grpc.bind_addr` instead.")]
    pub rpc_addr: Option<String>,
    #[deprecated(note = "Please use `grpc.server_addr` instead.")]
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
            default_column_prefix: None,
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
            concurrent_query_limiter_timeout: Duration::from_millis(100),
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            logging: LoggingOptions::default(),
            enable_telemetry: true,
            tracing: TracingOptions::default(),
            query: QueryOptions::default(),
            memory: MemoryOptions::default(),
            heartbeat_env_vars: vec![],

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
        Some(&[
            "heartbeat_env_vars",
            "meta_client.metasrv_addrs",
            "wal.broker_endpoints",
        ])
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

/// A serde `deserialize_with` helper for the `region_engine` field.
///
/// `region_engine` is normally a sequence of single-key tables, e.g.
/// `[[region_engine]]` / `[region_engine.mito]` in TOML, which serializes as a
/// JSON array of externally-tagged [`RegionEngineConfig`] values
/// (`[{"mito": {...}}, {"file": {...}}]`). That is how both the config-file
/// source and the `Self::default()` source represent it.
///
/// The environment-variable source is different: because env vars are parsed
/// as a dunder(`__`)-separated path, `REGION_ENGINE__MITO__GLOBAL_WRITE_BUFFER_REJECT_SIZE`
/// builds a *map* keyed by engine name (`{"mito": {"global_write_buffer_reject_size": ...}}`),
/// not a sequence. Deserializing that map straight into `Vec<RegionEngineConfig>`
/// fails with `invalid type: map, expected a sequence` (see
/// <https://github.com/GreptimeTeam/greptimedb/issues/8620>).
///
/// To reconcile the two shapes, an object is treated as a set of *partial*
/// per-engine overrides rather than a full replacement of the list: each key
/// must be a known engine name (`mito`, `file` or `metric`, matching the
/// `#[serde(rename = "...")]` tags on [`RegionEngineConfig`]) and its value is
/// merged onto that engine's own defaults (each engine config type either has
/// `#[serde(default)]` on the struct, or `default = ...` on every field, so a
/// partial object deserializes with the rest of the fields left at their
/// defaults). Engines that are not mentioned in the object keep whatever
/// value they had in `DatanodeOptions::default()`'s `region_engine` list, so
/// overriding e.g. just `mito.global_write_buffer_reject_size` through the
/// environment does not drop the `file` (or, once configured, `metric`)
/// engine entries.
///
/// This helper is shared by [`DatanodeOptions`] and `standalone::options::StandaloneOptions`,
/// which both use the same [`RegionEngineConfig`] type for this field.
pub fn deserialize_region_engine_options<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<RegionEngineConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Array(_) => serde_json::from_value(value).map_err(D::Error::custom),
        serde_json::Value::Object(overrides) => {
            let mut engines = DatanodeOptions::default().region_engine;
            for (engine_name, override_value) in overrides {
                let mut single = serde_json::Map::with_capacity(1);
                single.insert(engine_name.clone(), override_value);
                let parsed: RegionEngineConfig =
                    serde_json::from_value(serde_json::Value::Object(single)).map_err(|e| {
                        D::Error::custom(format!(
                            "invalid options for region engine `{engine_name}`: {e}"
                        ))
                    })?;

                if let Some(existing) = engines.iter_mut().find(|engine| {
                    std::mem::discriminant(*engine) == std::mem::discriminant(&parsed)
                }) {
                    *existing = parsed;
                } else {
                    engines.push(parsed);
                }
            }
            Ok(engines)
        }
        other => Err(D::Error::custom(format!(
            "invalid type for `region_engine`: expected an array or an object, found {}",
            json_value_type_name(&other)
        ))),
    }
}

fn json_value_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
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

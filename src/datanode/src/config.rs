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

use core::time::Duration;

use common_base::readable_size::ReadableSize;
use common_base::secrets::{ExposeSecret, SecretString};
use common_config::Configurable;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_wal::config::DatanodeWalConfig;
use common_workload::{sanitize_workload_types, DatanodeWorkloadType};
use file_engine::config::EngineConfig as FileEngineConfig;
use meta_client::MetaClientOptions;
use metric_engine::config::EngineConfig as MetricEngineConfig;
use mito2::config::MitoConfig;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;

pub const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize::gb(5);

/// Default data home in file storage
const DEFAULT_DATA_HOME: &str = "./greptimedb_data";

/// Object storage config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File(FileConfig),
    S3(S3Config),
    Oss(OssConfig),
    Azblob(AzblobConfig),
    Gcs(GcsConfig),
}

impl ObjectStoreConfig {
    /// Returns the object storage type name, such as `S3`, `Oss` etc.
    pub fn provider_name(&self) -> &'static str {
        match self {
            Self::File(_) => "File",
            Self::S3(_) => "S3",
            Self::Oss(_) => "Oss",
            Self::Azblob(_) => "Azblob",
            Self::Gcs(_) => "Gcs",
        }
    }

    /// Returns true when it's a remote object storage such as AWS s3 etc.
    pub fn is_object_storage(&self) -> bool {
        !matches!(self, Self::File(_))
    }

    /// Returns the object storage configuration name, return the provider name if it's empty.
    pub fn config_name(&self) -> &str {
        let name = match self {
            // file storage doesn't support name
            Self::File(_) => self.provider_name(),
            Self::S3(s3) => &s3.name,
            Self::Oss(oss) => &oss.name,
            Self::Azblob(az) => &az.name,
            Self::Gcs(gcs) => &gcs.name,
        };

        if name.trim().is_empty() {
            return self.provider_name();
        }

        name
    }
}

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

#[derive(Debug, Clone, Serialize, Default, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct FileConfig {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct ObjectStorageCacheConfig {
    /// The local file cache directory
    pub cache_path: Option<String>,
    /// The cache capacity in bytes
    pub cache_capacity: Option<ReadableSize>,
}

/// The http client options to the storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct HttpClientConfig {
    /// The maximum idle connection per host allowed in the pool.
    pub(crate) pool_max_idle_per_host: u32,

    /// The timeout for only the connect phase of a http client.
    #[serde(with = "humantime_serde")]
    pub(crate) connect_timeout: Duration,

    /// The total request timeout, applied from when the request starts connecting until the response body has finished.
    /// Also considered a total deadline.
    #[serde(with = "humantime_serde")]
    pub(crate) timeout: Duration,

    /// The timeout for idle sockets being kept-alive.
    #[serde(with = "humantime_serde")]
    pub(crate) pool_idle_timeout: Duration,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: 1024,
            connect_timeout: Duration::from_secs(30),
            timeout: Duration::from_secs(30),
            pool_idle_timeout: Duration::from_secs(90),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub name: String,
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub secret_access_key: SecretString,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Enable virtual host style so that opendal will send API requests in virtual host style instead of path style.
    /// By default, opendal will send API to https://s3.us-east-1.amazonaws.com/bucket_name
    /// Enabled, opendal will send API to https://bucket_name.s3.us-east-1.amazonaws.com
    pub enable_virtual_host_style: bool,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for S3Config {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.secret_access_key.expose_secret() == other.secret_access_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.region == other.region
            && self.enable_virtual_host_style == other.enable_virtual_host_style
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub name: String,
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for OssConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.access_key_secret.expose_secret() == other.access_key_secret.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AzblobConfig {
    pub name: String,
    pub container: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub account_name: SecretString,
    #[serde(skip_serializing)]
    pub account_key: SecretString,
    pub endpoint: String,
    pub sas_token: Option<String>,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for AzblobConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.container == other.container
            && self.root == other.root
            && self.account_name.expose_secret() == other.account_name.expose_secret()
            && self.account_key.expose_secret() == other.account_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.sas_token == other.sas_token
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsConfig {
    pub name: String,
    pub root: String,
    pub bucket: String,
    pub scope: String,
    #[serde(skip_serializing)]
    pub credential_path: SecretString,
    #[serde(skip_serializing)]
    pub credential: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for GcsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.root == other.root
            && self.bucket == other.bucket
            && self.scope == other.scope
            && self.credential_path.expose_secret() == other.credential_path.expose_secret()
            && self.credential.expose_secret() == other.credential.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            name: String::default(),
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            secret_access_key: SecretString::from(String::default()),
            enable_virtual_host_style: false,
            endpoint: Option::default(),
            region: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            access_key_secret: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl Default for AzblobConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            container: String::default(),
            root: String::default(),
            account_name: SecretString::from(String::default()),
            account_key: SecretString::from(String::default()),
            endpoint: String::default(),
            sas_token: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            root: String::default(),
            bucket: String::default(),
            scope: String::default(),
            credential_path: SecretString::from(String::default()),
            credential: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileConfig {})
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
    fn test_config_name() {
        let object_store_config = ObjectStoreConfig::default();
        assert_eq!("File", object_store_config.config_name());

        let s3_config = ObjectStoreConfig::S3(S3Config::default());
        assert_eq!("S3", s3_config.config_name());
        assert_eq!("S3", s3_config.provider_name());

        let s3_config = ObjectStoreConfig::S3(S3Config {
            name: "test".to_string(),
            ..Default::default()
        });
        assert_eq!("test", s3_config.config_name());
        assert_eq!("S3", s3_config.provider_name());
    }

    #[test]
    fn test_is_object_storage() {
        let store = ObjectStoreConfig::default();
        assert!(!store.is_object_storage());
        let s3_config = ObjectStoreConfig::S3(S3Config::default());
        assert!(s3_config.is_object_storage());
        let oss_config = ObjectStoreConfig::Oss(OssConfig::default());
        assert!(oss_config.is_object_storage());
        let gcs_config = ObjectStoreConfig::Gcs(GcsConfig::default());
        assert!(gcs_config.is_object_storage());
        let azblob_config = ObjectStoreConfig::Azblob(AzblobConfig::default());
        assert!(azblob_config.is_object_storage());
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
                    format!("{:?}", cfg.access_key_id)
                );
                assert_eq!("access_key_id", cfg.access_key_id.expose_secret());
            }
            _ => unreachable!(),
        }
    }
}

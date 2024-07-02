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
use common_base::secrets::{ExposeSecret, SecretString};
use common_config::Configurable;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_wal::config::DatanodeWalConfig;
use file_engine::config::EngineConfig as FileEngineConfig;
use meta_client::MetaClientOptions;
use mito2::config::MitoConfig;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use servers::Mode;

pub const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize::mb(256);

/// Default data home in file storage
const DEFAULT_DATA_HOME: &str = "/tmp/greptimedb";

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
    pub fn name(&self) -> &'static str {
        match self {
            Self::File(_) => "File",
            Self::S3(_) => "S3",
            Self::Oss(_) => "Oss",
            Self::Azblob(_) => "Azblob",
            Self::Gcs(_) => "Gcs",
        }
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
    pub providers: Vec<ObjectStoreConfig>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub secret_access_key: SecretString,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
}

impl PartialEq for S3Config {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.secret_access_key.expose_secret() == other.secret_access_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.region == other.region
            && self.cache == other.cache
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
}

impl PartialEq for OssConfig {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.access_key_secret.expose_secret() == other.access_key_secret.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AzblobConfig {
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
}

impl PartialEq for AzblobConfig {
    fn eq(&self, other: &Self) -> bool {
        self.container == other.container
            && self.root == other.root
            && self.account_name.expose_secret() == other.account_name.expose_secret()
            && self.account_key.expose_secret() == other.account_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.sas_token == other.sas_token
            && self.cache == other.cache
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsConfig {
    pub root: String,
    pub bucket: String,
    pub scope: String,
    #[serde(skip_serializing)]
    pub credential_path: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
}

impl PartialEq for GcsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
            && self.bucket == other.bucket
            && self.scope == other.scope
            && self.credential_path.expose_secret() == other.credential_path.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            secret_access_key: SecretString::from(String::default()),
            endpoint: Option::default(),
            region: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
        }
    }
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            access_key_secret: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
        }
    }
}

impl Default for AzblobConfig {
    fn default() -> Self {
        Self {
            container: String::default(),
            root: String::default(),
            account_name: SecretString::from(String::default()),
            account_key: SecretString::from(String::default()),
            endpoint: String::default(),
            sas_token: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
        }
    }
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            root: String::default(),
            bucket: String::default(),
            scope: String::default(),
            credential_path: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
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
    pub mode: Mode,
    pub node_id: Option<u64>,
    pub require_lease_before_startup: bool,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub grpc: GrpcOptions,
    pub heartbeat: HeartbeatOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub logging: LoggingOptions,
    pub enable_telemetry: bool,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,

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

impl Default for DatanodeOptions {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            node_id: None,
            require_lease_before_startup: false,
            init_regions_in_background: false,
            init_regions_parallelism: 16,
            grpc: GrpcOptions::default().with_addr("127.0.0.1:3001"),
            http: HttpOptions::default(),
            meta_client: None,
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            logging: LoggingOptions::default(),
            heartbeat: HeartbeatOptions::datanode_default(),
            enable_telemetry: true,
            export_metrics: ExportMetricsOption::default(),
            tracing: TracingOptions::default(),

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
                    format!("{:?}", cfg.access_key_id)
                );
                assert_eq!("access_key_id", cfg.access_key_id.expose_secret());
            }
            _ => unreachable!(),
        }
    }
}

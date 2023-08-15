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

use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_error::ext::BoxedError;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::info;
use common_telemetry::logging::LoggingOptions;
use meta_client::MetaClientOptions;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use servers::Mode;
use snafu::ResultExt;
use storage::config::{
    EngineConfig as StorageEngineConfig, DEFAULT_AUTO_FLUSH_INTERVAL, DEFAULT_MAX_FLUSH_TASKS,
    DEFAULT_PICKER_SCHEDULE_INTERVAL, DEFAULT_REGION_WRITE_BUFFER_SIZE,
};
use storage::scheduler::SchedulerConfig;

use crate::error::{Result, ShutdownInstanceSnafu};
use crate::heartbeat::HeartbeatTask;
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

pub const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize(1024);

/// Default data home in file storage
const DEFAULT_DATA_HOME: &str = "/tmp/greptimedb";

/// Object storage config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File(FileConfig),
    S3(S3Config),
    Oss(OssConfig),
    Azblob(AzblobConfig),
    Gcs(GcsConfig),
}

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Retention period for all tables.
    ///
    /// Default value is `None`, which means no TTL.
    ///
    /// The precedence order is: ttl in table options > global ttl.
    #[serde(with = "humantime_serde")]
    pub global_ttl: Option<Duration>,
    /// The working directory of database
    pub data_home: String,
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
    pub compaction: CompactionConfig,
    pub manifest: RegionManifestConfig,
    pub flush: FlushConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            global_ttl: None,
            data_home: DEFAULT_DATA_HOME.to_string(),
            store: ObjectStoreConfig::default(),
            compaction: CompactionConfig::default(),
            manifest: RegionManifestConfig::default(),
            flush: FlushConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct FileConfig {}

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
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
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
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
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
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
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
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
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
            cache_path: Option::default(),
            cache_capacity: Option::default(),
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
            cache_path: Option::default(),
            cache_capacity: Option::default(),
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
            cache_path: Option::default(),
            cache_capacity: Option::default(),
            sas_token: Option::default(),
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
            cache_path: Option::default(),
            cache_capacity: Option::default(),
        }
    }
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileConfig {})
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    // wal directory
    pub dir: Option<String>,
    // wal file size in bytes
    pub file_size: ReadableSize,
    // wal purge threshold in bytes
    pub purge_threshold: ReadableSize,
    // purge interval in seconds
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,
    // read batch size
    pub read_batch_size: usize,
    // whether to sync log file after every write
    pub sync_write: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: None,
            file_size: ReadableSize::mb(256), // log file size 256MB
            purge_threshold: ReadableSize::gb(4), // purge threshold 4GB
            purge_interval: Duration::from_secs(600),
            read_batch_size: 128,
            sync_write: false,
        }
    }
}

/// Options for region manifest
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct RegionManifestConfig {
    /// Region manifest checkpoint actions margin.
    /// Manifest service create a checkpoint every [checkpoint_margin] actions.
    pub checkpoint_margin: Option<u16>,
    /// Region manifest logs and checkpoints gc task execution duration.
    #[serde(with = "humantime_serde")]
    pub gc_duration: Option<Duration>,
    /// Whether to try creating a manifest checkpoint on region opening
    pub checkpoint_on_startup: bool,
    /// Whether to compress manifest and checkpoint file by gzip
    pub compress: bool,
}

impl Default for RegionManifestConfig {
    fn default() -> Self {
        Self {
            checkpoint_margin: Some(10u16),
            gc_duration: Some(Duration::from_secs(600)),
            checkpoint_on_startup: false,
            compress: false,
        }
    }
}

/// Options for table compaction
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct CompactionConfig {
    /// Max task number that can concurrently run.
    pub max_inflight_tasks: usize,
    /// Max files in level 0 to trigger compaction.
    pub max_files_in_level0: usize,
    /// Max task number for SST purge task after compaction.
    pub max_purge_tasks: usize,
    /// Buffer threshold while writing SST files
    pub sst_write_buffer_size: ReadableSize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_inflight_tasks: 4,
            max_files_in_level0: 8,
            max_purge_tasks: 32,
            sst_write_buffer_size: ReadableSize::mb(8),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct FlushConfig {
    /// Max inflight flush tasks.
    pub max_flush_tasks: usize,
    /// Default write buffer size for a region.
    pub region_write_buffer_size: ReadableSize,
    /// Interval to schedule auto flush picker to find region to flush.
    #[serde(with = "humantime_serde")]
    pub picker_schedule_interval: Duration,
    /// Interval to auto flush a region if it has not flushed yet.
    #[serde(with = "humantime_serde")]
    pub auto_flush_interval: Duration,
    /// Global write buffer size for all regions.
    pub global_write_buffer_size: Option<ReadableSize>,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            max_flush_tasks: DEFAULT_MAX_FLUSH_TASKS,
            region_write_buffer_size: DEFAULT_REGION_WRITE_BUFFER_SIZE,
            picker_schedule_interval: Duration::from_millis(
                DEFAULT_PICKER_SCHEDULE_INTERVAL.into(),
            ),
            auto_flush_interval: Duration::from_millis(DEFAULT_AUTO_FLUSH_INTERVAL.into()),
            global_write_buffer_size: None,
        }
    }
}

impl From<&DatanodeOptions> for SchedulerConfig {
    fn from(value: &DatanodeOptions) -> Self {
        Self {
            max_inflight_tasks: value.storage.compaction.max_inflight_tasks,
        }
    }
}

impl From<&DatanodeOptions> for StorageEngineConfig {
    fn from(value: &DatanodeOptions) -> Self {
        Self {
            compress_manifest: value.storage.manifest.compress,
            manifest_checkpoint_on_startup: value.storage.manifest.checkpoint_on_startup,
            manifest_checkpoint_margin: value.storage.manifest.checkpoint_margin,
            manifest_gc_duration: value.storage.manifest.gc_duration,
            max_files_in_l0: value.storage.compaction.max_files_in_level0,
            max_purge_tasks: value.storage.compaction.max_purge_tasks,
            sst_write_buffer_size: value.storage.compaction.sst_write_buffer_size,
            max_flush_tasks: value.storage.flush.max_flush_tasks,
            region_write_buffer_size: value.storage.flush.region_write_buffer_size,
            picker_schedule_interval: value.storage.flush.picker_schedule_interval,
            auto_flush_interval: value.storage.flush.auto_flush_interval,
            global_write_buffer_size: value.storage.flush.global_write_buffer_size,
            global_ttl: value.storage.global_ttl,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DatanodeOptions {
    pub mode: Mode,
    pub enable_memory_catalog: bool,
    pub node_id: Option<u64>,
    pub rpc_addr: String,
    pub rpc_hostname: Option<String>,
    pub rpc_runtime_size: usize,
    pub heartbeat: HeartbeatOptions,
    pub http_opts: HttpOptions,
    pub meta_client_options: Option<MetaClientOptions>,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
    pub enable_telemetry: bool,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_memory_catalog: false,
            node_id: None,
            rpc_addr: "127.0.0.1:3001".to_string(),
            rpc_hostname: None,
            rpc_runtime_size: 8,
            http_opts: HttpOptions::default(),
            meta_client_options: None,
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
            heartbeat: HeartbeatOptions::default(),
            enable_telemetry: true,
        }
    }
}

impl DatanodeOptions {
    pub fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client_options.metasrv_addrs"])
    }

    pub fn to_toml_string(&self) -> String {
        toml::to_string(&self).unwrap()
    }
}

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Option<Services>,
    instance: InstanceRef,
    heartbeat_task: Option<HeartbeatTask>,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions, plugins: Arc<Plugins>) -> Result<Datanode> {
        let (instance, heartbeat_task) = Instance::with_opts(&opts, plugins).await?;
        let services = match opts.mode {
            Mode::Distributed => Some(Services::try_new(instance.clone(), &opts).await?),
            Mode::Standalone => None,
        };
        Ok(Self {
            opts,
            services,
            instance,
            heartbeat_task,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");
        self.start_instance().await?;
        self.start_services().await
    }

    /// Start only the internal component of datanode.
    pub async fn start_instance(&mut self) -> Result<()> {
        let _ = self.instance.start().await;
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }
        Ok(())
    }

    /// Start services of datanode. This method call will block until services are shutdown.
    pub async fn start_services(&mut self) -> Result<()> {
        if let Some(service) = self.services.as_mut() {
            service.start(&self.opts).await
        } else {
            Ok(())
        }
    }

    pub fn get_instance(&self) -> InstanceRef {
        self.instance.clone()
    }

    pub async fn shutdown_instance(&self) -> Result<()> {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task
                .close()
                .await
                .map_err(BoxedError::new)
                .context(ShutdownInstanceSnafu)?;
        }
        let _ = self.instance.shutdown().await;
        Ok(())
    }

    async fn shutdown_services(&self) -> Result<()> {
        if let Some(service) = self.services.as_ref() {
            service.shutdown().await
        } else {
            Ok(())
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // We must shutdown services first
        self.shutdown_services().await?;
        self.shutdown_instance().await
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

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
        match opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert_eq!(
                    "Secret([REDACTED alloc::string::String])".to_string(),
                    format!("{:?}", cfg.access_key_id)
                );
                assert_eq!("access_key_id", cfg.access_key_id.expose_secret());
            }
            _ => unreachable!(),
        }
    }
}

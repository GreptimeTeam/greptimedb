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
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use common_telemetry::logging::LoggingOptions;
use meta_client::MetaClientOptions;
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::Mode;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::scheduler::SchedulerConfig;

use crate::error::Result;
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

pub const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize(1024);

/// Object storage config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File(FileConfig),
    S3(S3Config),
    Oss(OssConfig),
}

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StorageConfig {
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
    pub compaction: CompactionConfig,
    pub manifest: RegionManifestConfig,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct FileConfig {
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub bucket: String,
    pub root: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub bucket: String,
    pub root: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub endpoint: String,
    pub cache_path: Option<String>,
    pub cache_capacity: Option<ReadableSize>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileConfig {
            data_dir: "/tmp/greptimedb/data/".to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    // wal directory
    pub dir: String,
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
            dir: "/tmp/greptimedb/wal".to_string(),
            file_size: ReadableSize::gb(1),        // log file size 1G
            purge_threshold: ReadableSize::gb(50), // purge threshold 50G
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
}

impl Default for RegionManifestConfig {
    fn default() -> Self {
        Self {
            checkpoint_margin: Some(10u16),
            gc_duration: Some(Duration::from_secs(30)),
            checkpoint_on_startup: false,
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
            manifest_checkpoint_on_startup: value.storage.manifest.checkpoint_on_startup,
            manifest_checkpoint_margin: value.storage.manifest.checkpoint_margin,
            manifest_gc_duration: value.storage.manifest.gc_duration,
            max_files_in_l0: value.storage.compaction.max_files_in_level0,
            max_purge_tasks: value.storage.compaction.max_purge_tasks,
            sst_write_buffer_size: value.storage.compaction.sst_write_buffer_size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcedureConfig {
    /// Max retry times of procedure.
    pub max_retry_times: usize,
    /// Initial retry delay of procedures, increases exponentially.
    #[serde(with = "humantime_serde")]
    pub retry_delay: Duration,
    /// Storage config for procedure manager.
    pub store: ObjectStoreConfig,
}

impl Default for ProcedureConfig {
    fn default() -> ProcedureConfig {
        ProcedureConfig {
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            store: ObjectStoreConfig::File(FileConfig {
                data_dir: "/tmp/greptimedb/procedure/".to_string(),
            }),
        }
    }
}

impl ProcedureConfig {
    pub fn from_file_path(path: String) -> ProcedureConfig {
        ProcedureConfig {
            store: ObjectStoreConfig::File(FileConfig { data_dir: path }),
            ..Default::default()
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
    pub mysql_addr: String,
    pub mysql_runtime_size: usize,
    pub http_opts: HttpOptions,
    pub meta_client_options: Option<MetaClientOptions>,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
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
            mysql_addr: "127.0.0.1:4406".to_string(),
            mysql_runtime_size: 2,
            http_opts: HttpOptions::default(),
            meta_client_options: None,
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
        }
    }
}

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Option<Services>,
    instance: InstanceRef,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions) -> Result<Datanode> {
        let instance = Arc::new(Instance::new(&opts).await?);
        let services = match opts.mode {
            Mode::Distributed => Some(Services::try_new(instance.clone(), &opts).await?),
            Mode::Standalone => None,
        };
        Ok(Self {
            opts,
            services,
            instance,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");
        self.start_instance().await?;
        self.start_services().await
    }

    /// Start only the internal component of datanode.
    pub async fn start_instance(&mut self) -> Result<()> {
        self.instance.start().await
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
        self.instance.shutdown().await
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
    use super::*;

    #[test]
    fn test_toml() {
        let opts = DatanodeOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: DatanodeOptions = toml::from_str(&toml_string).unwrap();
    }
}

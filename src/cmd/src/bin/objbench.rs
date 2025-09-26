// Copyright 2025 Greptime Team
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

use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use cmd::error::{self, Result};
use colored::Colorize;
use datanode::config::ObjectStoreConfig;
use mito2::config::MitoConfig;
use mito2::read::Source;
use mito2::sst::file::{FileHandle, FileId, FileMeta};
use mito2::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use mito2::sst::parquet::{WriteOptions, PARQUET_METADATA_KEY};
use mito2::{build_access_layer, Metrics, OperationType, SstWriteRequest};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};

#[tokio::main]
pub async fn main() {
    // common_telemetry::init_default_ut_logging();
    let cmd = Command::parse();
    if let Err(e) = cmd.run().await {
        eprintln!("{}: {}", "Error".red().bold(), e);
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct StorageConfigWrapper {
    storage: StorageConfig,
}

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct StorageConfig {
    /// The working directory of database
    pub data_home: String,
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
}

#[derive(Debug, Parser)]
pub struct Command {
    /// Path to the object-store config file (TOML). Must deserialize into datanode::config::ObjectStoreConfig.
    #[clap(long, value_name = "FILE")]
    pub config: PathBuf,

    /// Source SST file path in object-store (e.g. "region_dir/<uuid>.parquet").
    #[clap(long, value_name = "PATH")]
    pub source: String,

    /// Target SST file path in object-store; its parent directory is used as destination region dir.
    #[clap(long, value_name = "PATH")]
    pub target: String,

    /// Verbose output
    #[clap(short, long, default_value_t = false)]
    pub verbose: bool,

    /// Output file path for pprof flamegraph (enables profiling)
    #[clap(long, value_name = "FILE")]
    pub pprof_file: Option<PathBuf>,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting objbench...".cyan().bold());

        // Build object store from config
        let cfg_str = std::fs::read_to_string(&self.config).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("failed to read config {}: {e}", self.config.display()),
            }
            .build()
        })?;
        let store_cfg: StorageConfigWrapper = toml::from_str(&cfg_str).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("failed to parse config {}: {e}", self.config.display()),
            }
            .build()
        })?;

        let object_store = build_object_store(&store_cfg.storage).await?;
        println!("{} Object store initialized", "✓".green());

        // Prepare source identifiers
        let (src_region_dir, src_file_id) = split_sst_path(&self.source)?;
        println!("{} Source path parsed: {}", "✓".green(), self.source);

        // Load parquet metadata to extract RegionMetadata and file stats
        println!("{}", "Loading parquet metadata...".yellow());
        let file_size = object_store
            .stat(&self.source)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("stat failed: {e}"),
                }
                .build()
            })?
            .content_length();
        let parquet_meta = load_parquet_metadata(object_store.clone(), &self.source, file_size)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("read parquet metadata failed: {e}"),
                }
                .build()
            })?;

        let region_meta = extract_region_metadata(&self.source, &parquet_meta)?;
        let num_rows = parquet_meta.file_metadata().num_rows() as u64;
        let num_row_groups = parquet_meta.num_row_groups() as u64;

        println!(
            "{} Metadata loaded - rows: {}, size: {} bytes",
            "✓".green(),
            num_rows,
            file_size
        );

        // Build a FileHandle for the source file
        let file_meta = FileMeta {
            region_id: region_meta.region_id,
            file_id: src_file_id,
            time_range: Default::default(),
            level: 0,
            file_size,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows,
            num_row_groups,
            sequence: None,
        };
        let src_handle = FileHandle::new(file_meta, new_noop_file_purger());

        // Build the reader for a single file via ParquetReaderBuilder
        println!("{}", "Building reader...".yellow());
        let (_src_access_layer, _cache_manager) =
            build_access_layer_simple(src_region_dir.clone(), object_store.clone()).await?;
        let reader_build_start = Instant::now();
        let reader = mito2::sst::parquet::reader::ParquetReaderBuilder::new(
            src_region_dir.clone(),
            src_handle.clone(),
            object_store.clone(),
        )
        .expected_metadata(Some(region_meta.clone()))
        .build()
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("build reader failed: {e}"),
            }
            .build()
        })?;

        let reader_build_elapsed = reader_build_start.elapsed();
        let total_rows = reader.parquet_metadata().file_metadata().num_rows();
        println!("{} Reader built in {:?}", "✓".green(), reader_build_elapsed);

        // Prepare target access layer for writing
        println!("{}", "Preparing target access layer...".yellow());
        let (tgt_access_layer, tgt_cache_manager) =
            build_access_layer_simple(self.target.clone(), object_store.clone()).await?;

        // Build write request
        let write_opts = WriteOptions::default();
        let write_req = SstWriteRequest {
            op_type: OperationType::Compact,
            metadata: region_meta,
            source: Source::Reader(Box::new(reader)),
            cache_manager: tgt_cache_manager,
            storage: None,
            max_sequence: None,
            index_options: Default::default(),
            inverted_index_config: MitoConfig::default().inverted_index,
            fulltext_index_config: MitoConfig::default().fulltext_index,
            bloom_filter_index_config: MitoConfig::default().bloom_filter_index,
        };

        // Write SST
        println!("{}", "Writing SST...".yellow());
        let mut metrics = Metrics::default();

        // Start profiling if pprof_file is specified
        #[cfg(unix)]
        let profiler_guard = if self.pprof_file.is_some() {
            println!("{} Starting profiling...", "⚡".yellow());
            Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(99)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()
                    .map_err(|e| {
                        error::IllegalConfigSnafu {
                            msg: format!("Failed to start profiler: {e}"),
                        }
                        .build()
                    })?,
            )
        } else {
            None
        };

        #[cfg(not(unix))]
        if self.pprof_file.is_some() {
            eprintln!(
                "{}: Profiling is not supported on this platform",
                "Warning".yellow()
            );
        }

        let write_start = Instant::now();
        let infos = tgt_access_layer
            .write_sst(write_req, &write_opts, &mut metrics)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("write_sst failed: {e}"),
                }
                .build()
            })?;

        let write_elapsed = write_start.elapsed();

        // Stop profiling and generate flamegraph if enabled
        #[cfg(unix)]
        if let (Some(guard), Some(pprof_file)) = (profiler_guard, &self.pprof_file) {
            println!("{} Generating flamegraph...", "🔥".yellow());
            match guard.report().build() {
                Ok(report) => {
                    let mut flamegraph_data = Vec::new();
                    if let Err(e) = report.flamegraph(&mut flamegraph_data) {
                        eprintln!(
                            "{}: Failed to generate flamegraph: {}",
                            "Warning".yellow(),
                            e
                        );
                    } else if let Err(e) = std::fs::write(pprof_file, flamegraph_data) {
                        eprintln!(
                            "{}: Failed to write flamegraph to {}: {}",
                            "Warning".yellow(),
                            pprof_file.display(),
                            e
                        );
                    } else {
                        println!(
                            "{} Flamegraph saved to {}",
                            "✓".green(),
                            pprof_file.display().to_string().cyan()
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "{}: Failed to generate pprof report: {}",
                        "Warning".yellow(),
                        e
                    );
                }
            }
        }
        assert_eq!(infos.len(), 1);
        let dst_file_id = infos[0].file_id;
        let dst_file_path = format!("{}{}", self.target, dst_file_id.as_parquet(),);

        // Report results with ANSI colors
        println!("\n{} {}", "Write complete!".green().bold(), "✓".green());
        println!("  {}: {}", "Destination file".bold(), dst_file_path.cyan());
        println!("  {}: {}", "Rows".bold(), total_rows.to_string().cyan());
        println!(
            "  {}: {}",
            "File size".bold(),
            format!("{} bytes", file_size).cyan()
        );
        println!(
            "  {}: {:?}",
            "Reader build time".bold(),
            reader_build_elapsed
        );
        println!("  {}: {:?}", "Total time".bold(), write_elapsed);

        // Print metrics in a formatted way
        println!(
            "  {}: {:?}, sum: {:?}",
            "Metrics".bold(),
            metrics,
            metrics.sum()
        );

        // Cleanup
        println!("\n{}", "Cleaning up...".yellow());
        object_store.delete(&dst_file_path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to delete dest file {}: {}", dst_file_path, e),
            }
            .build()
        })?;
        println!("{} Temporary file deleted", "✓".green());

        println!("\n{}", "Benchmark completed successfully!".green().bold());
        Ok(())
    }
}

fn split_sst_path(path: &str) -> Result<(String, FileId)> {
    let p = Path::new(path);
    let file_name = p.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
        error::IllegalConfigSnafu {
            msg: "invalid source path".to_string(),
        }
        .build()
    })?;
    let uuid_str = file_name.strip_suffix(".parquet").ok_or_else(|| {
        error::IllegalConfigSnafu {
            msg: "expect .parquet file".to_string(),
        }
        .build()
    })?;
    let file_id = FileId::parse_str(uuid_str).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("invalid file id: {e}"),
        }
        .build()
    })?;
    let parent = p
        .parent()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    Ok((parent, file_id))
}

fn extract_region_metadata(
    file_path: &str,
    meta: &parquet::file::metadata::ParquetMetaData,
) -> Result<RegionMetadataRef> {
    use parquet::format::KeyValue;
    let kvs: Option<&Vec<KeyValue>> = meta.file_metadata().key_value_metadata();
    let Some(kvs) = kvs else {
        return Err(error::IllegalConfigSnafu {
            msg: format!("{file_path}: missing parquet key_value metadata"),
        }
        .build());
    };
    let json = kvs
        .iter()
        .find(|kv| kv.key == PARQUET_METADATA_KEY)
        .and_then(|kv| kv.value.as_ref())
        .ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: format!("{file_path}: key {PARQUET_METADATA_KEY} not found or empty"),
            }
            .build()
        })?;
    let region: RegionMetadata = RegionMetadata::from_json(json).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("invalid region metadata json: {e}"),
        }
        .build()
    })?;
    Ok(std::sync::Arc::new(region))
}

async fn build_object_store(sc: &StorageConfig) -> Result<ObjectStore> {
    use datanode::config::ObjectStoreConfig::*;
    let oss = &sc.store;
    match oss {
        File(_) => {
            use object_store::services::Fs;
            let builder = Fs::default().root(&sc.data_home);
            Ok(ObjectStore::new(builder)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("init fs backend failed: {e}"),
                    }
                    .build()
                })?
                .finish())
        }
        S3(s3) => {
            use common_base::secrets::ExposeSecret;
            use object_store::services::S3;
            use object_store::util;
            let root = util::normalize_dir(&s3.root);
            let mut builder = S3::default()
                .root(&root)
                .bucket(&s3.bucket)
                .access_key_id(s3.access_key_id.expose_secret())
                .secret_access_key(s3.secret_access_key.expose_secret());
            if let Some(ep) = &s3.endpoint {
                builder = builder.endpoint(ep);
            }
            if let Some(region) = &s3.region {
                builder = builder.region(region);
            }
            if s3.enable_virtual_host_style {
                builder = builder.enable_virtual_host_style();
            }
            Ok(ObjectStore::new(builder)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("init s3 backend failed: {e}"),
                    }
                    .build()
                })?
                .finish())
        }
        Oss(oss) => {
            use common_base::secrets::ExposeSecret;
            use object_store::services::Oss;
            use object_store::util;
            let root = util::normalize_dir(&oss.root);
            let builder = Oss::default()
                .root(&root)
                .bucket(&oss.bucket)
                .endpoint(&oss.endpoint)
                .access_key_id(oss.access_key_id.expose_secret())
                .access_key_secret(oss.access_key_secret.expose_secret());
            Ok(ObjectStore::new(builder)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("init oss backend failed: {e}"),
                    }
                    .build()
                })?
                .finish())
        }
        Azblob(az) => {
            use common_base::secrets::ExposeSecret;
            use object_store::services::Azblob;
            use object_store::util;
            let root = util::normalize_dir(&az.root);
            let mut builder = Azblob::default()
                .root(&root)
                .container(&az.container)
                .endpoint(&az.endpoint)
                .account_name(az.account_name.expose_secret())
                .account_key(az.account_key.expose_secret());
            if let Some(token) = &az.sas_token {
                builder = builder.sas_token(token);
            }
            Ok(ObjectStore::new(builder)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("init azblob backend failed: {e}"),
                    }
                    .build()
                })?
                .finish())
        }
        Gcs(gcs) => {
            use common_base::secrets::ExposeSecret;
            use object_store::services::Gcs;
            use object_store::util;
            let root = util::normalize_dir(&gcs.root);
            let builder = Gcs::default()
                .root(&root)
                .bucket(&gcs.bucket)
                .scope(&gcs.scope)
                .credential_path(gcs.credential_path.expose_secret())
                .credential(gcs.credential.expose_secret())
                .endpoint(&gcs.endpoint);
            Ok(ObjectStore::new(builder)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("init gcs backend failed: {e}"),
                    }
                    .build()
                })?
                .finish())
        }
    }
}

async fn build_access_layer_simple(
    region_dir: String,
    object_store: ObjectStore,
) -> Result<(
    std::sync::Arc<mito2::AccessLayer>,
    std::sync::Arc<mito2::CacheManager>,
)> {
    // Minimal index aux path setup
    let mut mito_cfg = MitoConfig::default();
    // Use a temporary directory as aux path
    let data_home = std::env::temp_dir().join("greptime_objbench");
    let _ = std::fs::create_dir_all(&data_home);
    let _ = mito_cfg.index.sanitize(
        data_home.to_str().unwrap_or("/tmp"),
        &mito_cfg.inverted_index,
    );
    let access_layer = build_access_layer(&region_dir, object_store, &mito_cfg)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("build_access_layer failed: {e}"),
            }
            .build()
        })?;
    Ok((
        access_layer,
        std::sync::Arc::new(mito2::CacheManager::default()),
    ))
}

fn new_noop_file_purger() -> FilePurgerRef {
    #[derive(Debug)]
    struct Noop;
    impl FilePurger for Noop {
        fn send_request(&self, _request: PurgeRequest) {}
    }
    std::sync::Arc::new(Noop)
}

async fn load_parquet_metadata(
    object_store: ObjectStore,
    path: &str,
    file_size: u64,
) -> std::result::Result<
    parquet::file::metadata::ParquetMetaData,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::FOOTER_SIZE;
    let actual_size = if file_size == 0 {
        object_store.stat(path).await?.content_length()
    } else {
        file_size
    };
    if actual_size < FOOTER_SIZE as u64 {
        return Err("file too small".into());
    }
    let prefetch: u64 = 64 * 1024;
    let start = actual_size.saturating_sub(prefetch);
    let buffer = object_store
        .read_with(path)
        .range(start..actual_size)
        .await?
        .to_vec();
    let buffer_len = buffer.len();
    let mut footer = [0; 8];
    footer.copy_from_slice(&buffer[buffer_len - FOOTER_SIZE..]);
    let metadata_len = ParquetMetaDataReader::decode_footer(&footer)? as u64;
    if actual_size - (FOOTER_SIZE as u64) < metadata_len {
        return Err("invalid footer/metadata length".into());
    }
    if (metadata_len as usize) <= buffer_len - FOOTER_SIZE {
        let metadata_start = buffer_len - metadata_len as usize - FOOTER_SIZE;
        let meta = ParquetMetaDataReader::decode_metadata(
            &buffer[metadata_start..buffer_len - FOOTER_SIZE],
        )?;
        Ok(meta)
    } else {
        let metadata_start = actual_size - metadata_len - FOOTER_SIZE as u64;
        let data = object_store
            .read_with(path)
            .range(metadata_start..(actual_size - FOOTER_SIZE as u64))
            .await?
            .to_vec();
        let meta = ParquetMetaDataReader::decode_metadata(&data)?;
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageConfigWrapper;

    #[test]
    fn test_decode() {
        let cfg = std::fs::read_to_string("/home/lei/datanode-bulk.toml").unwrap();
        let storage: StorageConfigWrapper = toml::from_str(&cfg).unwrap();
        println!("{:?}", storage);
    }
}

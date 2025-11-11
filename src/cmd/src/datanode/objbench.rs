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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use colored::Colorize;
use datanode::config::RegionEngineConfig;
use datanode::store;
use either::Either;
use mito2::access_layer::{
    AccessLayer, AccessLayerRef, Metrics, OperationType, SstWriteRequest, WriteType,
};
use mito2::cache::{CacheManager, CacheManagerRef};
use mito2::config::{FulltextIndexConfig, MitoConfig, Mode};
use mito2::read::Source;
use mito2::sst::file::{FileHandle, FileMeta};
use mito2::sst::file_purger::{FilePurger, FilePurgerRef};
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::index::puffin_manager::PuffinManagerFactory;
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use mito2::sst::parquet::{PARQUET_METADATA_KEY, WriteOptions};
use mito2::worker::write_cache_from_config;
use object_store::ObjectStore;
use regex::Regex;
use snafu::OptionExt;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::FileId;

use crate::datanode::{StorageConfig, StorageConfigWrapper};
use crate::error;

/// Object storage benchmark command
#[derive(Debug, Parser)]
pub struct ObjbenchCommand {
    /// Path to the object-store config file (TOML). Must deserialize into object_store::config::ObjectStoreConfig.
    #[clap(long, value_name = "FILE")]
    pub config: PathBuf,

    /// Source SST file path in object-store (e.g. "region_dir/<uuid>.parquet").
    #[clap(long, value_name = "PATH")]
    pub source: String,

    /// Verbose output
    #[clap(short, long, default_value_t = false)]
    pub verbose: bool,

    /// Output file path for pprof flamegraph (enables profiling)
    #[clap(long, value_name = "FILE")]
    pub pprof_file: Option<PathBuf>,
}

fn parse_config(config_path: &PathBuf) -> error::Result<(StorageConfig, MitoConfig)> {
    let cfg_str = std::fs::read_to_string(config_path).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("failed to read config {}: {e}", config_path.display()),
        }
        .build()
    })?;

    let store_cfg: StorageConfigWrapper = toml::from_str(&cfg_str).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("failed to parse config {}: {e}", config_path.display()),
        }
        .build()
    })?;

    let storage_config = store_cfg.storage;
    let mito_engine_config = store_cfg
        .region_engine
        .into_iter()
        .filter_map(|c| {
            if let RegionEngineConfig::Mito(mito) = c {
                Some(mito)
            } else {
                None
            }
        })
        .next()
        .with_context(|| error::IllegalConfigSnafu {
            msg: format!("Engine config not found in {:?}", config_path),
        })?;
    Ok((storage_config, mito_engine_config))
}

impl ObjbenchCommand {
    pub async fn run(&self) -> error::Result<()> {
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting objbench with config:".cyan().bold());

        // Build object store from config
        let (store_cfg, mut mito_engine_config) = parse_config(&self.config)?;

        let object_store = build_object_store(&store_cfg).await?;
        println!("{} Object store initialized", "✓".green());

        // Prepare source identifiers
        let components = parse_file_dir_components(&self.source)?;
        println!(
            "{} Source path parsed: {}, components: {:?}",
            "✓".green(),
            self.source,
            components
        );

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
            file_id: components.file_id,
            time_range: Default::default(),
            level: 0,
            file_size,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_file_id: None,
            num_rows,
            num_row_groups,
            sequence: None,
            partition_expr: None,
            num_series: 0,
        };
        let src_handle = FileHandle::new(file_meta, new_noop_file_purger());

        // Build the reader for a single file via ParquetReaderBuilder
        let table_dir = components.table_dir();
        let (src_access_layer, cache_manager) = build_access_layer_simple(
            &components,
            object_store.clone(),
            &mut mito_engine_config,
            &store_cfg.data_home,
        )
        .await?;
        let reader_build_start = Instant::now();

        let reader = ParquetReaderBuilder::new(
            table_dir,
            components.path_type,
            src_handle.clone(),
            object_store.clone(),
        )
        .expected_metadata(Some(region_meta.clone()))
        .build()
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("build reader failed: {e:?}"),
            }
            .build()
        })?;

        let reader_build_elapsed = reader_build_start.elapsed();
        let total_rows = reader.parquet_metadata().file_metadata().num_rows();
        println!("{} Reader built in {:?}", "✓".green(), reader_build_elapsed);

        // Build write request
        let fulltext_index_config = FulltextIndexConfig {
            create_on_compaction: Mode::Disable,
            ..Default::default()
        };

        let write_req = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata: region_meta,
            source: Either::Left(Source::Reader(Box::new(reader))),
            cache_manager,
            storage: None,
            max_sequence: None,
            index_options: Default::default(),
            index_config: mito_engine_config.index.clone(),
            inverted_index_config: MitoConfig::default().inverted_index,
            fulltext_index_config,
            bloom_filter_index_config: MitoConfig::default().bloom_filter_index,
        };

        // Write SST
        println!("{}", "Writing SST...".yellow());

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
        let mut metrics = Metrics::new(WriteType::Flush);
        let infos = src_access_layer
            .write_sst(write_req, &WriteOptions::default(), &mut metrics)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("write_sst failed: {e:?}"),
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
                        println!("{}: Failed to generate flamegraph: {}", "Error".red(), e);
                    } else if let Err(e) = std::fs::write(pprof_file, flamegraph_data) {
                        println!(
                            "{}: Failed to write flamegraph to {}: {}",
                            "Error".red(),
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
                    println!("{}: Failed to generate pprof report: {}", "Error".red(), e);
                }
            }
        }
        assert_eq!(infos.len(), 1);
        let dst_file_id = infos[0].file_id;
        let dst_file_path = format!("{}/{}.parquet", components.region_dir(), dst_file_id);
        let mut dst_index_path = None;
        if infos[0].index_metadata.file_size > 0 {
            dst_index_path = Some(format!(
                "{}/index/{}.puffin",
                components.region_dir(),
                dst_file_id
            ));
        }

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
        println!("  {}: {:?}", "Metrics".bold(), metrics,);

        // Print infos
        println!("  {}: {:?}", "Index".bold(), infos[0].index_metadata);

        // Cleanup
        println!("\n{}", "Cleaning up...".yellow());
        object_store.delete(&dst_file_path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to delete dest file {}: {}", dst_file_path, e),
            }
            .build()
        })?;
        println!("{} Temporary file {} deleted", "✓".green(), dst_file_path);

        if let Some(index_path) = dst_index_path {
            object_store.delete(&index_path).await.map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Failed to delete dest index file {}: {}", index_path, e),
                }
                .build()
            })?;
            println!(
                "{} Temporary index file {} deleted",
                "✓".green(),
                index_path
            );
        }

        println!("\n{}", "Benchmark completed successfully!".green().bold());
        Ok(())
    }
}

#[derive(Debug)]
struct FileDirComponents {
    catalog: String,
    schema: String,
    table_id: u32,
    region_sequence: u32,
    path_type: PathType,
    file_id: FileId,
}

impl FileDirComponents {
    fn table_dir(&self) -> String {
        format!("data/{}/{}/{}", self.catalog, self.schema, self.table_id)
    }

    fn region_dir(&self) -> String {
        let region_name = region_name(self.table_id, self.region_sequence);
        match self.path_type {
            PathType::Bare => {
                format!(
                    "data/{}/{}/{}/{}",
                    self.catalog, self.schema, self.table_id, region_name
                )
            }
            PathType::Data => {
                format!(
                    "data/{}/{}/{}/{}/data",
                    self.catalog, self.schema, self.table_id, region_name
                )
            }
            PathType::Metadata => {
                format!(
                    "data/{}/{}/{}/{}/metadata",
                    self.catalog, self.schema, self.table_id, region_name
                )
            }
        }
    }
}

fn parse_file_dir_components(path: &str) -> error::Result<FileDirComponents> {
    // Define the regex pattern to match all three path styles
    let pattern =
        r"^data/([^/]+)/([^/]+)/([^/]+)/([^/]+)_([^/]+)(?:/data|/metadata)?/(.+).parquet$";

    // Compile the regex
    let re = Regex::new(pattern).expect("Invalid regex pattern");

    // Determine the path type
    let path_type = if path.contains("/data/") {
        PathType::Data
    } else if path.contains("/metadata/") {
        PathType::Metadata
    } else {
        PathType::Bare
    };

    // Try to match the path
    let components = (|| {
        let captures = re.captures(path)?;
        if captures.len() != 7 {
            return None;
        }
        let mut components = FileDirComponents {
            catalog: "".to_string(),
            schema: "".to_string(),
            table_id: 0,
            region_sequence: 0,
            path_type,
            file_id: FileId::default(),
        };
        // Extract the components
        components.catalog = captures.get(1)?.as_str().to_string();
        components.schema = captures.get(2)?.as_str().to_string();
        components.table_id = captures[3].parse().ok()?;
        components.region_sequence = captures[5].parse().ok()?;
        let file_id_str = &captures[6];
        components.file_id = FileId::parse_str(file_id_str).ok()?;
        Some(components)
    })();
    components.context(error::IllegalConfigSnafu {
        msg: format!("Expect valid source file path, got: {}", path),
    })
}

fn extract_region_metadata(
    file_path: &str,
    meta: &parquet::file::metadata::ParquetMetaData,
) -> error::Result<RegionMetadataRef> {
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
    Ok(Arc::new(region))
}

async fn build_object_store(sc: &StorageConfig) -> error::Result<ObjectStore> {
    store::new_object_store(sc.store.clone(), &sc.data_home)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to build object store: {e:?}"),
            }
            .build()
        })
}

async fn build_access_layer_simple(
    components: &FileDirComponents,
    object_store: ObjectStore,
    config: &mut MitoConfig,
    data_home: &str,
) -> error::Result<(AccessLayerRef, CacheManagerRef)> {
    let _ = config.index.sanitize(data_home, &config.inverted_index);
    let puffin_manager = PuffinManagerFactory::new(
        &config.index.aux_path,
        config.index.staging_size.as_bytes(),
        Some(config.index.write_buffer_size.as_bytes() as _),
        config.index.staging_ttl,
    )
    .await
    .map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("Failed to build access layer: {e:?}"),
        }
        .build()
    })?;

    let intermediate_manager = IntermediateManager::init_fs(&config.index.aux_path)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to build IntermediateManager: {e:?}"),
            }
            .build()
        })?
        .with_buffer_size(Some(config.index.write_buffer_size.as_bytes() as _));

    let cache_manager =
        build_cache_manager(config, puffin_manager.clone(), intermediate_manager.clone()).await?;
    let layer = AccessLayer::new(
        components.table_dir(),
        components.path_type,
        object_store,
        puffin_manager,
        intermediate_manager,
    );
    Ok((Arc::new(layer), cache_manager))
}

async fn build_cache_manager(
    config: &MitoConfig,
    puffin_manager: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
) -> error::Result<CacheManagerRef> {
    let write_cache = write_cache_from_config(config, puffin_manager, intermediate_manager)
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to build write cache: {e:?}"),
            }
            .build()
        })?;
    let cache_manager = Arc::new(
        CacheManager::builder()
            .sst_meta_cache_size(config.sst_meta_cache_size.as_bytes())
            .vector_cache_size(config.vector_cache_size.as_bytes())
            .page_cache_size(config.page_cache_size.as_bytes())
            .selector_result_cache_size(config.selector_result_cache_size.as_bytes())
            .index_metadata_size(config.index.metadata_cache_size.as_bytes())
            .index_content_size(config.index.content_cache_size.as_bytes())
            .index_content_page_size(config.index.content_cache_page_size.as_bytes())
            .index_result_cache_size(config.index.result_cache_size.as_bytes())
            .puffin_metadata_size(config.index.metadata_cache_size.as_bytes())
            .write_cache(write_cache)
            .build(),
    );
    Ok(cache_manager)
}

fn new_noop_file_purger() -> FilePurgerRef {
    #[derive(Debug)]
    struct Noop;
    impl FilePurger for Noop {
        fn remove_file(&self, _file_meta: FileMeta, _is_delete: bool) {}
    }
    Arc::new(Noop)
}

async fn load_parquet_metadata(
    object_store: ObjectStore,
    path: &str,
    file_size: u64,
) -> Result<parquet::file::metadata::ParquetMetaData, Box<dyn std::error::Error + Send + Sync>> {
    use parquet::file::FOOTER_SIZE;
    use parquet::file::metadata::ParquetMetaDataReader;
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
    let footer = ParquetMetaDataReader::decode_footer_tail(&footer)?;
    let metadata_len = footer.metadata_length() as u64;
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
    use std::path::PathBuf;
    use std::str::FromStr;

    use common_base::readable_size::ReadableSize;
    use store_api::region_request::PathType;

    use crate::datanode::objbench::{parse_config, parse_file_dir_components};

    #[test]
    fn test_parse_dir() {
        let meta_path = "data/greptime/public/1024/1024_0000000000/metadata/00020380-009c-426d-953e-b4e34c15af34.parquet";
        let c = parse_file_dir_components(meta_path).unwrap();
        assert_eq!(
            c.file_id.to_string(),
            "00020380-009c-426d-953e-b4e34c15af34"
        );
        assert_eq!(c.catalog, "greptime");
        assert_eq!(c.schema, "public");
        assert_eq!(c.table_id, 1024);
        assert_eq!(c.region_sequence, 0);
        assert_eq!(c.path_type, PathType::Metadata);

        let c = parse_file_dir_components(
            "data/greptime/public/1024/1024_0000000000/data/00020380-009c-426d-953e-b4e34c15af34.parquet",
        ).unwrap();
        assert_eq!(
            c.file_id.to_string(),
            "00020380-009c-426d-953e-b4e34c15af34"
        );
        assert_eq!(c.catalog, "greptime");
        assert_eq!(c.schema, "public");
        assert_eq!(c.table_id, 1024);
        assert_eq!(c.region_sequence, 0);
        assert_eq!(c.path_type, PathType::Data);

        let c = parse_file_dir_components(
            "data/greptime/public/1024/1024_0000000000/00020380-009c-426d-953e-b4e34c15af34.parquet",
        ).unwrap();
        assert_eq!(
            c.file_id.to_string(),
            "00020380-009c-426d-953e-b4e34c15af34"
        );
        assert_eq!(c.catalog, "greptime");
        assert_eq!(c.schema, "public");
        assert_eq!(c.table_id, 1024);
        assert_eq!(c.region_sequence, 0);
        assert_eq!(c.path_type, PathType::Bare);
    }

    #[test]
    fn test_parse_config() {
        let path = "../../config/datanode.example.toml";
        let (storage, engine) = parse_config(&PathBuf::from_str(path).unwrap()).unwrap();
        assert_eq!(storage.data_home, "./greptimedb_data");
        assert_eq!(engine.index.staging_size, ReadableSize::gb(2));
    }
}

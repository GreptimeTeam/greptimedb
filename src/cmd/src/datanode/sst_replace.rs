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

use clap::Parser;
use colored::Colorize;
use mito2::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use mito2::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use mito2::region::ManifestStats;
use mito2::sst::file::{FileFormat, FileMeta};
use mito2::sst::file_metadata::FileMetadataInterner;
use mito2::sst::location::region_dir_from_table_dir;
use mito2::sst::parquet::max_row_group_decoded_size;
use mito2::sst::parquet::metadata::{extract_primary_key_range, extract_time_range};
use object_store::{Buffer, ObjectStore};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::record::reader::RowIter;
use parquet::schema::types::Type;
use store_api::region_request::PathType;
use store_api::storage::FileId;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

use crate::datanode::tool_util::{
    build_object_store, extract_region_metadata, max_row_group_uncompressed_size, parse_config,
    parse_file_id, parse_path_type, parse_region_id,
};
use crate::error;

/// Replace a mito region SST and update the corresponding manifest metadata.
#[derive(Debug, Parser)]
pub struct SstReplaceCommand {
    /// Path to config TOML file (same format as standalone/datanode config).
    #[clap(long, value_name = "FILE")]
    config: PathBuf,

    /// Region ID: either numeric u64 (e.g. "4398046511104") or "table_id:region_num" (e.g. "1024:0").
    #[clap(long)]
    region_id: String,

    /// Table directory relative to data home (e.g. "data/greptime/public/1024/").
    #[clap(long)]
    table_dir: String,

    /// SST file id to replace.
    #[clap(long)]
    file_id: String,

    /// Local parquet file used as the replacement.
    #[clap(long, value_name = "FILE", conflicts_with = "replacement_object")]
    replacement_file: Option<PathBuf>,

    /// Object-store parquet path used as the replacement.
    #[clap(long, value_name = "PATH", conflicts_with = "replacement_file")]
    replacement_object: Option<String>,

    /// Path type for the region: auto, bare, data, metadata.
    #[clap(long, default_value = "auto")]
    path_type: String,

    /// Write the replacement under a new file ID and publish the manifest edit.
    #[clap(long, default_value_t = false)]
    confirm: bool,

    /// Verbose output.
    #[clap(short, long, default_value_t = false)]
    verbose: bool,
}

impl SstReplaceCommand {
    pub async fn run(&self) -> error::Result<()> {
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting sst-replace...".cyan().bold());

        let region_id = parse_region_id(&self.region_id)?;
        let file_id = parse_file_id(&self.file_id)?;
        let path_type = parse_optional_path_type(&self.path_type)?;
        let replacement = self.replacement()?;

        let (store_cfg, mito_config, _wal_config) = parse_config(&self.config)?;
        let object_store = build_object_store(&store_cfg).await?;
        println!("{} Object store initialized", "[ok]".green());

        let candidates = path_type
            .map(|path_type| vec![path_type])
            .unwrap_or_else(|| vec![PathType::Bare, PathType::Data, PathType::Metadata]);
        let mut found = None;
        let mut existing_manifests = Vec::new();
        let file_metadata_interner = FileMetadataInterner::new();
        for candidate in candidates {
            let stats = ManifestStats::default();
            let region_dir = region_dir_from_table_dir(&self.table_dir, region_id, candidate);
            let manifest_opts = RegionManifestOptions::new(
                &mito_config,
                &region_dir,
                &object_store,
                file_metadata_interner.clone(),
            );
            let Some(manifest_manager) = RegionManifestManager::open(manifest_opts, &stats)
                .await
                .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("open {} manifest failed: {e:?}", path_type_name(candidate)),
                }
                .build()
            })?
            else {
                continue;
            };
            existing_manifests.push(path_type_name(candidate));
            let manifest = manifest_manager.manifest();
            if let Some(file) = manifest.files.get(&file_id) {
                if found.is_some() {
                    return error::IllegalConfigSnafu {
                        msg: format!(
                            "file {} exists in multiple manifests under region {}; specify --path-type",
                            file_id, region_id
                        ),
                    }
                    .fail();
                }
                found = Some((candidate, manifest_manager, file.clone()));
            }
        }
        let Some((path_type, mut manifest_manager, old_file)) = found else {
            let suffix = if existing_manifests.is_empty() {
                "no manifest found".to_string()
            } else {
                format!(
                    "manifests found for [{}], but none contains file {}",
                    existing_manifests.join(", "),
                    file_id
                )
            };
            return error::IllegalConfigSnafu {
                msg: format!("region manifest not found for {}: {}", region_id, suffix),
            }
            .fail();
        };

        if old_file.format != FileFormat::Parquet
            || old_file.region_id != region_id
            || old_file.level == mito2::sst::file::LEVEL_SHARED
        {
            return error::IllegalConfigSnafu {
                msg: format!("file {file_id} must be a locally owned, unshared Parquet SST"),
            }
            .fail();
        }
        let target_path = old_file.file_path(&self.table_dir, path_type);
        let target_stat = object_store.stat(&target_path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("stat target SST {} failed: {e}", target_path),
            }
            .build()
        })?;
        let replacement_bytes = load_replacement_bytes(&object_store, replacement)
            .await?
            .to_bytes();
        let new_file_size = replacement_bytes.len() as u64;
        let replacement_reader =
            SerializedFileReader::new(replacement_bytes.clone()).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("read replacement parquet metadata failed: {e}"),
                }
                .build()
            })?;
        let parquet_meta = replacement_reader.metadata();
        let new_num_rows =
            u64::try_from(parquet_meta.file_metadata().num_rows()).map_err(|_| {
                error::IllegalConfigSnafu {
                    msg: "replacement has a negative row count".to_string(),
                }
                .build()
            })?;
        let new_num_row_groups = parquet_meta.num_row_groups() as u64;
        validate_replacement(&old_file, new_num_rows, new_num_row_groups)?;

        let source = extract_region_metadata("replacement SST", parquet_meta)?;
        let time_index = source.time_index_column();
        let manifest = manifest_manager.manifest();
        if source.region_id != old_file.region_id
            || time_index.column_id != manifest.metadata.time_index_column().column_id
        {
            return error::IllegalConfigSnafu {
                msg: "replacement region or time index differs from the target".to_string(),
            }
            .fail();
        }
        let time_range = extract_time_range(parquet_meta, time_index).ok_or_else(|| {
            error::IllegalConfigSnafu {
                msg: "replacement has no exact time-index bounds".to_string(),
            }
            .build()
        })?;
        if (old_file.sequence.is_some() || old_file.preserve_row_sequence)
            && self.replacement_object.as_deref() != Some(target_path.as_str())
        {
            // The edit retains the original sequence fence and per-row trust.
            let original = object_store.read(&target_path).await.map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("read target SST {target_path}: {e}"),
                }
                .build()
            })?;
            validate_sequences(original, &replacement_reader).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("validate replacement sequences: {e}"),
                }
                .build()
            })?;
        }
        let (primary_key_min, primary_key_max) =
            extract_primary_key_range(parquet_meta, &source).unzip();
        // A new identity isolates data, index and footer caches, including on restart.
        // A replacement has no index sidecar, series count or proven partition scope.
        let new_file = FileMeta {
            file_id: FileId::random(),
            region_id: old_file.region_id,
            level: old_file.level,
            sequence: old_file.sequence,
            preserve_row_sequence: old_file.preserve_row_sequence,
            time_range,
            primary_key_min,
            primary_key_max,
            file_size: new_file_size,
            num_rows: new_num_rows,
            num_row_groups: new_num_row_groups,
            max_row_group_uncompressed_size: max_row_group_uncompressed_size(parquet_meta),
            max_row_group_decoded_size: max_row_group_decoded_size(parquet_meta),
            ..Default::default()
        };
        let replacement_path = new_file.file_path(&self.table_dir, path_type);

        println!("{} Region: {}", "[ok]".green(), region_id);
        println!("{} Target SST: {}", "[ok]".green(), target_path.cyan());
        println!(
            "{} Size: {} -> {} bytes",
            "[ok]".green(),
            target_stat.content_length(),
            new_file_size
        );
        println!(
            "{} Rows: {}, row groups: {}",
            "[ok]".green(),
            new_num_rows,
            new_num_row_groups
        );

        if !self.confirm {
            println!(
                "{} Dry run only. Re-run with --confirm to publish the replacement.",
                "[dry-run]".yellow()
            );
            return Ok(());
        }

        object_store
            .write(&replacement_path, replacement_bytes)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("write replacement SST {} failed: {e}", replacement_path),
                }
                .build()
            })?;

        let edit = RegionEdit {
            files_to_add: vec![new_file],
            files_to_remove: vec![old_file.as_ref().clone()],
            ..Default::default()
        };
        let version = manifest_manager
            .update(
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit)),
                false,
            )
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!(
                        "update manifest for replacement {replacement_path} failed: {e:?}"
                    ),
                }
                .build()
            })?;

        println!(
            "{} Replacement committed at {replacement_path}, manifest version {}",
            "[ok]".green(),
            version
        );
        Ok(())
    }

    fn replacement(&self) -> error::Result<Replacement<'_>> {
        match (&self.replacement_file, &self.replacement_object) {
            (Some(path), None) => Ok(Replacement::Local(path)),
            (None, Some(path)) => Ok(Replacement::Object(path)),
            _ => error::IllegalConfigSnafu {
                msg: "specify exactly one of --replacement-file or --replacement-object"
                    .to_string(),
            }
            .fail(),
        }
    }
}

enum Replacement<'a> {
    Local(&'a PathBuf),
    Object(&'a str),
}

async fn load_replacement_bytes(
    object_store: &ObjectStore,
    replacement: Replacement<'_>,
) -> error::Result<Buffer> {
    match replacement {
        Replacement::Local(path) => tokio::fs::read(path).await.map(Buffer::from).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("read replacement file {} failed: {e}", path.display()),
            }
            .build()
        }),
        Replacement::Object(path) => object_store.read(path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("read replacement object {} failed: {e}", path),
            }
            .build()
        }),
    }
}

// Compare only the sequence column. Physical row-group boundaries and encodings
// may change in a layout rewrite without changing logical sequences.
fn validate_sequences(
    original: Buffer,
    replacement: &dyn FileReader,
) -> parquet::errors::Result<()> {
    fn sequences(reader: &dyn FileReader) -> parquet::errors::Result<RowIter<'_>> {
        let schema = reader.metadata().file_metadata().schema();
        let column = schema
            .get_fields()
            .iter()
            .find(|field| field.name() == SEQUENCE_COLUMN_NAME)
            .ok_or_else(|| {
                parquet::errors::ParquetError::General("missing SST sequence column".into())
            })?;
        let projection = Type::group_type_builder(schema.name())
            .with_fields(vec![column.clone()])
            .build()?;
        reader.get_row_iter(Some(projection))
    }
    let original = SerializedFileReader::new(original.to_bytes())?;
    let mut old = sequences(&original)?;
    let mut new = sequences(replacement)?;
    loop {
        match (old.next().transpose()?, new.next().transpose()?) {
            (Some(old), Some(new)) if old.get_ulong(0)? == new.get_ulong(0)? => {}
            (None, None) => return Ok(()),
            _ => {
                return Err(parquet::errors::ParquetError::General(
                    "replacement changes the target's row sequences".into(),
                ));
            }
        }
    }
}

fn validate_replacement(
    old_file: &mito2::sst::file::FileMeta,
    new_num_rows: u64,
    new_num_row_groups: u64,
) -> error::Result<()> {
    if old_file.num_rows != 0 && old_file.num_rows != new_num_rows {
        return error::IllegalConfigSnafu {
            msg: format!(
                "replacement row count mismatch: manifest has {}, replacement has {}",
                old_file.num_rows, new_num_rows
            ),
        }
        .fail();
    }
    if old_file.num_row_groups != 0 && old_file.num_row_groups != new_num_row_groups {
        return error::IllegalConfigSnafu {
            msg: format!(
                "replacement row group count mismatch: manifest has {}, replacement has {}",
                old_file.num_row_groups, new_num_row_groups
            ),
        }
        .fail();
    }
    Ok(())
}

fn parse_optional_path_type(value: &str) -> error::Result<Option<PathType>> {
    match value.to_lowercase().as_str() {
        "auto" => Ok(None),
        _ => parse_path_type(value).map(Some).map_err(|_| {
            error::IllegalConfigSnafu {
                msg: format!("invalid path_type '{value}', expected: auto, bare, data, metadata"),
            }
            .build()
        }),
    }
}

fn path_type_name(path_type: PathType) -> &'static str {
    match path_type {
        PathType::Bare => "bare",
        PathType::Data => "data",
        PathType::Metadata => "metadata",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replace_refreshes_pruning_bounds() {
        check_replacement(true).await;
    }

    #[tokio::test]
    async fn test_replace_cached_delete_with_put_at_same_key_and_time() {
        check_replacement(false).await;
    }

    async fn check_replacement(move_row: bool) {
        use datafusion::prelude::{col, lit};
        use datafusion_common::ScalarValue;
        use futures::TryStreamExt;
        use mito2::config::MitoConfig;
        use mito2::test_util::{self, CreateRequestBuilder, TestEnv};
        use store_api::region_engine::RegionEngine;
        use store_api::region_request::{RegionOpenRequest, RegionRequest};
        use store_api::storage::{RegionId, ScanRequest};

        let mut target = TestEnv::new().await;
        let mut donor = TestEnv::new().await;
        let region_id = RegionId::new(1, 1);
        let create = CreateRequestBuilder::new().table_dir("table").build();
        let schema = test_util::rows_schema(&create);
        let region_dir = region_dir_from_table_dir(&create.table_dir, region_id, create.path_type);
        let config = MitoConfig {
            experimental_metadata_aggr: true,
            enable_write_cache: true,
            ..Default::default()
        };
        // Two independent regions have the same schema and sequence. The target
        // has a cached Delete and the replacement has one Put.
        let mut paths = Vec::new();
        let (new_key, new_second) = if move_row { ("b", 2) } else { ("a", 1) };
        for (env, key, second, delete) in [
            (&mut target, "a", 1, true),
            (&mut donor, new_key, new_second, false),
        ] {
            let engine = env.create_engine(config.clone()).await;
            engine
                .handle_request(region_id, RegionRequest::Create(create.clone()))
                .await
                .unwrap();
            let rows = api::v1::Rows {
                schema: schema.clone(),
                rows: test_util::build_rows_for_key(key, second, second + 1, 0),
            };
            if delete {
                test_util::delete_rows(&engine, region_id, rows).await;
            } else {
                test_util::put_rows(&engine, region_id, rows).await;
            }
            test_util::flush_region(&engine, region_id, None).await;
            engine.stop().await.unwrap();
            let dir = env
                .data_home()
                .join("data")
                .join(region_dir.trim_start_matches('/'));
            let files: Vec<_> = std::fs::read_dir(dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                .collect();
            assert_eq!(files.len(), 1);
            paths.push(files.into_iter().next().unwrap());
        }
        let data_home = target.data_home().join("data");
        let config_file = target.data_home().join("replace.toml");
        std::fs::write(
            &config_file,
            format!(
                "[storage]\ntype = \"File\"\ndata_home = {}\n[[region_engine]]\n[region_engine.mito]\n",
                toml::Value::String(data_home.to_string_lossy().into_owned())
            ),
        )
        .unwrap();
        let command = SstReplaceCommand {
            config: config_file,
            region_id: region_id.as_u64().to_string(),
            table_dir: create.table_dir.clone(),
            file_id: paths[0].file_stem().unwrap().to_str().unwrap().to_owned(),
            replacement_file: Some(paths[1].clone()),
            replacement_object: None,
            path_type: "bare".into(),
            confirm: true,
            verbose: false,
        };
        let previous_bytes = std::fs::read(&paths[0]).unwrap();
        command.run().await.unwrap();
        assert_eq!(std::fs::read(&paths[0]).unwrap(), previous_bytes);
        let engine = target.open_engine(config).await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    table_dir: create.table_dir,
                    path_type: create.path_type,
                    options: create.options,
                    skip_wal_replay: false,
                    checkpoint: None,
                    requirements: Default::default(),
                }),
            )
            .await
            .unwrap();
        let queries = if move_row {
            let cutoff = lit(ScalarValue::TimestampMillisecond(Some(1500), None));
            vec![
                (vec![col("ts").gt_eq(cutoff)], 1),
                (vec![col("tag_0").eq(lit("a"))], 0),
                (vec![col("tag_0").eq(lit("b"))], 1),
            ]
        } else {
            vec![(vec![], 1)]
        };
        for (filters, expected) in queries {
            let request = ScanRequest {
                filters,
                ..Default::default()
            };
            let scanned = engine
                .scan_to_stream(region_id, request.clone())
                .await
                .unwrap()
                .try_fold(
                    0,
                    |count, batch| async move { Ok(count + batch.num_rows()) },
                )
                .await
                .unwrap();
            assert_eq!(scanned, expected);
            let scanner = engine
                .handle_query(
                    region_id,
                    ScanRequest {
                        metadata_aggr_hint: true,
                        ..request
                    },
                )
                .await
                .unwrap();
            if expected == 1 {
                assert_eq!(scanner.properties().exact_num_rows(), Some(expected));
            } else {
                assert!(
                    scanner
                        .properties()
                        .exact_num_rows()
                        .is_none_or(|count| count == expected)
                );
            }
        }
        engine.stop().await.unwrap();
    }

    #[test]
    fn test_validate_replacement_counts() {
        let old_file = mito2::sst::file::FileMeta {
            num_rows: 4,
            num_row_groups: 1,
            ..Default::default()
        };
        assert!(validate_replacement(&old_file, 4, 1).is_ok());
        assert!(validate_replacement(&old_file, 3, 1).is_err());
        assert!(validate_replacement(&old_file, 4, 2).is_err());

        let legacy_file = mito2::sst::file::FileMeta::default();
        assert!(validate_replacement(&legacy_file, 4, 1).is_ok());
    }
}

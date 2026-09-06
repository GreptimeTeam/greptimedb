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
use mito2::sst::file::RegionFileId;
use mito2::sst::location::{region_dir_from_table_dir, sst_file_path};
use object_store::ObjectStore;
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use store_api::region_request::PathType;

use crate::datanode::tool_util::{
    build_object_store, max_row_group_uncompressed_size, parse_config, parse_file_id,
    parse_path_type, parse_region_id,
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

    /// Actually overwrite the SST object and append a manifest delta.
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
        for candidate in candidates {
            let stats = ManifestStats::default();
            let region_dir = region_dir_from_table_dir(&self.table_dir, region_id, candidate);
            let manifest_opts =
                RegionManifestOptions::new(&mito_config, &region_dir, &object_store);
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

        let region_file_id = RegionFileId::new(old_file.region_id, old_file.file_id);
        let target_path = sst_file_path(&self.table_dir, region_file_id, path_type);
        let target_stat = object_store.stat(&target_path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("stat target SST {} failed: {e}", target_path),
            }
            .build()
        })?;
        let replacement_bytes = load_replacement_bytes(&object_store, replacement).await?;
        let new_file_size = replacement_bytes.len() as u64;
        let parquet_meta = decode_parquet_metadata(&replacement_bytes).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("read replacement parquet metadata failed: {e}"),
            }
            .build()
        })?;
        let new_num_rows = parquet_meta.file_metadata().num_rows() as u64;
        let new_num_row_groups = parquet_meta.num_row_groups() as u64;
        validate_replacement(&old_file, new_num_rows, new_num_row_groups)?;

        let mut new_file = old_file.clone();
        new_file.file_size = new_file_size;
        new_file.num_rows = new_num_rows;
        new_file.num_row_groups = new_num_row_groups;
        new_file.max_row_group_uncompressed_size = max_row_group_uncompressed_size(&parquet_meta);

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
                "{} Dry run only. Re-run with --confirm to overwrite the SST and update the manifest.",
                "[dry-run]".yellow()
            );
            return Ok(());
        }

        object_store
            .write(&target_path, replacement_bytes)
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("write replacement SST {} failed: {e}", target_path),
                }
                .build()
            })?;

        let edit = RegionEdit {
            files_to_add: vec![new_file],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };
        let version = manifest_manager
            .update(
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit)),
                false,
            )
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("update manifest failed: {e:?}"),
                }
                .build()
            })?;

        println!(
            "{} Replacement committed, manifest version {}",
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
) -> error::Result<Vec<u8>> {
    match replacement {
        Replacement::Local(path) => tokio::fs::read(path).await.map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("read replacement file {} failed: {e}", path.display()),
            }
            .build()
        }),
        Replacement::Object(path) => {
            object_store
                .read(path)
                .await
                .map(|b| b.to_vec())
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("read replacement object {} failed: {e}", path),
                    }
                    .build()
                })
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

fn decode_parquet_metadata(
    data: &[u8],
) -> Result<parquet::file::metadata::ParquetMetaData, Box<dyn std::error::Error + Send + Sync>> {
    if data.len() < FOOTER_SIZE {
        return Err("file too small".into());
    }
    let footer_start = data.len() - FOOTER_SIZE;
    let mut footer = [0; FOOTER_SIZE];
    footer.copy_from_slice(&data[footer_start..]);
    let footer = FooterTail::try_new(&footer)?;
    let metadata_len = footer.metadata_length();
    if footer_start < metadata_len {
        return Err("invalid footer/metadata length".into());
    }
    let metadata_start = footer_start - metadata_len;
    Ok(ParquetMetaDataReader::decode_metadata(
        &data[metadata_start..footer_start],
    )?)
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

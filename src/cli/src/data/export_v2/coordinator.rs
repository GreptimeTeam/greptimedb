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

use common_telemetry::info;
use futures::TryStreamExt;
use object_store::ErrorKind;
use snafu::ResultExt;

use super::data::{CopyOptions, build_copy_target, execute_copy_database};
use super::error::{Result, StorageOperationSnafu};
use super::manifest::{ChunkStatus, DataFormat, Manifest, TimeRange};
use super::storage::{SnapshotStorage, StorageScheme};
use crate::common::ObjectStoreConfig;
use crate::database::DatabaseClient;

struct ExportContext<'a> {
    storage: &'a dyn SnapshotStorage,
    database_client: &'a DatabaseClient,
    snapshot_uri: &'a str,
    storage_config: &'a ObjectStoreConfig,
    catalog: &'a str,
    schemas: &'a [String],
    format: DataFormat,
    parallelism: usize,
}

pub async fn export_data(
    storage: &dyn SnapshotStorage,
    database_client: &DatabaseClient,
    snapshot_uri: &str,
    storage_config: &ObjectStoreConfig,
    manifest: &mut Manifest,
    parallelism: usize,
) -> Result<()> {
    if manifest.chunks.is_empty() {
        return Ok(());
    }

    for idx in 0..manifest.chunks.len() {
        // Skip already completed or skipped chunks (resume support)
        if matches!(
            manifest.chunks[idx].status,
            ChunkStatus::Completed | ChunkStatus::Skipped
        ) {
            continue;
        }

        let (chunk_id, time_range) = mark_chunk_in_progress(manifest, idx);
        manifest.touch();
        storage.write_manifest(manifest).await?;

        let context = ExportContext {
            storage,
            database_client,
            snapshot_uri,
            storage_config,
            catalog: &manifest.catalog,
            schemas: &manifest.schemas,
            format: manifest.format,
            parallelism,
        };
        let export_result = export_chunk(&context, chunk_id, time_range).await;

        match export_result {
            Ok(files) => {
                mark_chunk_completed(manifest, idx, files);
                manifest.touch();
                storage.write_manifest(manifest).await?;
            }
            Err(err) => {
                let err_string = err.to_string();
                mark_chunk_failed(manifest, idx, err_string);
                manifest.touch();
                storage.write_manifest(manifest).await?;
                return Err(err);
            }
        }
    }

    Ok(())
}

fn mark_chunk_in_progress(manifest: &mut Manifest, idx: usize) -> (u32, TimeRange) {
    let chunk = &mut manifest.chunks[idx];
    chunk.mark_in_progress();
    (chunk.id, chunk.time_range.clone())
}

fn mark_chunk_completed(manifest: &mut Manifest, idx: usize, files: Vec<String>) {
    let chunk = &mut manifest.chunks[idx];
    if files.is_empty() {
        chunk.mark_skipped();
    } else {
        chunk.mark_completed(files);
    }
}

fn mark_chunk_failed(manifest: &mut Manifest, idx: usize, error: String) {
    let chunk = &mut manifest.chunks[idx];
    chunk.mark_failed(error);
}

async fn export_chunk(
    context: &ExportContext<'_>,
    chunk_id: u32,
    time_range: TimeRange,
) -> Result<Vec<String>> {
    let scheme = StorageScheme::from_uri(context.snapshot_uri)?;
    let needs_dir = matches!(scheme, StorageScheme::File);
    let copy_options = CopyOptions {
        format: context.format,
        time_range,
        parallelism: context.parallelism,
    };

    for schema in context.schemas {
        let prefix = format!("data/{schema}/{chunk_id}/");
        if needs_dir {
            context
                .storage
                .object_store()
                .create_dir(&prefix)
                .await
                .context(StorageOperationSnafu {
                    operation: format!("create dir {}", prefix),
                })?;
        }

        let target = build_copy_target(
            context.snapshot_uri,
            context.storage_config,
            schema,
            chunk_id,
        )?;
        execute_copy_database(
            context.database_client,
            context.catalog,
            schema,
            &target,
            &copy_options,
        )
        .await?;
    }

    let files = list_chunk_files(context.storage, context.schemas, chunk_id).await?;
    info!("Collected {} files for chunk {}", files.len(), chunk_id);
    Ok(files)
}

async fn list_chunk_files(
    storage: &dyn SnapshotStorage,
    schemas: &[String],
    chunk_id: u32,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    for schema in schemas {
        let prefix = format!("data/{schema}/{chunk_id}/");
        let lister = storage
            .object_store()
            .lister_with(&prefix)
            .recursive(true)
            .await;

        let mut lister = match lister {
            Ok(lister) => lister,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).context(StorageOperationSnafu {
                    operation: format!("list {}", prefix),
                });
            }
        };

        while let Some(entry) = lister.try_next().await.context(StorageOperationSnafu {
            operation: format!("list {}", prefix),
        })? {
            if entry.metadata().is_dir() {
                continue;
            }
            files.push(entry.path().to_string());
        }
    }

    files.sort();
    Ok(files)
}

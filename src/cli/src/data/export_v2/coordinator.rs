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

use common_telemetry::{error, info};
use futures::{StreamExt, TryStreamExt};
use object_store::ErrorKind;
use snafu::ResultExt;
use tokio::sync::Mutex;

use super::error::{Result, StorageOperationSnafu};
use super::manifest::{ChunkStatus, DataFormat, Manifest, TimeRange};
use super::storage::{SnapshotStorage, StorageScheme};
use crate::common::ObjectStoreConfig;
use crate::data::copy::{CopyOptions, build_copy_target, execute_copy_database_to};
use crate::data::retry::{RetryConfig, run_with_retry};
use crate::database::DatabaseClient;

struct ExportContext<'a> {
    storage: &'a dyn SnapshotStorage,
    database_client: &'a DatabaseClient,
    snapshot_uri: &'a str,
    storage_config: &'a ObjectStoreConfig,
    catalog: &'a str,
    schemas: &'a [String],
    format: DataFormat,
    worker_parallelism: usize,
}

impl<'a> Clone for ExportContext<'a> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage,
            database_client: self.database_client,
            snapshot_uri: self.snapshot_uri,
            storage_config: self.storage_config,
            catalog: self.catalog,
            schemas: self.schemas,
            format: self.format,
            worker_parallelism: self.worker_parallelism,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn export_data(
    storage: &dyn SnapshotStorage,
    database_client: &DatabaseClient,
    snapshot_uri: &str,
    storage_config: &ObjectStoreConfig,
    manifest: &mut Manifest,
    worker_parallelism: usize,
    chunk_parallelism: usize,
    retry: RetryConfig,
) -> Result<()> {
    if manifest.chunks.is_empty() {
        return Ok(());
    }

    let pending = manifest
        .chunks
        .iter()
        .filter(|chunk| !matches!(chunk.status, ChunkStatus::Completed | ChunkStatus::Skipped))
        .map(|chunk| chunk.id)
        .collect::<Vec<_>>();

    if pending.is_empty() {
        return Ok(());
    }

    let manifest_state = Arc::new(Mutex::new(manifest.clone()));

    let context = ExportContext {
        storage,
        database_client,
        snapshot_uri,
        storage_config,
        catalog: &manifest.catalog,
        schemas: &manifest.schemas,
        format: manifest.format,
        worker_parallelism,
    };

    let results =
        futures::stream::iter(
            pending.into_iter().enumerate().map(|(idx, chunk_id)| {
                let context = context.clone();
                let retry = retry.clone();
                let manifest_state = manifest_state.clone();
                async move {
                    export_single_chunk(&context, &retry, manifest_state, chunk_id, idx + 1).await
                }
            }),
        )
        .buffer_unordered(chunk_parallelism.max(1))
        .collect::<Vec<_>>()
        .await;

    let mut first_err = None;
    for result in results {
        if let Err(err) = result
            && first_err.is_none()
        {
            first_err = Some(err);
        }
    }
    let final_manifest = manifest_state.lock().await.clone();
    *manifest = final_manifest;

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

async fn update_manifest_status(
    storage: &dyn SnapshotStorage,
    manifest_state: &Mutex<Manifest>,
    chunk_id: u32,
    update: impl FnOnce(&mut Manifest, usize),
) -> Result<TimeRange> {
    let mut manifest = manifest_state.lock().await;
    let idx = manifest
        .chunks
        .iter()
        .position(|chunk| chunk.id == chunk_id)
        .expect("chunk id must exist");
    let time_range = manifest.chunks[idx].time_range.clone();
    update(&mut manifest, idx);
    manifest.touch();
    storage.write_manifest(&manifest).await?;
    Ok(time_range)
}

async fn export_single_chunk(
    context: &ExportContext<'_>,
    retry: &RetryConfig,
    manifest_state: Arc<Mutex<Manifest>>,
    chunk_id: u32,
    seq: usize,
) -> Result<()> {
    let time_range = update_manifest_status(
        context.storage,
        &manifest_state,
        chunk_id,
        |manifest, idx| {
            manifest.chunks[idx].mark_in_progress();
        },
    )
    .await?;

    info!(
        "Exporting chunk {} ({:?} ~ {:?}) [{}]",
        chunk_id, time_range.start, time_range.end, seq
    );

    let retry_label = format!("export chunk {}", chunk_id);
    let result = run_with_retry(&retry_label, retry, || async {
        export_chunk(context, chunk_id, time_range.clone()).await
    })
    .await;

    match result {
        Ok((files, retries)) => {
            update_manifest_status(
                context.storage,
                &manifest_state,
                chunk_id,
                |manifest, idx| {
                    if files.is_empty() {
                        manifest.chunks[idx].mark_skipped();
                    } else {
                        manifest.chunks[idx].mark_completed(files);
                    }
                },
            )
            .await?;
            if retries > 0 {
                info!(
                    "Export chunk {}: retry succeeded after {} retries",
                    chunk_id, retries
                );
            }
            Ok(())
        }
        Err(failure) => {
            let err_string = failure.error.to_string();
            update_manifest_status(
                context.storage,
                &manifest_state,
                chunk_id,
                |manifest, idx| {
                    manifest.chunks[idx].mark_failed(err_string.clone());
                },
            )
            .await?;
            error!(
                "Export chunk {} failed after {} retries: {}",
                chunk_id, failure.retries, err_string
            );
            Err(failure.error)
        }
    }
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
        parallelism: context.worker_parallelism,
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
        execute_copy_database_to(
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

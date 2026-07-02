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
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use crate::common::ObjectStoreConfig;
use crate::data::export_v2::data::{CopyOptions, build_copy_target, execute_copy_database};
use crate::data::export_v2::error::{Error, Result};
use crate::data::export_v2::manifest::{ChunkStatus, DataFormat, Manifest, TimeRange};
use crate::data::path::data_dir_for_schema_chunk;
use crate::data::progress::{ProgressPhase, ProgressReporter};
use crate::data::snapshot_storage::{SnapshotStorage, StorageScheme};
use crate::database::DatabaseClient;

/// Owned, manifest-independent context shared by all chunk export futures.
///
/// `catalog`/`schemas`/`format` are cloned from the manifest up front so the
/// export futures never borrow the manifest, leaving the coordinator free to
/// mutate and persist it while chunks are in flight.
struct ExportContext<'a> {
    storage: &'a dyn SnapshotStorage,
    database_client: &'a DatabaseClient,
    snapshot_uri: &'a str,
    storage_config: &'a ObjectStoreConfig,
    catalog: String,
    schemas: Vec<String>,
    format: DataFormat,
    parallelism: usize,
}

pub struct ExportDataOptions<'a> {
    pub snapshot_uri: &'a str,
    pub storage_config: &'a ObjectStoreConfig,
    pub parallelism: usize,
    pub chunk_parallelism: usize,
}

pub async fn export_data(
    storage: &dyn SnapshotStorage,
    database_client: &DatabaseClient,
    manifest: &mut Manifest,
    options: ExportDataOptions<'_>,
    progress: &dyn ProgressReporter,
) -> Result<()> {
    if manifest.chunks.is_empty() {
        return Ok(());
    }

    let context = ExportContext {
        storage,
        database_client,
        snapshot_uri: options.snapshot_uri,
        storage_config: options.storage_config,
        catalog: manifest.catalog.clone(),
        schemas: manifest.schemas.clone(),
        format: manifest.format,
        parallelism: options.parallelism,
    };

    // One progress unit per chunk. Already completed/skipped chunks from a
    // previous run count up front so the reported total matches the chunk plan
    // regardless of resume position; each chunk finalized in this run (completed,
    // skipped, or failed) then increments exactly once.
    let progress_phase = ProgressPhase::start(
        progress,
        "Export data chunks",
        Some(manifest.chunks.len() as u64),
    );
    let already_done = (manifest.completed_count() + manifest.skipped_count()) as u64;
    if already_done > 0 {
        progress.inc(already_done);
    }

    let result = if options.chunk_parallelism <= 1 {
        export_data_serial(&context, storage, manifest, progress).await
    } else {
        export_data_concurrent(
            &context,
            storage,
            manifest,
            options.chunk_parallelism,
            progress,
        )
        .await
    };

    progress_phase.finish();
    result
}

/// Exports chunks one at a time, preserving the original serial behavior.
async fn export_data_serial(
    context: &ExportContext<'_>,
    storage: &dyn SnapshotStorage,
    manifest: &mut Manifest,
    progress: &dyn ProgressReporter,
) -> Result<()> {
    for idx in 0..manifest.chunks.len() {
        if matches!(
            manifest.chunks[idx].status,
            ChunkStatus::Completed | ChunkStatus::Skipped
        ) {
            continue;
        }

        let (chunk_id, time_range) = mark_chunk_in_progress(manifest, idx);
        manifest.touch();
        storage.write_manifest(manifest).await?;

        let export_result = export_chunk(context, chunk_id, time_range).await;

        let result = match export_result {
            Ok(files) => {
                mark_chunk_completed(manifest, idx, files);
                Ok(())
            }
            Err(err) => {
                mark_chunk_failed(manifest, idx, err.to_string());
                Err(err)
            }
        };

        manifest.touch();
        storage.write_manifest(manifest).await?;
        // The chunk is finalized (completed, skipped, or failed) and persisted.
        progress.inc(1);

        result?;
    }

    Ok(())
}

/// Exports up to `chunk_parallelism` chunks concurrently on the client.
///
/// The coordinator owns all manifest mutation/persistence: it marks chunks
/// `InProgress` and persists the manifest before polling their futures, then
/// applies each chunk result and persists again on completion. The export
/// futures only run COPY DATABASE and collect files; they never touch the
/// manifest, so manifest writes stay serialized in this task.
///
/// On the first chunk failure we stop scheduling new chunks but let already
/// in-flight chunks finish and persist their final status, then return the
/// first error.
async fn export_data_concurrent(
    context: &ExportContext<'_>,
    storage: &dyn SnapshotStorage,
    manifest: &mut Manifest,
    chunk_parallelism: usize,
    progress: &dyn ProgressReporter,
) -> Result<()> {
    let mut pending = FuturesUnordered::new();
    let mut next_idx = 0;
    let mut first_error: Option<Error> = None;

    loop {
        let mut scheduled = false;

        // Schedule eligible chunks in order up to the parallelism limit. Once a
        // failure is seen, stop scheduling but keep draining in-flight chunks.
        while first_error.is_none() && pending.len() < chunk_parallelism {
            let Some(idx) = next_eligible_chunk(manifest, &mut next_idx) else {
                break;
            };

            let (chunk_id, time_range) = mark_chunk_in_progress(manifest, idx);
            scheduled = true;

            pending.push(async move {
                let result = export_chunk(context, chunk_id, time_range).await;
                (idx, result)
            });
        }

        if scheduled {
            manifest.touch();
            storage.write_manifest(manifest).await?;
        }

        let Some((idx, export_result)) = pending.next().await else {
            break;
        };

        match export_result {
            Ok(files) => mark_chunk_completed(manifest, idx, files),
            Err(err) => {
                mark_chunk_failed(manifest, idx, err.to_string());
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        manifest.touch();
        storage.write_manifest(manifest).await?;
        // The chunk is finalized (completed, skipped, or failed) and persisted.
        progress.inc(1);
    }

    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Returns the index of the next chunk eligible for export, scanning forward
/// from `next_idx` and skipping already Completed/Skipped chunks. Advances
/// `next_idx` past the returned chunk so each chunk is scheduled at most once.
fn next_eligible_chunk(manifest: &Manifest, next_idx: &mut usize) -> Option<usize> {
    while *next_idx < manifest.chunks.len() {
        let idx = *next_idx;
        *next_idx += 1;
        if !matches!(
            manifest.chunks[idx].status,
            ChunkStatus::Completed | ChunkStatus::Skipped
        ) {
            return Some(idx);
        }
    }
    None
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
        chunk.mark_completed(files, None);
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

    for schema in &context.schemas {
        let prefix = data_dir_for_schema_chunk(schema, chunk_id);
        if needs_dir {
            context.storage.create_dir_all(&prefix).await?;
        }

        let target = build_copy_target(
            context.snapshot_uri,
            context.storage_config,
            schema,
            chunk_id,
        )?;
        execute_copy_database(
            context.database_client,
            &context.catalog,
            schema,
            &target,
            &copy_options,
        )
        .await?;
    }

    let files = list_chunk_files(context.storage, &context.schemas, chunk_id).await?;
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
        let prefix = data_dir_for_schema_chunk(schema, chunk_id);
        files.extend(storage.list_files_recursive(&prefix).await?);
    }

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::export_v2::manifest::ChunkMeta;

    fn pending_manifest(n: u32) -> Manifest {
        let mut manifest = Manifest::new_full(
            "greptime".to_string(),
            vec!["public".to_string()],
            TimeRange::unbounded(),
            DataFormat::Parquet,
        );
        manifest.chunks = (1..=n)
            .map(|id| ChunkMeta::new(id, TimeRange::unbounded()))
            .collect();
        manifest
    }

    #[test]
    fn test_next_eligible_chunk_scans_in_order() {
        let manifest = pending_manifest(3);
        let mut next_idx = 0;

        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(0));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(1));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(2));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), None);
    }

    #[test]
    fn test_next_eligible_chunk_skips_completed_and_skipped() {
        let mut manifest = pending_manifest(4);
        manifest.chunks[0].mark_completed(vec!["data/public/1/f.parquet".to_string()], None);
        manifest.chunks[2].mark_skipped();
        let mut next_idx = 0;

        // Chunk 0 (completed) and chunk 2 (skipped) are skipped; failed/pending eligible.
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(1));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(3));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), None);
    }

    #[test]
    fn test_next_eligible_chunk_treats_failed_and_in_progress_as_eligible() {
        let mut manifest = pending_manifest(2);
        manifest.chunks[0].mark_failed("boom".to_string());
        manifest.chunks[1].mark_in_progress();
        let mut next_idx = 0;

        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(0));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), Some(1));
        assert_eq!(next_eligible_chunk(&manifest, &mut next_idx), None);
    }
}

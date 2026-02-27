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

use std::result::Result as StdResult;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use common_telemetry::{error, info};
use futures::StreamExt;
#[cfg(test)]
use snafu::GenerateImplicitData;
#[cfg(test)]
use snafu::Location;
use snafu::ResultExt;

#[cfg(test)]
use super::error::Error;
use super::error::{ExportSnafu, ImportIncompleteSnafu, Result};
use super::state::ImportStateStore;
use crate::common::ObjectStoreConfig;
use crate::data::copy::{build_copy_source, execute_copy_database_from};
use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus, DataFormat, Manifest};
use crate::data::retry::{RetryConfig, RetryFailure, run_with_retry};
use crate::database::DatabaseClient;

#[cfg(test)]
static IMPORT_FAIL_ONCE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
fn take_injected_failure() -> Option<Error> {
    if std::env::var("GREPTIME_TEST_IMPORT_FAIL_ONCE")
        .ok()
        .as_deref()
        != Some("1")
    {
        return None;
    }
    if IMPORT_FAIL_ONCE.swap(true, Ordering::SeqCst) {
        return None;
    }
    Some(Error::StateOperation {
        operation: "import retry test".into(),
        path: "GREPTIME_TEST_IMPORT_FAIL_ONCE".into(),
        error: std::io::Error::other("injected failure"),
        location: Location::generate(),
    })
}

#[derive(Clone)]
pub struct ImportContext {
    pub catalog: String,
    pub snapshot_uri: String,
    pub storage_config: ObjectStoreConfig,
    pub database_client: DatabaseClient,
    pub format: DataFormat,
    pub worker_parallelism: usize,
    pub chunk_parallelism: usize,
    pub retry: RetryConfig,
    pub state_store: Arc<ImportStateStore>,
}

pub async fn import_data(
    context: ImportContext,
    manifest: &Manifest,
    schemas: &[String],
) -> Result<()> {
    let mut pending_chunks = Vec::new();
    for (idx, chunk) in manifest.chunks.iter().enumerate() {
        if chunk.status == ChunkStatus::Skipped {
            continue;
        }
        if context.state_store.is_completed(chunk.id).await {
            continue;
        }
        pending_chunks.push((idx, chunk.clone()));
    }

    if pending_chunks.is_empty() {
        return Ok(());
    }

    let total = manifest.chunks.len();
    info!(
        "Importing data: {} pending chunks, {} schemas (chunk_parallelism={}, worker_parallelism={})",
        pending_chunks.len(),
        schemas.len(),
        context.chunk_parallelism,
        context.worker_parallelism
    );

    let schemas = Arc::new(schemas.to_vec());
    let results =
        futures::stream::iter(
            pending_chunks.into_iter().map(|(idx, chunk)| {
                let context = context.clone();
                let schemas = schemas.clone();
                async move {
                    import_single_chunk(context, chunk, schemas.as_ref(), idx + 1, total).await
                }
            }),
        )
        .buffer_unordered(context.chunk_parallelism.max(1))
        .collect::<Vec<_>>()
        .await;

    let failed = results.iter().filter(|result| result.is_err()).count();
    if failed > 0 {
        return ImportIncompleteSnafu {
            failed_chunks: failed,
            state_path: context.state_store.path().display().to_string(),
        }
        .fail();
    }

    Ok(())
}

async fn import_single_chunk(
    context: ImportContext,
    chunk: ChunkMeta,
    schemas: &[String],
    seq: usize,
    total: usize,
) -> Result<()> {
    context
        .state_store
        .update_chunk(chunk.id, ChunkStatus::InProgress, None)
        .await?;

    info!(
        "[{}/{}] Chunk {} ({:?} ~ {:?})",
        seq, total, chunk.id, chunk.time_range.start, chunk.time_range.end
    );

    let result: StdResult<(), RetryFailure<super::error::Error>> = async {
        for schema in schemas {
            if !chunk_has_schema_files(&chunk, schema) {
                info!("  {}: no data, skipped", schema);
                continue;
            }

            info!("  {}: importing...", schema);
            let source = build_copy_source(
                &context.snapshot_uri,
                &context.storage_config,
                schema,
                chunk.id,
            )
            .context(ExportSnafu)
            .map_err(|error| RetryFailure { error, retries: 0 })?;
            let retry_label = format!("[{}/{}] Chunk {}", seq, total, chunk.id);
            let retry = context.retry.clone();
            let outcome = run_with_retry(&retry_label, &retry, || async {
                #[cfg(test)]
                if let Some(error) = take_injected_failure() {
                    return Err(error);
                }
                execute_copy_database_from(
                    &context.database_client,
                    &context.catalog,
                    schema,
                    &source,
                    context.format,
                    context.worker_parallelism,
                )
                .await
                .context(ExportSnafu)
            })
            .await?;
            if outcome.1 > 0 {
                info!("  {}: retry succeeded after {} retries", schema, outcome.1);
            }
            info!("  {}: done", schema);
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            context
                .state_store
                .update_chunk(chunk.id, ChunkStatus::Completed, None)
                .await?;
            info!("[{}/{}] Chunk {}: done", seq, total, chunk.id);
            Ok(())
        }
        Err(failure) => {
            let message = failure.error.to_string();
            context
                .state_store
                .update_chunk(chunk.id, ChunkStatus::Failed, Some(message.clone()))
                .await?;
            error!(
                "[{}/{}] Chunk {}: failed after {} retries: {}",
                seq, total, chunk.id, failure.retries, message
            );
            Err(failure.error)
        }
    }
}

pub(super) fn chunk_has_schema_files(chunk: &ChunkMeta, schema: &str) -> bool {
    let prefix_with_data = format!("data/{schema}/{}/", chunk.id);
    let prefix_without_data = format!("{schema}/{}/", chunk.id);
    chunk.files.iter().any(|path| {
        let normalized = path.trim_start_matches('/');
        normalized.starts_with(&prefix_with_data) || normalized.starts_with(&prefix_without_data)
    })
}

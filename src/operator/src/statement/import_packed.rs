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

use std::collections::HashMap;
use std::sync::Arc;

use common_datasource::file_format::Format;
use common_datasource::file_format::parquet::packed_reader::{
    PackReadWindows, PackedParquetReader, WINDOW_SIZE,
};
use common_datasource::object_store::{BuiltBackend, build_backend_with_path};
use common_datasource::packed_snapshot::{ObjectKind, PACK_INDEX_FILE, PackIndex};
use common_error::ext::BoxedError;
use common_query::Output;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::TableRef;
use table::requests::CopyDatabaseRequest;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::sync::CancellationToken;

use crate::error::{self, Result};
use crate::statement::StatementExecutor;
use crate::statement::database_copy::validate_database_directory;

/// All table identities are resolved before the frontend authorizes any write.
pub struct PreparedPackedImport {
    backend: BuiltBackend,
    index: PackIndex,
    parallelism: usize,
    pub tables: Vec<TableRef>,
}

impl StatementExecutor {
    pub async fn prepare_packed_import(
        &self,
        req: CopyDatabaseRequest,
        ctx: &QueryContextRef,
    ) -> Result<PreparedPackedImport> {
        validate_database_directory(&req.location)?;
        if req.with.get("metric_data_layout").map(String::as_str) != Some("packed")
            || !matches!(
                Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?,
                Format::Parquet(_)
            )
            || req
                .with
                .get("continue_on_error")
                .is_some_and(|v| v != "false")
        {
            return error::InvalidCopyParameterSnafu {
                key: "metric_data_layout",
                value: "packed requires parquet and continue_on_error=false",
            }
            .fail();
        }
        let backend =
            build_backend_with_path(&req.location, &req.connection, &self.local_file_access)
                .await
                .context(error::BuildBackendSnafu)?;
        if !backend
            .is_file(PACK_INDEX_FILE)
            .await
            .context(error::ReadObjectSnafu {
                path: PACK_INDEX_FILE,
            })?
        {
            return error::InvalidCopyParameterSnafu {
                key: "index",
                value: "not a regular file",
            }
            .fail();
        }
        let bytes = backend
            .object_store
            .read(PACK_INDEX_FILE)
            .await
            .context(error::ReadObjectSnafu {
                path: PACK_INDEX_FILE,
            })?
            .to_bytes();
        let mut index: PackIndex = serde_json::from_slice(&bytes).map_err(|e| {
            error::InvalidCopyParameterSnafu {
                key: "pack_index",
                value: e.to_string(),
            }
            .build()
        })?;
        index
            .validate()
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        index
            .tables
            .sort_by(|a, b| (&a.object, a.offset).cmp(&(&b.object, b.offset)));
        let names: Vec<_> = index.tables.iter().map(|t| t.table_name.clone()).collect();
        let mut tables = self
            .capture_database_export_tables(&req, Some(&names), ctx)
            .await?;
        if tables.len() != names.len() {
            return error::InvalidCopyParameterSnafu {
                key: "index",
                value: "physical tables and views are not data targets",
            }
            .fail();
        }
        let positions: HashMap<_, _> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), i))
            .collect();
        tables.sort_by_key(|table| positions.get(table.table_info().name.as_str()).copied());
        for object in &index.objects {
            let meta = backend
                .object_store
                .stat(&object.path)
                .await
                .context(error::ReadObjectSnafu { path: &object.path })?;
            if !backend
                .is_file_with_mode(&object.path, meta.mode())
                .await
                .context(error::ReadObjectSnafu { path: &object.path })?
            {
                return error::InvalidCopyParameterSnafu {
                    key: "object",
                    value: &object.path,
                }
                .fail();
            }
            if meta.content_length() != object.length {
                return error::InvalidCopyParameterSnafu {
                    key: "object_length",
                    value: &object.path,
                }
                .fail();
            }
        }
        Ok(PreparedPackedImport {
            backend,
            index,
            tables,
            parallelism: crate::statement::database_copy::parse_parallelism_from_option_map(
                &req.with,
            ),
        })
    }

    pub async fn import_packed(
        &self,
        plan: PreparedPackedImport,
        cancellation: &CancellationToken,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let windows = PackReadWindows::new(plan.backend.object_store.clone());
        let mut pending = PendingPackedInserts::new(plan.parallelism);
        let objects: HashMap<_, _> = plan
            .index
            .objects
            .iter()
            .map(|o| (o.path.as_str(), o))
            .collect();
        let result: Result<()> = async {
            for (entry, table) in plan.index.tables.iter().zip(plan.tables) {
                if cancellation.is_cancelled() {
                    return error::PackedImportCancelledSnafu.fail();
                }
                let object = objects.get(entry.object.as_str()).ok_or_else(|| {
                    error::InvalidCopyParameterSnafu {
                        key: "object",
                        value: &entry.object,
                    }
                    .build()
                })?;
                match object.kind {
                    ObjectKind::Pack if entry.length <= WINDOW_SIZE as u64 => {
                        let reader = PackedParquetReader::new(
                            Arc::clone(&windows),
                            object.path.clone(),
                            object.length,
                            entry.offset,
                            entry.length,
                        )
                        .context(error::ReadParquetMetadataSnafu)?;
                        self.copy_indexed_parquet(
                            reader,
                            table,
                            entry.row_count,
                            &mut pending,
                            cancellation,
                            ctx.clone(),
                        )
                        .await?
                    }
                    ObjectKind::Pack | ObjectKind::Parquet => {
                        // The virtual file keeps footer/column offsets relative to this
                        // stream. Parquet decoder allocations are separate from the cache.
                        let reader = plan
                            .backend
                            .object_store
                            .reader_with(&object.path)
                            .chunk(256 * 1024)
                            .await
                            .context(error::ReadObjectSnafu { path: &object.path })?
                            .into_futures_async_read(entry.offset..entry.offset + entry.length)
                            .await
                            .context(error::ReadObjectSnafu { path: &object.path })?
                            .compat();
                        self.copy_indexed_parquet(
                            reader,
                            table,
                            entry.row_count,
                            &mut pending,
                            cancellation,
                            ctx.clone(),
                        )
                        .await?
                    }
                };
            }
            Ok(())
        }
        .await;
        let drained = pending.drain().await;
        common_telemetry::debug!(
            peak_pending_bytes = pending.peak_pending_bytes,
            peak_batch_bytes = pending.peak_batch_bytes,
            "Packed import insertion payloads; codec and compressed windows accounted separately"
        );
        result?;
        drained?;
        Ok(Output::new(
            common_query::OutputData::AffectedRows(pending.rows),
            common_query::OutputMeta::new_with_cost(pending.cost),
        ))
    }
}

/// Retains all started writes until completion, including after the first error.
pub(crate) struct PendingPackedInserts {
    tasks: FuturesUnordered<tokio::task::JoinHandle<(usize, Result<Output>)>>,
    bytes: usize,
    parallelism: usize,
    rows: usize,
    cost: usize,
    peak_pending_bytes: usize,
    peak_batch_bytes: usize,
}

impl PendingPackedInserts {
    fn new(parallelism: usize) -> Self {
        Self {
            tasks: FuturesUnordered::new(),
            bytes: 0,
            parallelism,
            rows: 0,
            cost: 0,
            peak_pending_bytes: 0,
            peak_batch_bytes: 0,
        }
    }

    /// Backpressure before decoding, while allowing one current batch outside the queue.
    pub(crate) async fn before_decode(&mut self) -> Result<()> {
        while let Some(Some(output)) = self.tasks.next().now_or_never() {
            self.complete(output)?;
        }
        while self.tasks.len() >= self.parallelism || self.bytes >= 32 * 1024 * 1024 {
            self.complete_one().await?;
        }
        Ok(())
    }

    pub(crate) async fn admit(
        &mut self,
        bytes: usize,
        insert: impl std::future::Future<Output = Result<Output>> + Send + 'static,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        while let Some(Some(output)) = self.tasks.next().now_or_never() {
            self.complete(output)?;
        }
        self.peak_batch_bytes = self.peak_batch_bytes.max(bytes);
        const BUDGET: usize = 32 * 1024 * 1024;
        if bytes > BUDGET {
            self.drain().await?;
            if cancellation.is_cancelled() {
                return error::PackedImportCancelledSnafu.fail();
            }
            let (rows, cost) = insert.await?.extract_rows_and_cost();
            self.rows += rows;
            self.cost += cost;
            return Ok(());
        }
        while self.tasks.len() >= self.parallelism || self.bytes + bytes > BUDGET {
            self.complete_one().await?;
        }
        if cancellation.is_cancelled() {
            return error::PackedImportCancelledSnafu.fail();
        }
        self.bytes += bytes;
        self.peak_pending_bytes = self.peak_pending_bytes.max(self.bytes);
        self.tasks.push(common_runtime::spawn_query(
            async move { (bytes, insert.await) },
        ));
        Ok(())
    }

    async fn complete_one(&mut self) -> Result<()> {
        if let Some(output) = self.tasks.next().await {
            self.complete(output)?;
        }
        Ok(())
    }

    fn complete(
        &mut self,
        output: std::result::Result<(usize, Result<Output>), common_runtime::JoinError>,
    ) -> Result<()> {
        let (bytes, output) = output.context(error::JoinTaskSnafu)?;
        self.bytes -= bytes;
        let (rows, cost) = output?.extract_rows_and_cost();
        self.rows += rows;
        self.cost += cost;
        Ok(())
    }

    async fn drain(&mut self) -> Result<()> {
        let mut first = None;
        while !self.tasks.is_empty() {
            if let Err(error) = self.complete_one().await {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn oversized_batch_drains_and_runs_exclusively() {
        let cancellation = CancellationToken::new();
        let mut queue = PendingPackedInserts::new(4);
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let completed = completed.clone();
            queue
                .admit(
                    1024,
                    async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(Output::new_with_affected_rows(1))
                    },
                    &cancellation,
                )
                .await
                .unwrap();
        }
        let observed = completed.clone();
        queue
            .admit(
                40 * 1024 * 1024,
                async move {
                    assert_eq!(observed.load(Ordering::SeqCst), 2);
                    Ok(Output::new_with_affected_rows(3))
                },
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(queue.rows, 5);
        assert!(queue.tasks.is_empty());
        assert!(queue.peak_pending_bytes <= 32 * 1024 * 1024);
        assert_eq!(queue.peak_batch_bytes, 40 * 1024 * 1024);
    }

    #[tokio::test]
    async fn concurrent_insert_queue_drains_after_error_and_cancel() {
        let cancellation = CancellationToken::new();
        let mut queue = PendingPackedInserts::new(2);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let completed = Arc::new(AtomicUsize::new(0));
        for fail in [true, false] {
            let barrier = barrier.clone();
            let completed = completed.clone();
            queue
                .admit(
                    16 * 1024 * 1024,
                    async move {
                        barrier.wait().await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        if fail {
                            error::InvalidCopyParameterSnafu {
                                key: "test",
                                value: "failure",
                            }
                            .fail()
                        } else {
                            Ok(Output::new_with_affected_rows(1))
                        }
                    },
                    &cancellation,
                )
                .await
                .unwrap();
        }
        cancellation.cancel();
        assert!(
            queue
                .admit(1, async { panic!("must not start") }, &cancellation)
                .await
                .is_err()
        );
        let _ = queue.drain().await;
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(queue.bytes, 0);
        assert!(queue.tasks.is_empty());
    }
}

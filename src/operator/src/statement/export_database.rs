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

//! Request-scoped database export preparation. The frontend authorizes the entire
//! captured selection before preparation; these trusted methods do not check ACLs.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;

use common_datasource::file_format::Format;
use common_datasource::object_store::{FILE_SCHEMA, FS_SCHEMA, build_backend_for_write, parse_url};
use common_meta::key::table_route::TableRouteValue;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::TableRef;
use table::metadata::TableType;
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use tokio_util::sync::CancellationToken;

use crate::error::{self, InvalidDatabaseExportSnafu, Result};
use crate::statement::StatementExecutor;
use crate::statement::database_copy::{
    DatabaseExportFile, parse_parallelism_from_option_map, validate_database_directory,
    validate_database_export_layout,
};
use crate::statement::export_logical_tables::{LogicalTableExport, LogicalTableExportLimits};

/// A validated request-scoped selection, not a metadata snapshot or an ACL token.
pub struct PreparedDatabaseExport {
    request: CopyDatabaseRequest,
    jobs: Vec<DatabaseExportJob>,
}

impl PreparedDatabaseExport {
    /// Inspect physical grouping through the integration testing adapter.
    #[cfg(feature = "testing")]
    pub fn job_count_for_test(&self) -> usize {
        self.jobs.len()
    }
}

enum DatabaseExportJob {
    Ordinary {
        table: TableRef,
        output: DatabaseExportFile,
    },
    Metric(LogicalTableExport),
}

impl StatementExecutor {
    /// Validate all jobs and destinations before executing any query or writer.
    /// Callers must authorize every captured table before calling this method.
    pub async fn prepare_database_export(
        &self,
        req: CopyDatabaseRequest,
        tables: Vec<TableRef>,
    ) -> Result<PreparedDatabaseExport> {
        validate_database_export_layout(&req.with)?;
        validate_database_directory(&req.location)?;
        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;
        ensure!(
            matches!(format, Format::Parquet(_)),
            error::UnsupportedFormatSnafu { format }
        );
        let (scheme, _, _) = parse_url(&req.location).context(error::BuildBackendSnafu)?;
        let local =
            scheme.eq_ignore_ascii_case(FS_SCHEMA) || scheme.eq_ignore_ascii_case(FILE_SCHEMA);
        let mut filenames = HashSet::new();
        let mut logical = Vec::new();
        let mut jobs = Vec::new();
        for table in tables {
            let info = table.table_info();
            let name = &info.name;
            let output = DatabaseExportFile::new(&req.location, name, ".parquet")?;
            ensure!(
                filenames.insert(if local {
                    output.path.to_ascii_lowercase()
                } else {
                    output.path.clone()
                }),
                InvalidDatabaseExportSnafu {
                    reason: format!("duplicate output name: {name}")
                }
            );
            ensure!(
                info.catalog_name == req.catalog_name
                    && info.schema_name == req.schema_name
                    && table.table_type() == TableType::Base,
                InvalidDatabaseExportSnafu {
                    reason: "expected base tables in the selected schema"
                }
            );
            if info.meta.engine == METRIC_ENGINE_NAME {
                ensure!(
                    info.meta
                        .options
                        .extra_options
                        .contains_key(LOGICAL_TABLE_METADATA_KEY),
                    InvalidDatabaseExportSnafu {
                        reason: "expected a Metric logical table"
                    }
                );
                logical.push(table);
            } else {
                jobs.push(DatabaseExportJob::Ordinary { table, output });
            }
        }
        let ids = logical
            .iter()
            .map(|t| t.table_info().table_id())
            .collect::<Vec<_>>();
        let routes = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .batch_get(&ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        let mut groups = BTreeMap::<_, Vec<TableRef>>::new();
        for (table, route) in logical.into_iter().zip(routes) {
            let Some(TableRouteValue::Logical(route)) = route else {
                return InvalidDatabaseExportSnafu {
                    reason: format!(
                        "missing or non-logical route for {}",
                        table.table_info().table_id()
                    ),
                }
                .fail();
            };
            groups
                .entry(route.physical_table_id())
                .or_default()
                .push(table);
        }
        let physical_ids = groups.keys().copied().collect::<Vec<_>>();
        let mut physical = self
            .catalog_manager
            .tables_by_ids(&req.catalog_name, &req.schema_name, &physical_ids)
            .await
            .context(error::CatalogSnafu)?
            .into_iter()
            .map(|t| (t.table_info().table_id(), t))
            .collect::<BTreeMap<_, _>>();
        for (id, tables) in groups {
            let table = physical
                .remove(&id)
                .with_context(|| InvalidDatabaseExportSnafu {
                    reason: format!("missing physical table {id} in the selected schema"),
                })?;
            jobs.push(DatabaseExportJob::Metric(
                LogicalTableExport::try_new_in_directory(table, &tables, &req.location)?,
            ));
        }
        build_backend_for_write(&req.location, &req.connection, &self.local_file_access)
            .await
            .context(error::BuildBackendSnafu)?;
        Ok(PreparedDatabaseExport { request: req, jobs })
    }
}

/// Returned only after every started job has closed or drained its owned I/O.
#[derive(Debug)]
pub struct DatabaseExportSummary {
    pub rows: usize,
    pub output_files: Vec<String>,
}

impl StatementExecutor {
    /// Execute with one job budget. The caller must keep this future alive until
    /// it returns, including after cooperative cancellation. Closed files remain
    /// owned by the caller's attempt; this method does not publish completion.
    pub async fn export_database(
        &self,
        plan: PreparedDatabaseExport,
        cancellation: &CancellationToken,
        ctx: QueryContextRef,
    ) -> Result<DatabaseExportSummary> {
        let mut output_files = Vec::new();
        for job in &plan.jobs {
            match job {
                DatabaseExportJob::Ordinary { output, .. } => {
                    output_files.push(output.location.clone())
                }
                DatabaseExportJob::Metric(unit) => {
                    output_files.extend(unit.output_files().map(|file| file.location.clone()))
                }
            }
        }
        output_files.sort();
        let req = &plan.request;
        let rows = run_database_export_jobs(
            plan.jobs,
            parse_parallelism_from_option_map(&req.with),
            cancellation,
            |job, token| {
                let ctx = ctx.clone();
                async move {
                    match job {
                        DatabaseExportJob::Metric(unit) => self
                            .export_logical_tables(
                                &unit,
                                &req.location,
                                &req.connection,
                                req.time_range.as_ref(),
                                LogicalTableExportLimits::default(),
                                &token,
                                ctx,
                            )
                            .await
                            .map(|summary| summary.rows),
                        DatabaseExportJob::Ordinary { table, output } => {
                            let info = table.table_info();
                            let copy = CopyTableRequest {
                                catalog_name: info.catalog_name.clone(),
                                schema_name: info.schema_name.clone(),
                                table_name: info.name.clone(),
                                location: output.location,
                                with: req.with.clone(),
                                connection: req.connection.clone(),
                                pattern: None,
                                direction: CopyDirection::Export,
                                timestamp_range: req.time_range,
                                limit: None,
                            };
                            self.copy_captured_table_to(table, copy, ctx).await
                        }
                    }
                }
            },
        )
        .await?;
        Ok(DatabaseExportSummary { rows, output_files })
    }
}

async fn run_database_export_jobs<J, F: Future<Output = Result<usize>>>(
    jobs: impl IntoIterator<Item = J>,
    parallelism: usize,
    cancellation: &CancellationToken,
    mut run: impl FnMut(J, CancellationToken) -> F,
) -> Result<usize> {
    let token = CancellationToken::new();
    let mut jobs = jobs.into_iter();
    let mut active = FuturesUnordered::new();
    let mut first_error = None;
    let mut rows = 0;
    loop {
        while first_error.is_none()
            && !cancellation.is_cancelled()
            && active.len() < parallelism.max(1)
        {
            let Some(job) = jobs.next() else { break };
            active.push(run(job, token.clone()));
        }
        if first_error.is_none() && cancellation.is_cancelled() {
            first_error = Some(error::DatabaseExportCancelledSnafu.build());
            token.cancel();
        }
        if active.is_empty() {
            break;
        }
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled(), if first_error.is_none() => {
                first_error = Some(error::DatabaseExportCancelledSnafu.build());
                token.cancel();
                continue;
            }
            result = active.next() => result,
        };
        match result {
            Some(Ok(count)) => rows += count,
            Some(Err(err)) if first_error.is_none() => {
                first_error = Some(err);
                token.cancel();
            }
            Some(Err(err)) => common_telemetry::warn!(err; "Failed to drain database export job"),
            None => break,
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(rows),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Semaphore, mpsc};

    use super::*;

    #[tokio::test]
    async fn bounded_admission_and_drain() {
        for cancel in [false, true] {
            let cancellation = CancellationToken::new();
            let start_error = Arc::new(Semaphore::new(0));
            let finish_io = Arc::new(Semaphore::new(0));
            let (started, mut receiver) = mpsc::unbounded_channel();
            let finished = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn({
                let cancellation = cancellation.clone();
                let start_error = start_error.clone();
                let finish_io = finish_io.clone();
                let finished = finished.clone();
                async move {
                    run_database_export_jobs(0..4, 2, &cancellation, |job, token| {
                        let started = started.clone();
                        let start_error = start_error.clone();
                        let finish_io = finish_io.clone();
                        let finished = finished.clone();
                        async move {
                            started.send(job).unwrap();
                            if job == 0 {
                                if cancel {
                                    token.cancelled().await;
                                } else {
                                    start_error.acquire().await.unwrap().forget();
                                }
                                return InvalidDatabaseExportSnafu {
                                    reason: "first error",
                                }
                                .fail();
                            }
                            // Ordinary COPY continues its I/O even when Metric jobs cancel.
                            token.cancelled().await;
                            started.send(10).unwrap();
                            finish_io.acquire().await.unwrap().forget();
                            finished.fetch_add(1, Ordering::SeqCst);
                            InvalidDatabaseExportSnafu {
                                reason: "drain error",
                            }
                            .fail()
                        }
                    })
                    .await
                }
            });
            assert_eq!(receiver.recv().await, Some(0));
            assert_eq!(receiver.recv().await, Some(1));
            assert!(receiver.try_recv().is_err());
            if cancel {
                cancellation.cancel();
            } else {
                start_error.add_permits(1);
            }
            assert_eq!(receiver.recv().await, Some(10));
            assert!(!task.is_finished());
            finish_io.add_permits(1);
            let err = task.await.unwrap().unwrap_err();
            if cancel {
                assert!(matches!(err, error::Error::DatabaseExportCancelled { .. }));
            } else {
                assert!(
                    matches!(err, error::Error::InvalidDatabaseExport { reason } if reason == "first error")
                );
            }
            assert_eq!(finished.load(Ordering::SeqCst), 1);
            assert_eq!(receiver.recv().await, None);
        }
    }

    #[tokio::test]
    async fn cancellation_before_admission_and_successful_refill() {
        let token = CancellationToken::new();
        token.cancel();
        let result = run_database_export_jobs(0..4, 2, &token, |_, _| async {
            panic!("cancelled job admitted")
        })
        .await;
        assert!(matches!(
            result,
            Err(error::Error::DatabaseExportCancelled { .. })
        ));
        let started = AtomicUsize::new(0);
        let result = run_database_export_jobs(0..7, 2, &CancellationToken::new(), |job, _| {
            started.fetch_add(1, Ordering::SeqCst);
            async move { Ok(job) }
        })
        .await
        .unwrap();
        assert_eq!(result, 21);
        assert_eq!(started.load(Ordering::SeqCst), 7);
    }
}

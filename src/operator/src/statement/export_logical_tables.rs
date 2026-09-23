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

//! Internal physical-table export into logical-table Parquet files.
//!
//! Callers must authorize the captured tables and destination before invoking
//! this component, and keep the source stable. Each destination must be owned
//! exclusively by the export attempt. Completion and cleanup of closed files
//! belong to the caller's chunk protocol, not individual file completion.

pub(crate) mod writers;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, StructArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;
use common_datasource::object_store::build_backend_for_write;
use common_datasource::parquet_writer::{
    ParquetCreationPolicy, ParquetFileWriter, ParquetWriterLimits,
};
use common_meta::key::table_route::{TableRouteManager, TableRouteValue};
use common_query::OutputData;
use common_recordbatch::SendableRecordBatchStream;
use common_time::range::TimestampRange;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::TableReference as DfTableReference;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col};
use futures::StreamExt;
use object_store::ObjectStore;
use session::context::QueryContextRef;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME as TABLE_ID, DATA_SCHEMA_TSID_COLUMN_NAME as TSID,
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use table::TableRef;
use table::metadata::TableId;
use table::table::adapter::DfTableProviderAdapter;
use tokio_util::sync::CancellationToken;

use crate::error::{self, InvalidLogicalTableExportSnafu, LogicalTableExportResourceSnafu, Result};
use crate::statement::StatementExecutor;
use crate::statement::database_copy::DatabaseExportFile;
use crate::statement::export_logical_tables::writers::{ExportWriteBudget, Payload, TableWriters};

/// Export preprocessing and per-file limits. Query memory and spill remain
/// governed by the query engine.
/// Batch checks happen after allocation; flush thresholds are not hard RSS caps.
#[derive(Clone, Copy, Debug)]
pub struct LogicalTableExportLimits {
    /// Maximum retained scan batch before routing.
    pub input_batch_bytes: usize,
    /// Maximum estimated expanded logical values in one conversion slice.
    pub conversion_bytes: usize,
    /// Limits owned by the single-file Parquet writer.
    pub writer: ParquetWriterLimits,
}

impl Default for LogicalTableExportLimits {
    fn default() -> Self {
        Self {
            input_batch_bytes: 64 * 1024 * 1024,
            conversion_bytes: 1024 * 1024,
            writer: ParquetWriterLimits {
                row_group_rows: 8192,
                flush_threshold_bytes: 8 * 1024 * 1024,
                max_row_groups: 4096,
            },
        }
    }
}

impl LogicalTableExportLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            self.input_batch_bytes > 0 && self.conversion_bytes > 0,
            InvalidLogicalTableExportSnafu {
                reason: "export limits must be positive"
            }
        );
        Ok(())
    }
}

/// Metadata captured for one physical table and its selected logical tables.
/// Construct one unit per physical table, schema and time chunk. Capturing these
/// references supplies no snapshot isolation or locking guarantee.
pub struct LogicalTableExport {
    physical_table: TableRef,
    scan_projection: Vec<usize>,
    logical_tables: BTreeMap<TableId, LogicalTableProjection>,
}

pub(crate) struct LogicalTableProjection {
    output: DatabaseExportFile,
    schema: SchemaRef,
    projection: Vec<usize>,
}

impl LogicalTableExport {
    /// Capture schemas from selected Metric table references.
    /// Export validates their physical-table association against table routes.
    pub fn try_new(physical: TableRef, tables: &[TableRef]) -> Result<Self> {
        Self::try_new_in_directory(physical, tables, "")
    }

    pub(crate) fn try_new_in_directory(
        physical: TableRef,
        tables: &[TableRef],
        directory: &str,
    ) -> Result<Self> {
        let physical_info = physical.table_info();
        ensure!(
            physical_info.meta.engine == METRIC_ENGINE_NAME
                && physical_info
                    .meta
                    .options
                    .extra_options
                    .contains_key(PHYSICAL_TABLE_METADATA_KEY)
                && !tables.is_empty(),
            InvalidLogicalTableExportSnafu {
                reason: "expected a Metric physical table and selected logical tables"
            }
        );
        let physical_schema = physical.schema();
        let id_index = physical_schema.column_index_by_name(TABLE_ID).context(
            InvalidLogicalTableExportSnafu {
                reason: "physical schema has no __table_id",
            },
        )?;
        let mut scan_projection = BTreeSet::from([id_index]);
        let mut logical_tables = BTreeMap::new();
        for table in tables {
            let info = table.table_info();
            let name = &info.name;
            ensure!(
                info.catalog_name == physical_info.catalog_name
                    && info.schema_name == physical_info.schema_name
                    && info.meta.engine == METRIC_ENGINE_NAME
                    && info
                        .meta
                        .options
                        .extra_options
                        .contains_key(LOGICAL_TABLE_METADATA_KEY),
                InvalidLogicalTableExportSnafu {
                    reason: format!(
                        "{name} is not a Metric logical table in the physical table schema"
                    )
                }
            );
            let schema = table.schema().arrow_schema().clone();
            let indices = schema
                .fields()
                .iter()
                .map(|field| {
                    ensure!(
                        field.name() != TABLE_ID && field.name() != TSID,
                        InvalidLogicalTableExportSnafu {
                            reason: "logical schema contains internal Metric columns"
                        }
                    );
                    let index = physical_schema
                        .column_index_by_name(field.name())
                        .with_context(|| InvalidLogicalTableExportSnafu {
                            reason: format!("physical schema lacks {}", field.name()),
                        })?;
                    ensure!(
                        physical_schema.arrow_schema().field(index).data_type()
                            == field.data_type(),
                        InvalidLogicalTableExportSnafu {
                            reason: format!("physical/logical type mismatch for {}", field.name())
                        }
                    );
                    Ok(index)
                })
                .collect::<Result<Vec<_>>>()?;
            scan_projection.extend(indices.iter().copied());
            ensure!(
                logical_tables
                    .insert(
                        info.table_id(),
                        LogicalTableProjection {
                            output: DatabaseExportFile::new(directory, name, ".parquet")?,
                            schema,
                            projection: indices,
                        }
                    )
                    .is_none(),
                InvalidLogicalTableExportSnafu {
                    reason: "duplicate logical table"
                }
            );
        }
        let scan_projection = scan_projection.into_iter().collect::<Vec<_>>();
        for file in logical_tables.values_mut() {
            for index in &mut file.projection {
                *index = scan_projection.binary_search(index).map_err(|_| {
                    error::UnexpectedSnafu {
                        violated: "logical column missing from physical projection",
                    }
                    .build()
                })?;
            }
        }
        Ok(Self {
            physical_table: physical,
            scan_projection,
            logical_tables,
        })
    }

    pub(crate) fn output_files(&self) -> impl Iterator<Item = &DatabaseExportFile> {
        self.logical_tables.values().map(|table| &table.output)
    }

    async fn validate_table_routes(&self, manager: &TableRouteManager) -> Result<()> {
        let table_ids = self.logical_tables.keys().copied().collect::<Vec<_>>();
        let routes = manager
            .table_route_storage()
            .batch_get(&table_ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        let physical_table_id = self.physical_table.table_info().table_id();
        for (table_id, route) in table_ids.into_iter().zip(routes) {
            ensure!(
                matches!(route, Some(TableRouteValue::Logical(route)) if route.physical_table_id() == physical_table_id),
                InvalidLogicalTableExportSnafu {
                    reason: format!(
                        "logical table {table_id} does not belong to physical table {physical_table_id}"
                    )
                }
            );
        }
        Ok(())
    }

    fn build_plan(&self, time_range: Option<&TimestampRange>) -> Result<LogicalPlan> {
        let info = self.physical_table.table_info();
        let filters = self
            .physical_table
            .schema()
            .timestamp_column()
            .and_then(|column| {
                common_query::logical_plan::build_filter_from_timestamp(&column.name, time_range)
            })
            .into_iter()
            .collect::<Vec<_>>();
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(self.physical_table.clone()),
        )));
        let mut builder = LogicalPlanBuilder::scan_with_filters(
            DfTableReference::full(
                info.catalog_name.clone(),
                info.schema_name.clone(),
                info.name.clone(),
            ),
            source,
            Some(self.scan_projection.clone()),
            filters.clone(),
        )
        .context(error::BuildDfLogicalPlanSnafu)?;
        for filter in filters {
            builder = builder
                .filter(filter)
                .context(error::BuildDfLogicalPlanSnafu)?;
        }
        builder
            .sort(vec![col(TABLE_ID).sort(true, false)])
            .context(error::BuildDfLogicalPlanSnafu)?
            .build()
            .context(error::BuildDfLogicalPlanSnafu)
    }
}

/// Counts returned only after every selected logical file has been closed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LogicalTableExportSummary {
    pub rows: usize,
    pub skipped_rows: usize,
    pub files: usize,
}

impl StatementExecutor {
    /// Export selected logical tables to a fresh directory using one physical query.
    /// The caller bounds concurrent units and owns retry/cleanup of this directory.
    /// Cancellation waits for in-flight I/O before aborting the active upload.
    /// Closed files remain an incomplete chunk until the caller publishes its
    /// completion state.
    #[allow(clippy::too_many_arguments)]
    pub async fn export_logical_tables(
        &self,
        unit: &LogicalTableExport,
        directory: &str,
        connection: &HashMap<String, String>,
        time_range: Option<&TimestampRange>,
        limits: LogicalTableExportLimits,
        cancellation: &CancellationToken,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalTableExportSummary> {
        self.export_logical_tables_managed(
            unit,
            directory,
            connection,
            time_range,
            limits,
            &cancellation.child_token(),
            query_ctx,
            ExportWriteBudget::new(1),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn export_logical_tables_managed(
        &self,
        unit: &LogicalTableExport,
        directory: &str,
        connection: &HashMap<String, String>,
        time_range: Option<&TimestampRange>,
        limits: LogicalTableExportLimits,
        cancellation: &CancellationToken,
        query_ctx: QueryContextRef,
        budget: Arc<ExportWriteBudget>,
    ) -> Result<LogicalTableExportSummary> {
        limits.validate()?;
        let (store, stream) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return error::LogicalTableExportCancelledSnafu.fail(),
            result = async {
                unit.validate_table_routes(self.table_metadata_manager.table_route_manager()).await?;
                let store = build_backend_for_write(&format!("{}/", directory.trim_end_matches('/')), connection, &self.local_file_access)
                    .await.context(error::BuildBackendSnafu)?;
                let output = self.query_engine.execute(unit.build_plan(time_range)?, query_ctx)
                    .await.context(error::ExecLogicalPlanSnafu)?;
                let stream = match output.data {
                    OutputData::Stream(stream) => stream,
                    OutputData::RecordBatches(batches) => batches.as_stream(),
                    _ => return error::UnexpectedSnafu { violated: "expected physical query rows" }.fail(),
                };
                Ok((store, stream))
            } => result?,
        };
        export_stream_managed(unit, stream, &store, limits, cancellation, budget).await
    }
}

#[cfg(test)]
async fn export_stream(
    unit: &LogicalTableExport,
    stream: SendableRecordBatchStream,
    store: &ObjectStore,
    limits: LogicalTableExportLimits,
    cancellation: &CancellationToken,
) -> Result<LogicalTableExportSummary> {
    export_stream_managed(
        unit,
        stream,
        store,
        limits,
        &cancellation.child_token(),
        ExportWriteBudget::new(1),
    )
    .await
}

async fn export_stream_managed(
    unit: &LogicalTableExport,
    stream: SendableRecordBatchStream,
    store: &ObjectStore,
    limits: LogicalTableExportLimits,
    cancellation: &CancellationToken,
    budget: Arc<ExportWriteBudget>,
) -> Result<LogicalTableExportSummary> {
    let mut writers = TableWriters::new(budget.clone());
    let result = write_tables(
        unit,
        stream,
        store,
        limits,
        cancellation,
        &mut writers,
        &budget,
    )
    .await;
    let (summary, result) = match result {
        Ok(summary) => (summary, Ok(())),
        Err(error) => (LogicalTableExportSummary::default(), Err(error)),
    };
    writers.drain(result, cancellation).await?;
    Ok(summary)
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    ensure!(
        !cancellation.is_cancelled(),
        error::LogicalTableExportCancelledSnafu
    );
    Ok(())
}

async fn write_tables(
    unit: &LogicalTableExport,
    mut stream: SendableRecordBatchStream,
    store: &ObjectStore,
    limits: LogicalTableExportLimits,
    cancellation: &CancellationToken,
    writers: &mut TableWriters,
    budget: &ExportWriteBudget,
) -> Result<LogicalTableExportSummary> {
    let id_index =
        stream
            .schema()
            .column_index_by_name(TABLE_ID)
            .context(error::UnexpectedSnafu {
                violated: "physical query omitted __table_id",
            })?;
    let mut summary = LogicalTableExportSummary::default();
    let mut previous = None;
    let mut written = BTreeSet::new();
    loop {
        let batch = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return error::LogicalTableExportCancelledSnafu.fail(),
            batch = stream.next() => batch,
        };
        let Some(batch) = batch else {
            break;
        };
        let batch = batch
            .context(error::BuildRecordBatchSnafu)?
            .into_df_record_batch();
        ensure!(
            batch.get_array_memory_size() <= limits.input_batch_bytes,
            LogicalTableExportResourceSnafu {
                reason: "scan batch exceeds input byte budget"
            }
        );
        let ids = batch
            .column(id_index)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .context(error::UnexpectedSnafu {
                violated: "__table_id must be UInt32",
            })?;
        ensure!(
            ids.null_count() == 0,
            error::UnexpectedSnafu {
                violated: "null __table_id"
            }
        );
        let mut start = 0;
        while start < batch.num_rows() {
            check_cancelled(cancellation)?;
            let id = ids.value(start);
            ensure!(
                previous.is_none_or(|last| last <= id),
                error::UnexpectedSnafu {
                    violated: "physical query is not ordered by __table_id"
                }
            );
            previous = Some(id);
            let mut end = start + 1;
            while end < batch.num_rows() && ids.value(end) == id {
                end += 1;
            }
            if writers.table_id().is_some_and(|last| last != id) {
                writers.close_input();
            }
            check_cancelled(cancellation)?;
            if let Some(file) = unit.logical_tables.get(&id) {
                if writers.table_id().is_none() {
                    writers.open(id, file, store, limits, cancellation).await?;
                    written.insert(id);
                    summary.files += 1;
                }
                let projected = batch
                    .project(&file.projection)
                    .context(error::ProjectSchemaSnafu)?;
                let mut offset = start;
                while offset < end {
                    let (expanded, consumed) = expand_bounded_slice(
                        projected.clone(),
                        file.schema.clone(),
                        offset,
                        end,
                        limits.conversion_bytes,
                        budget,
                        cancellation,
                    )
                    .await?;
                    check_cancelled(cancellation)?;
                    writers.send(expanded, cancellation).await?;
                    check_cancelled(cancellation)?;
                    offset += consumed;
                    summary.rows += consumed;
                }
            } else {
                summary.skipped_rows += end - start;
            }
            start = end;
        }
    }
    writers.close_input();
    for (&id, file) in &unit.logical_tables {
        if !written.contains(&id) {
            check_cancelled(cancellation)?;
            writers.open(id, file, store, limits, cancellation).await?;
            writers.close_input();
            summary.files += 1;
        }
    }
    check_cancelled(cancellation)?;
    Ok(summary)
}

struct ActiveWriter {
    path: String,
    writer: ParquetFileWriter,
}

impl ActiveWriter {
    async fn open(
        table: &LogicalTableProjection,
        store: &ObjectStore,
        limits: LogicalTableExportLimits,
    ) -> Result<Self> {
        let path = table.output.path.clone();
        let conditional = store.info().capability().write_with_if_not_exists;
        if !conditional {
            ensure!(
                !store
                    .exists(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?,
                InvalidLogicalTableExportSnafu {
                    reason: format!("output already exists: {path}")
                }
            );
        }
        let writer = ParquetFileWriter::open_with_creation(
            table.schema.clone(),
            store.clone(),
            &path,
            1,
            Some(limits.writer),
            if conditional {
                ParquetCreationPolicy::IfNotExists
            } else {
                ParquetCreationPolicy::Overwrite
            },
        )
        .await
        .map_err(|error| map_writer_error(error, &path))?;
        Ok(Self { path, writer })
    }
}

async fn expand_bounded_slice(
    batch: RecordBatch,
    schema: SchemaRef,
    start: usize,
    end: usize,
    requested: usize,
    budget: &ExportWriteBudget,
    cancellation: &CancellationToken,
) -> Result<(Payload, usize)> {
    let (conversion, retained) = ExportWriteBudget::conversion_budget(&batch, requested)?;
    let input = batch.clone();
    let (len, estimated) = common_runtime::spawn_blocking_global(move || {
        rows_within_budget(&input, start, end, conversion)
    })
    .await
    .context(error::JoinTaskSnafu)??;
    let reservation = retained.saturating_add(estimated.saturating_mul(4));
    let permit = budget.reserve(reservation, cancellation).await?;
    common_runtime::spawn_blocking_global(move || {
        let slice = batch.slice(start, len);
        let expanded = expand_export_batch(&slice, schema)?;
        ensure!(
            expanded.get_array_memory_size() <= reservation,
            LogicalTableExportResourceSnafu {
                reason: "converted backing buffers exceed reservation"
            }
        );
        Ok((
            Payload {
                batch: expanded,
                permit,
            },
            len,
        ))
    })
    .await
    .context(error::JoinTaskSnafu)?
}

/// Limits nested casts to selected children: Arrow's List cast otherwise expands
/// the entire values array, even when the parent has been sliced.
pub(crate) fn expand_export_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| expand_export_array(array, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).context(error::ComputeArrowSnafu)
}

fn expand_export_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    match (array.data_type(), target) {
        (DataType::Dictionary(_, _), _) => {
            downcast_dictionary_array! {
                array => {
                    // Select dictionary entries before recursively converting nested values.
                    let selected = take(array.values().as_ref(), array.keys(), None)
                        .context(error::ComputeArrowSnafu)?;
                    expand_export_array(&selected, target)
                },
                _ => error::UnexpectedSnafu { violated: "invalid dictionary array" }.fail(),
            }
        }
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[0];
            let end = offsets[list.len()];
            let values = list.values().slice(start as usize, (end - start) as usize);
            let values = expand_export_array(&values, field.data_type())?;
            let offsets = OffsetBuffer::new(
                offsets
                    .iter()
                    .map(|offset| offset - start)
                    .collect::<Vec<_>>()
                    .into(),
            );
            Ok(Arc::new(
                ListArray::try_new(field.clone(), offsets, values, list.nulls().cloned())
                    .context(error::ComputeArrowSnafu)?,
            ))
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let array = array.as_struct();
            let columns = array
                .columns()
                .iter()
                .zip(fields)
                .map(|(array, field)| expand_export_array(array, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(
                StructArray::try_new(fields.clone(), columns, array.nulls().cloned())
                    .context(error::ComputeArrowSnafu)?,
            ))
        }
        _ => cast(array, target).context(error::ComputeArrowSnafu),
    }
}

pub(crate) fn map_writer_error(
    source: common_datasource::error::Error,
    path: &str,
) -> error::Error {
    match source {
        common_datasource::error::Error::ParquetWriteCancelled {} => {
            error::LogicalTableExportCancelledSnafu.build()
        }
        common_datasource::error::Error::InvalidParquetWriterLimits {} => {
            InvalidLogicalTableExportSnafu {
                reason: "Parquet writer limits must be positive",
            }
            .build()
        }
        common_datasource::error::Error::ParquetWriterResource { reason } => {
            LogicalTableExportResourceSnafu { reason }.build()
        }
        source => error::WriteStreamToFileSnafu { path }.into_error(source),
    }
}

// Count only selected logical values before dictionary expansion, including nested
// histogram lists/structs. Offset and validity overhead is charged per value.
fn estimate_value_size(array: &dyn Array, row: usize) -> Result<usize> {
    if array.is_null(row) && !matches!(array.data_type(), DataType::Struct(_) | DataType::List(_)) {
        return Ok(32);
    }
    let bytes = match array.data_type() {
        DataType::Boolean => 1,
        DataType::Null => 0,
        DataType::Utf8 => array.as_string::<i32>().value(row).len(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).len(),
        DataType::Binary => array.as_binary::<i32>().value(row).len(),
        DataType::LargeBinary => array.as_binary::<i64>().value(row).len(),
        DataType::Struct(_) => {
            array
                .as_struct()
                .columns()
                .iter()
                .try_fold(0usize, |sum, child| {
                    Ok::<_, error::Error>(
                        sum.saturating_add(estimate_value_size(child.as_ref(), row)?),
                    )
                })?
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (offsets[row] as usize..offsets[row + 1] as usize).try_fold(0usize, |sum, index| {
                Ok::<_, error::Error>(
                    sum.saturating_add(estimate_value_size(list.values().as_ref(), index)?),
                )
            })?
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array! {
                array => {
                    match array.key(row) {
                        Some(index) => estimate_value_size(array.values().as_ref(), index)?,
                        None => 0,
                    }
                },
                _ => return error::UnexpectedSnafu { violated: "invalid dictionary array" }.fail(),
            }
        }
        other => other
            .primitive_width()
            .with_context(|| InvalidLogicalTableExportSnafu {
                reason: format!("unsupported Metric Parquet type: {other}"),
            })?,
    };
    Ok(bytes.saturating_add(16))
}

pub(crate) fn rows_within_budget(
    batch: &RecordBatch,
    start: usize,
    end: usize,
    budget: usize,
) -> Result<(usize, usize)> {
    let mut bytes = 0usize;
    let mut row = start;
    while row < end {
        let row_bytes = batch.columns().iter().try_fold(0usize, |sum, array| {
            Ok::<_, error::Error>(sum.saturating_add(estimate_value_size(array.as_ref(), row)?))
        })?;
        if row_bytes > budget.saturating_sub(bytes) {
            break;
        }
        bytes += row_bytes;
        row += 1;
    }
    ensure!(
        row > start,
        LogicalTableExportResourceSnafu {
            reason: "one expanded logical row exceeds conversion byte budget"
        }
    );
    Ok((row - start, bytes))
}

#[cfg(test)]
mod tests;

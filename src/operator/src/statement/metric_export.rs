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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{Array, AsArray, UInt32Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;
use common_datasource::object_store::build_backend_for_write;
use common_query::OutputData;
use common_recordbatch::SendableRecordBatchStream;
use common_time::range::TimestampRange;
use datafusion::datasource::DefaultTableSource;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion_common::TableReference as DfTableReference;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col};
use futures::StreamExt;
use object_store::ObjectStore;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME as TABLE_ID, DATA_SCHEMA_TSID_COLUMN_NAME as TSID,
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use table::TableRef;
use table::table::adapter::DfTableProviderAdapter;
use tokio_util::sync::CancellationToken;

use crate::error::{self, InvalidMetricExportSnafu, MetricExportResourceSnafu, Result};
use crate::statement::StatementExecutor;

/// Writer-side limits. Query memory and spill remain governed by the query engine.
/// Batch checks happen after allocation; flush thresholds are not hard RSS caps.
#[derive(Clone, Copy, Debug)]
pub struct MetricExportLimits {
    /// Maximum retained scan batch before routing.
    pub input_batch_bytes: usize,
    /// Maximum estimated expanded logical values in one conversion slice.
    pub conversion_bytes: usize,
    /// Maximum rows per conversion slice and Parquet row group.
    pub row_group_rows: usize,
    /// Flush when buffered encoder memory or encoded size reaches this threshold.
    pub writer_bytes: usize,
    /// Maximum row groups retained in one file's footer metadata.
    pub max_row_groups: usize,
}

impl Default for MetricExportLimits {
    fn default() -> Self {
        Self {
            input_batch_bytes: 64 * 1024 * 1024,
            conversion_bytes: 1024 * 1024,
            row_group_rows: 8192,
            writer_bytes: 8 * 1024 * 1024,
            max_row_groups: 4096,
        }
    }
}

impl MetricExportLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            self.input_batch_bytes > 0
                && self.conversion_bytes > 0
                && self.row_group_rows > 0
                && self.writer_bytes > 0
                && self.max_row_groups > 0,
            InvalidMetricExportSnafu {
                reason: "export limits must be positive"
            }
        );
        Ok(())
    }
}

/// Metadata captured for one physical table and its selected logical tables.
/// Construct one unit per physical table, schema and time chunk. Capturing these
/// references supplies no snapshot isolation or locking guarantee.
pub struct MetricExportUnit {
    physical: TableRef,
    projection: Vec<usize>,
    logical: BTreeMap<u32, LogicalFile>,
}

struct LogicalFile {
    name: String,
    schema: SchemaRef,
    projection: Vec<usize>,
}

impl MetricExportUnit {
    /// Validate associations and capture schemas from already selected table references.
    pub fn try_new(physical: TableRef, tables: &[TableRef]) -> Result<Self> {
        let physical_info = physical.table_info();
        ensure!(
            physical_info.meta.engine == METRIC_ENGINE_NAME
                && physical_info
                    .meta
                    .options
                    .extra_options
                    .contains_key(PHYSICAL_TABLE_METADATA_KEY)
                && !tables.is_empty(),
            InvalidMetricExportSnafu {
                reason: "expected a Metric physical table and selected logical tables"
            }
        );
        let physical_schema = physical.schema();
        let id_index =
            physical_schema
                .column_index_by_name(TABLE_ID)
                .context(InvalidMetricExportSnafu {
                    reason: "physical schema has no __table_id",
                })?;
        let mut projection = BTreeSet::from([id_index]);
        let mut logical = BTreeMap::new();
        for table in tables {
            let info = table.table_info();
            let name = &info.name;
            ensure!(
                !name.contains('/') && !name.contains('\\'),
                InvalidMetricExportSnafu {
                    reason: "logical table names must not contain path separators"
                }
            );
            ensure!(
                info.catalog_name == physical_info.catalog_name
                    && info.schema_name == physical_info.schema_name
                    && info.meta.engine == METRIC_ENGINE_NAME
                    && info
                        .meta
                        .options
                        .extra_options
                        .get(LOGICAL_TABLE_METADATA_KEY)
                        == Some(&physical_info.name),
                InvalidMetricExportSnafu {
                    reason: format!("{name} does not belong to the physical table")
                }
            );
            let schema = table.schema().arrow_schema().clone();
            let indices = schema
                .fields()
                .iter()
                .map(|field| {
                    ensure!(
                        field.name() != TABLE_ID && field.name() != TSID,
                        InvalidMetricExportSnafu {
                            reason: "logical schema contains internal Metric columns"
                        }
                    );
                    let index = physical_schema
                        .column_index_by_name(field.name())
                        .with_context(|| InvalidMetricExportSnafu {
                            reason: format!("physical schema lacks {}", field.name()),
                        })?;
                    ensure!(
                        physical_schema.arrow_schema().field(index).data_type()
                            == field.data_type(),
                        InvalidMetricExportSnafu {
                            reason: format!("physical/logical type mismatch for {}", field.name())
                        }
                    );
                    Ok(index)
                })
                .collect::<Result<Vec<_>>>()?;
            projection.extend(indices.iter().copied());
            ensure!(
                logical
                    .insert(
                        info.table_id(),
                        LogicalFile {
                            name: name.clone(),
                            schema,
                            projection: indices,
                        }
                    )
                    .is_none(),
                InvalidMetricExportSnafu {
                    reason: "duplicate logical table"
                }
            );
        }
        let projection = projection.into_iter().collect::<Vec<_>>();
        for file in logical.values_mut() {
            for index in &mut file.projection {
                *index = projection.binary_search(index).map_err(|_| {
                    error::UnexpectedSnafu {
                        violated: "logical column missing from physical projection",
                    }
                    .build()
                })?;
            }
        }
        Ok(Self {
            physical,
            projection,
            logical,
        })
    }

    fn plan(&self, time_range: Option<&TimestampRange>) -> Result<LogicalPlan> {
        let info = self.physical.table_info();
        let filters = self
            .physical
            .schema()
            .timestamp_column()
            .and_then(|column| {
                common_query::logical_plan::build_filter_from_timestamp(&column.name, time_range)
            })
            .into_iter()
            .collect::<Vec<_>>();
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(self.physical.clone()),
        )));
        let mut builder = LogicalPlanBuilder::scan_with_filters(
            DfTableReference::full(
                info.catalog_name.clone(),
                info.schema_name.clone(),
                info.name.clone(),
            ),
            source,
            Some(self.projection.clone()),
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
pub struct MetricExportSummary {
    pub rows: usize,
    pub skipped_rows: usize,
    pub files: usize,
}

impl StatementExecutor {
    /// Export an authorized unit to a fresh directory using one physical query.
    /// The caller bounds concurrent units and owns retry/cleanup of this directory.
    /// Cancellation aborts the active upload; previously closed files remain an
    /// incomplete chunk until the caller publishes its completion state.
    #[allow(clippy::too_many_arguments)]
    pub async fn export_metric_unit(
        &self,
        unit: &MetricExportUnit,
        directory: &str,
        connection: &HashMap<String, String>,
        time_range: Option<&TimestampRange>,
        limits: MetricExportLimits,
        cancellation: &CancellationToken,
        query_ctx: QueryContextRef,
    ) -> Result<MetricExportSummary> {
        limits.validate()?;
        let (store, stream) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return error::MetricExportCancelledSnafu.fail(),
            result = async {
                let store = build_backend_for_write(&format!("{}/", directory.trim_end_matches('/')), connection, &self.local_file_access)
                    .await.context(error::BuildBackendSnafu)?;
                let output = self.query_engine.execute(unit.plan(time_range)?, query_ctx)
                    .await.context(error::ExecLogicalPlanSnafu)?;
                let stream = match output.data {
                    OutputData::Stream(stream) => stream,
                    OutputData::RecordBatches(batches) => batches.as_stream(),
                    _ => return error::UnexpectedSnafu { violated: "expected physical query rows" }.fail(),
                };
                Ok((store, stream))
            } => result?,
        };
        export_cancellable_stream(unit, stream, &store, limits, cancellation).await
    }
}

async fn export_cancellable_stream(
    unit: &MetricExportUnit,
    stream: SendableRecordBatchStream,
    store: &ObjectStore,
    limits: MetricExportLimits,
    cancellation: &CancellationToken,
) -> Result<MetricExportSummary> {
    let mut active = None;
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => error::MetricExportCancelledSnafu.fail(),
        result = export_stream(unit, stream, store, limits, &mut active) => result,
    };
    if result.is_err() {
        if let Some(mut writer) = active {
            let aborted = writer.sink.abort().await;
            if aborted
                .as_ref()
                .is_err_and(|e| e.kind() == object_store::ErrorKind::Unsupported)
            {
                // Secure filesystem writes are not atomic and cannot abort. Drop
                // the handle before removing this attempt's unfinished file.
                let path = writer.path.clone();
                drop(writer);
                store
                    .delete(&path)
                    .await
                    .context(common_datasource::error::WriteObjectSnafu { path: &path })
                    .context(error::WriteStreamToFileSnafu { path: &path })?;
            } else {
                aborted
                    .context(common_datasource::error::WriteObjectSnafu { path: &writer.path })
                    .context(error::WriteStreamToFileSnafu { path: &writer.path })?;
            }
        }
    }
    result
}

async fn export_stream(
    unit: &MetricExportUnit,
    mut stream: SendableRecordBatchStream,
    store: &ObjectStore,
    limits: MetricExportLimits,
    active: &mut Option<LogicalWriter>,
) -> Result<MetricExportSummary> {
    let id_index =
        stream
            .schema()
            .column_index_by_name(TABLE_ID)
            .context(error::UnexpectedSnafu {
                violated: "physical query omitted __table_id",
            })?;
    let mut summary = MetricExportSummary::default();
    let mut previous = None;
    let mut written = BTreeSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch
            .context(error::BuildRecordBatchSnafu)?
            .into_df_record_batch();
        ensure!(
            batch.get_array_memory_size() <= limits.input_batch_bytes,
            MetricExportResourceSnafu {
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
            if active.as_ref().is_some_and(|writer| writer.id != id) {
                finish_active(active).await?;
            }
            if let Some(file) = unit.logical.get(&id) {
                if active.is_none() {
                    *active = Some(LogicalWriter::open(id, file, store, limits).await?);
                    written.insert(id);
                    summary.files += 1;
                }
                let projected = batch
                    .project(&file.projection)
                    .context(error::ProjectSchemaSnafu)?;
                let writer = active.as_mut().context(error::UnexpectedSnafu {
                    violated: "missing logical writer",
                })?;
                let mut offset = start;
                while offset < end {
                    let (encoder, consumed, bytes) = encode_slice(
                        writer.encoder.take().context(error::UnexpectedSnafu {
                            violated: "missing Parquet encoder",
                        })?,
                        projected.clone(),
                        file.schema.clone(),
                        offset,
                        end,
                        limits,
                    )
                    .await?;
                    writer.encoder = Some(encoder);
                    writer.write(bytes).await?;
                    offset += consumed;
                    summary.rows += consumed;
                }
            } else {
                summary.skipped_rows += end - start;
            }
            start = end;
        }
    }
    finish_active(active).await?;
    for (&id, file) in &unit.logical {
        if !written.contains(&id) {
            *active = Some(LogicalWriter::open(id, file, store, limits).await?);
            finish_active(active).await?;
            summary.files += 1;
        }
    }
    Ok(summary)
}

type Encoder = ArrowWriter<Vec<u8>>;

struct LogicalWriter {
    id: u32,
    path: String,
    encoder: Option<Encoder>,
    sink: object_store::Writer,
}

impl LogicalWriter {
    async fn open(
        id: u32,
        file: &LogicalFile,
        store: &ObjectStore,
        limits: MetricExportLimits,
    ) -> Result<Self> {
        let path = format!("{}.parquet", file.name);
        ensure!(
            !store
                .exists(&path)
                .await
                .context(error::ReadObjectSnafu { path: &path })?,
            InvalidMetricExportSnafu {
                reason: format!("output already exists: {path}")
            }
        );
        let mut properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(limits.row_group_rows))
            .set_max_row_group_bytes(None)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_truncate_length(None)
            .set_column_index_truncate_length(None);
        for field in file.schema.fields() {
            if matches!(field.data_type(), DataType::Timestamp(_, _)) {
                let column = ColumnPath::new(vec![field.name().clone()]);
                properties = properties
                    .set_column_dictionary_enabled(column.clone(), false)
                    .set_column_encoding(column, Encoding::DELTA_BINARY_PACKED);
            }
        }
        let encoder = parquet_result(
            ArrowWriter::try_new(Vec::new(), file.schema.clone(), Some(properties.build())),
            &path,
        )?;
        let sink = store
            .writer_with(&path)
            .concurrent(1)
            .chunk(8 * 1024 * 1024)
            .await
            .context(common_datasource::error::WriteObjectSnafu { path: &path })
            .context(error::WriteStreamToFileSnafu { path: &path })?;
        Ok(Self {
            id,
            path,
            encoder: Some(encoder),
            sink,
        })
    }

    async fn write(&mut self, bytes: Vec<u8>) -> Result<()> {
        if !bytes.is_empty() {
            self.sink
                .write(bytes)
                .await
                .context(common_datasource::error::WriteObjectSnafu { path: &self.path })
                .context(error::WriteStreamToFileSnafu { path: &self.path })?;
        }
        Ok(())
    }
}

async fn finish_active(active: &mut Option<LogicalWriter>) -> Result<()> {
    if let Some(writer) = active.as_mut() {
        let mut encoder = writer.encoder.take().context(error::UnexpectedSnafu {
            violated: "missing Parquet encoder",
        })?;
        let path = writer.path.clone();
        let bytes = common_runtime::spawn_blocking_global(move || {
            parquet_result(encoder.finish(), &path)?;
            Ok::<_, error::Error>(std::mem::take(encoder.inner_mut()))
        })
        .await
        .context(error::JoinTaskSnafu)??;
        writer.write(bytes).await?;
        writer
            .sink
            .close()
            .await
            .context(common_datasource::error::WriteObjectSnafu { path: &writer.path })
            .context(error::WriteStreamToFileSnafu { path: &writer.path })?;
        *active = None;
    }
    Ok(())
}

async fn encode_slice(
    mut encoder: Encoder,
    batch: RecordBatch,
    schema: SchemaRef,
    start: usize,
    end: usize,
    limits: MetricExportLimits,
) -> Result<(Encoder, usize, Vec<u8>)> {
    common_runtime::spawn_blocking_global(move || {
        ensure!(
            encoder.flushed_row_groups().len() < limits.max_row_groups,
            MetricExportResourceSnafu {
                reason: "Parquet row-group metadata budget exceeded"
            }
        );
        let end = end.min(start.saturating_add(limits.row_group_rows - encoder.in_progress_rows()));
        let len = bounded_slice_len(&batch, start, end, limits.conversion_bytes)?;
        let slice = batch.slice(start, len);
        let arrays = slice
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(array, field)| cast(array, field.data_type()).context(error::ComputeArrowSnafu))
            .collect::<Result<Vec<_>>>()?;
        let expanded = RecordBatch::try_new(schema, arrays).context(error::ComputeArrowSnafu)?;
        parquet_result(encoder.write(&expanded), "logical Parquet file")?;
        if encoder.memory_size() >= limits.writer_bytes
            || encoder.in_progress_size() >= limits.writer_bytes
        {
            parquet_result(encoder.flush(), "logical Parquet file")?;
        }
        // Draining the backing Vec preserves ArrowWriter's tracked byte offsets.
        let bytes = std::mem::take(encoder.inner_mut());
        Ok((encoder, len, bytes))
    })
    .await
    .context(error::JoinTaskSnafu)?
}

fn parquet_result<T>(result: datafusion::parquet::errors::Result<T>, path: &str) -> Result<T> {
    result
        .context(common_datasource::error::WriteParquetSnafu { path })
        .context(error::WriteStreamToFileSnafu { path })
}

// Count only selected logical values before dictionary expansion, including nested
// histogram lists/structs. Offset and validity overhead is charged per value.
fn value_bytes(array: &dyn Array, row: usize) -> Result<usize> {
    if array.is_null(row) {
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
                    Ok::<_, error::Error>(sum.saturating_add(value_bytes(child.as_ref(), row)?))
                })?
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (offsets[row] as usize..offsets[row + 1] as usize).try_fold(0usize, |sum, index| {
                Ok::<_, error::Error>(
                    sum.saturating_add(value_bytes(list.values().as_ref(), index)?),
                )
            })?
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array! {
                array => {
                    match array.key(row) {
                        Some(index) => value_bytes(array.values().as_ref(), index)?,
                        None => 0,
                    }
                },
                _ => return error::UnexpectedSnafu { violated: "invalid dictionary array" }.fail(),
            }
        }
        other => other.primitive_width().context(InvalidMetricExportSnafu {
            reason: format!("unsupported Metric Parquet type: {other}"),
        })?,
    };
    Ok(bytes.saturating_add(16))
}

fn bounded_slice_len(
    batch: &RecordBatch,
    start: usize,
    end: usize,
    budget: usize,
) -> Result<usize> {
    let mut bytes = 0usize;
    let mut row = start;
    while row < end {
        let row_bytes = batch.columns().iter().try_fold(0usize, |sum, array| {
            Ok::<_, error::Error>(sum.saturating_add(value_bytes(array.as_ref(), row)?))
        })?;
        if row_bytes > budget.saturating_sub(bytes) {
            break;
        }
        bytes += row_bytes;
        row += 1;
    }
    ensure!(
        row > start,
        MetricExportResourceSnafu {
            reason: "one expanded logical row exceeds conversion byte budget"
        }
    );
    Ok(row - start)
}

#[cfg(test)]
mod tests;

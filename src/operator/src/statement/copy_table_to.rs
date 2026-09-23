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

use arrow::array::{Array, ArrayRef, AsArray, UInt64Array, make_array};
use arrow::datatypes::DataType;
use client::OutputData;
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::Format;
use common_datasource::file_format::csv::stream_to_csv;
use common_datasource::file_format::json::stream_to_json;
use common_datasource::file_format::parquet::stream_to_parquet;
use common_datasource::object_store::build_backend_for_write_with_path;
use common_datasource::parquet_writer::ParquetFileWriter;
use common_query::Output;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::{
    SendableRecordBatchMapper, SendableRecordBatchStream, map_json_type_to_string,
    map_json_type_to_string_schema,
};
use common_telemetry::{debug, tracing};
use datafusion::datasource::DefaultTableSource;
use datafusion_common::TableReference as DfTableReference;
use datafusion_expr::LogicalPlanBuilder;
use futures::StreamExt;
use object_store::ObjectStore;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use table::TableRef;
use table::requests::CopyTableRequest;
use table::table::adapter::DfTableProviderAdapter;
use table::table_reference::TableReference;
use tokio_util::sync::CancellationToken;

use crate::error::{self, BuildDfLogicalPlanSnafu, ExecLogicalPlanSnafu, Result};
use crate::statement::StatementExecutor;
use crate::statement::export_logical_tables::writers::ExportWriteBudget;
use crate::statement::export_logical_tables::{
    expand_export_batch, map_writer_error, rows_within_budget,
};

// The buffer size should be greater than 5MB (minimum multipart upload size).
/// Buffer size to flush data to object stores.
const WRITE_BUFFER_THRESHOLD: ReadableSize = ReadableSize::mb(8);

/// Default number of concurrent write, it only works on object store backend(e.g., S3).
const WRITE_CONCURRENCY: usize = 8;

impl StatementExecutor {
    async fn stream_to_file(
        &self,
        stream: SendableRecordBatchStream,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<usize> {
        let threshold = WRITE_BUFFER_THRESHOLD.as_bytes() as usize;

        let stream = Box::pin(SendableRecordBatchMapper::new(
            stream,
            map_json_type_to_string,
            map_json_type_to_string_schema,
        ));
        match format {
            Format::Csv(format) => stream_to_csv(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
                format,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Json(format) => stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
                format,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Parquet(_) => stream_to_parquet(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                WRITE_CONCURRENCY,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            _ => error::UnsupportedFormatSnafu {
                format: format.clone(),
            }
            .fail(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_table_to(
        &self,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let table_ref = TableReference::full(&req.catalog_name, &req.schema_name, &req.table_name);
        let table = self.get_table(&table_ref).await?;
        self.copy_captured_table_to(table, req, query_ctx).await
    }

    pub(crate) async fn copy_captured_table_to(
        &self,
        table: TableRef,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        self.copy_captured_table_to_managed(table, req, query_ctx, None)
            .await
    }

    pub(crate) async fn copy_captured_table_to_managed(
        &self,
        table: TableRef,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
        managed: Option<(&ExportWriteBudget, &CancellationToken)>,
    ) -> Result<usize> {
        let info = table.table_info();
        let table_ref = TableReference::full(&info.catalog_name, &info.schema_name, &info.name);
        let table_id = info.table_id();
        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;

        let df_table_ref = DfTableReference::from(table_ref);

        let filters = table
            .schema()
            .timestamp_column()
            .and_then(|c| {
                common_query::logical_plan::build_filter_from_timestamp(
                    &c.name,
                    req.timestamp_range.as_ref(),
                )
            })
            .into_iter()
            .collect::<Vec<_>>();

        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            df_table_ref,
            table_source,
            None,
            filters.clone(),
        )
        .context(BuildDfLogicalPlanSnafu)?;
        for f in filters {
            builder = builder.filter(f).context(BuildDfLogicalPlanSnafu)?;
        }
        let plan = builder.build().context(BuildDfLogicalPlanSnafu)?;

        let output = self
            .query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)?;

        let CopyTableRequest {
            location,
            connection,
            ..
        } = &req;

        debug!("Copy table: {table_id} to location: {location}");
        self.copy_to_file_managed(&format, output, location, connection, managed)
            .await
    }

    pub(crate) async fn copy_to_file(
        &self,
        format: &Format,
        output: Output,
        location: &str,
        connection: &HashMap<String, String>,
    ) -> Result<usize> {
        self.copy_to_file_managed(format, output, location, connection, None)
            .await
    }

    async fn copy_to_file_managed(
        &self,
        format: &Format,
        output: Output,
        location: &str,
        connection: &HashMap<String, String>,
        managed: Option<(&ExportWriteBudget, &CancellationToken)>,
    ) -> Result<usize> {
        let output = if managed.is_none() {
            output
                .map_dictionary_to_values()
                .context(error::BuildRecordBatchSnafu)?
        } else {
            output
        };
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        let backend =
            build_backend_for_write_with_path(location, connection, &self.local_file_access)
                .await
                .context(error::BuildBackendSnafu)?;
        let filename = backend.object_path.context(error::UnexpectedSnafu {
            violated: format!("Expected filename, path: {location}"),
        })?;
        if let Some((budget, token)) = managed {
            stream_to_managed_parquet(stream, backend.object_store, &filename, budget, token).await
        } else {
            self.stream_to_file(stream, format, backend.object_store, &filename)
                .await
        }
    }
}

pub(crate) async fn stream_to_managed_parquet(
    mut stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    budget: &ExportWriteBudget,
    token: &CancellationToken,
) -> Result<usize> {
    use common_recordbatch::{RecordBatch, map_dictionary_to_values_schema};
    let original = stream.schema();
    let (expanded_schema, expand) = map_dictionary_to_values_schema(original.clone());
    let json_columns = expanded_schema
        .column_schemas()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            (column.data_type.is_json() && !column.data_type.is_json2()).then_some(index)
        })
        .collect::<Vec<_>>();
    let (mapped_schema, json) = map_json_type_to_string_schema(expanded_schema.clone());
    let output_schema = if json {
        mapped_schema
    } else {
        expanded_schema.clone()
    };
    let mut writer =
        ParquetFileWriter::open(output_schema.arrow_schema().clone(), store, path, 1, None)
            .await
            .context(error::WriteStreamToFileSnafu { path })?;
    let mut started = false;
    let result = async {
        let mut rows = 0;
        loop {
            let batch = tokio::select! {
                biased;
                _ = token.cancelled() => return error::LogicalTableExportCancelledSnafu.fail(),
                batch = stream.next() => batch,
            };
            let Some(batch) = batch else { break };
            let batch = batch
                .context(error::BuildRecordBatchSnafu)?
                .into_df_record_batch();
            let mut offset = 0;
            while offset < batch.num_rows() {
                // The scan batch remains query-owned. Bound the downstream slice and
                // detach its buffers below if it would retain more than its reservation.
                let (conversion, retained) =
                    ExportWriteBudget::conversion_budget(0, batch.num_columns(), usize::MAX)?;
                let input = batch.clone();
                let json_columns = json_columns.clone();
                let (len, estimated) = common_runtime::spawn_blocking_global(move || {
                    rows_within_budget(&input, offset, input.num_rows(), conversion, &json_columns)
                })
                .await
                .context(error::JoinTaskSnafu)??;
                let reservation = retained.saturating_add(estimated.saturating_mul(4));
                let permit = budget.reserve(reservation, token).await?;
                let batch = batch.clone();
                let (original, expanded_schema, output_schema) = (
                    original.clone(),
                    expanded_schema.clone(),
                    output_schema.clone(),
                );
                let (batch, len) = common_runtime::spawn_blocking_global(move || {
                    let mut batch = RecordBatch::from_df_record_batch(
                        original.clone(),
                        batch.slice(offset, len),
                    );
                    if expand {
                        batch = RecordBatch::from_df_record_batch(
                            expanded_schema.clone(),
                            expand_export_batch(
                                &batch.into_df_record_batch(),
                                expanded_schema.arrow_schema().clone(),
                            )?,
                        );
                    }
                    if json {
                        batch = map_json_type_to_string(batch, &expanded_schema, &output_schema)
                            .context(error::BuildRecordBatchSnafu)?;
                    }
                    let mut batch = batch.into_df_record_batch();
                    if batch.get_array_memory_size() > reservation {
                        let indices = UInt64Array::from_iter_values(0..batch.num_rows() as u64);
                        batch = arrow::compute::take_record_batch(&batch, &indices)
                            .context(error::ComputeArrowSnafu)?;
                        let arrays = batch
                            .columns()
                            .iter()
                            .map(compact_view_buffers)
                            .collect::<Result<Vec<_>>>()?;
                        batch = arrow::record_batch::RecordBatch::try_new(batch.schema(), arrays)
                            .context(error::ComputeArrowSnafu)?;
                    }
                    ensure!(
                        batch.get_array_memory_size() <= reservation,
                        error::LogicalTableExportResourceSnafu {
                            reason: "converted backing buffers exceed reservation"
                        }
                    );
                    Ok::<_, error::Error>((batch, len))
                })
                .await
                .context(error::JoinTaskSnafu)??;
                started = true;
                let write = writer.write(batch, Some(token)).await;
                drop(permit);
                write.map_err(|error| map_writer_error(error, path))?;
                rows += len;
                offset += len;
            }
        }
        started = true;
        writer
            .finish(Some(token))
            .await
            .map_err(|error| map_writer_error(error, path))?;
        Ok(rows)
    }
    .await;
    if result.is_err() {
        token.cancel();
        if started && let Err(error) = writer.abort().await {
            common_telemetry::warn!(error; "Failed to abort ordinary export file");
        }
    }
    result
}

// Arrow take copies ordinary buffers but shares view data buffers, including
// views nested inside lists or structs. Reclaim those unselected values too.
fn compact_view_buffers(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => Ok(Arc::new(array.as_string_view().gc())),
        DataType::BinaryView => Ok(Arc::new(array.as_binary_view().gc())),
        _ => {
            let data = array.to_data();
            if data.child_data().is_empty() {
                return Ok(array.clone());
            }
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    compact_view_buffers(&make_array(child.clone())).map(|array| array.to_data())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(make_array(
                data.into_builder()
                    .child_data(children)
                    .build()
                    .context(error::ComputeArrowSnafu)?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, StringArray,
        StringViewArray,
    };
    use arrow::datatypes::Int32Type;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};

    use super::*;

    #[tokio::test]
    async fn managed_copy_preserves_existing_file_before_sink_open() {
        let temp_dir = common_test_util::temp_dir::create_temp_dir("managed_copy_existing_file");
        let store = object_store::secure_fs::SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        let path = "existing.parquet";
        store.write(path, "original").await.unwrap();
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "value",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.arrow_schema().clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let batches = async_stream::stream! {
            yield Ok(batch);
            yield Err(datafusion::error::DataFusionError::Execution("source failed".into()));
        };
        let stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema.arrow_schema().clone(),
            batches,
        );
        let stream =
            common_recordbatch::adapter::RecordBatchStreamAdapter::try_new(Box::pin(stream))
                .unwrap();

        let result = stream_to_managed_parquet(
            Box::pin(stream),
            store.clone(),
            path,
            &ExportWriteBudget::new(1),
            &CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            store.read(path).await.unwrap().to_bytes().as_ref(),
            b"original"
        );
    }

    #[tokio::test]
    async fn managed_copy_rechunks_large_backing_buffers() {
        let value = "x".repeat(9 * 1024);
        for view in [false, true] {
            let data_type = if view {
                ConcreteDataType::utf8_view_datatype()
            } else {
                ConcreteDataType::string_datatype()
            };
            let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
                "value", data_type, false,
            )]));
            let values = vec![value.as_str(); 8192];
            let array: ArrayRef = if view {
                Arc::new(StringViewArray::from(values))
            } else {
                Arc::new(StringArray::from(values))
            };
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema().clone(),
                vec![array],
            )
            .unwrap();
            assert!(batch.get_array_memory_size() > 64 * 1024 * 1024);
            // Also exercise a small slice that still references the large allocation.
            for input in [batch.clone(), batch.slice(200, 3)] {
                let rows = input.num_rows();
                let batches = RecordBatches::try_new(
                    schema.clone(),
                    vec![RecordBatch::from_df_record_batch(schema.clone(), input)],
                )
                .unwrap();
                let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
                let budget = ExportWriteBudget::new(1);
                assert_eq!(
                    stream_to_managed_parquet(
                        batches.as_stream(),
                        store.clone(),
                        "large.parquet",
                        &budget,
                        &CancellationToken::new(),
                    )
                    .await
                    .unwrap(),
                    rows
                );
                let reader = ParquetRecordBatchReaderBuilder::try_new(
                    store.read("large.parquet").await.unwrap().to_bytes(),
                )
                .unwrap()
                .build()
                .unwrap();
                let mut actual_rows = 0;
                for batch in reader {
                    let batch = batch.unwrap();
                    let strings = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
                    assert!(
                        strings
                            .as_string::<i32>()
                            .iter()
                            .all(|item| item == Some(value.as_str()))
                    );
                    actual_rows += batch.num_rows();
                }
                assert_eq!(actual_rows, rows);
                assert_eq!(budget.available(), (1, 64 * 1024 * 1024));
            }
        }
    }

    #[tokio::test]
    async fn managed_copy_preserves_dictionary_json_views_and_empty_schema() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "host",
                ConcreteDataType::dictionary_datatype(
                    ConcreteDataType::int32_datatype(),
                    ConcreteDataType::string_datatype(),
                ),
                true,
            ),
            ColumnSchema::new("json", ConcreteDataType::json_datatype(), true),
            ColumnSchema::new("text_view", ConcreteDataType::utf8_view_datatype(), true),
            ColumnSchema::new(
                "binary_view",
                ConcreteDataType::binary_view_datatype(),
                true,
            ),
        ]));
        let dictionary = DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(1)]),
            Arc::new(StringArray::from(vec!["tag", &"x".repeat(3 * 1024 * 1024)])),
        );
        let json = datatypes::types::parse_string_to_jsonb(&format!(
            r#"{{"value":"escaped\ntext{}","n":123456789}}"#,
            "y".repeat(160_000)
        ))
        .unwrap();
        let arrays = vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(BinaryArray::from(vec![
                Some(json.as_slice()),
                None,
                None,
                Some(json.as_slice()),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("short"),
                None,
                Some("long view"),
                Some("long view"),
            ])),
            Arc::new(BinaryViewArray::from(vec![
                Some(&b"short"[..]),
                None,
                Some(&b"long view"[..]),
                Some(&b"long view"[..]),
            ])),
        ];
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.arrow_schema().clone(), arrays)
                .unwrap();
        let batch = RecordBatch::from_df_record_batch(schema.clone(), batch);
        for empty in [false, true] {
            let batches = RecordBatches::try_new(
                schema.clone(),
                if empty { vec![] } else { vec![batch.clone()] },
            )
            .unwrap();
            let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
            let budget = ExportWriteBudget::new(1);
            let token = CancellationToken::new();
            let managed = stream_to_managed_parquet(
                batches.as_stream(),
                store.clone(),
                "managed.parquet",
                &budget,
                &token,
            )
            .await
            .unwrap();
            let output = Output::new_with_record_batches(batches)
                .map_dictionary_to_values()
                .unwrap();
            let OutputData::RecordBatches(batches) = output.data else {
                panic!("expected batches");
            };
            let stream = SendableRecordBatchMapper::new(
                batches.as_stream(),
                map_json_type_to_string,
                map_json_type_to_string_schema,
            );
            let ordinary = stream_to_parquet(
                Box::pin(DfRecordBatchStreamAdapter::new(Box::pin(stream))),
                store.clone(),
                "ordinary.parquet",
                WRITE_CONCURRENCY,
            )
            .await
            .unwrap();
            assert_eq!(managed, ordinary);
            let mut outputs = Vec::new();
            for name in ["managed.parquet", "ordinary.parquet"] {
                let reader = ParquetRecordBatchReaderBuilder::try_new(
                    store.read(name).await.unwrap().to_bytes(),
                )
                .unwrap();
                let schema = reader.schema().clone();
                let values = reader
                    .build()
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
                outputs.push((schema, values));
            }
            assert_eq!(outputs[0], outputs[1]);
            assert_eq!(budget.available(), (1, 64 * 1024 * 1024));
        }
    }
}

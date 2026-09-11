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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use client::{Output, OutputData, OutputMeta};
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::{
    CsvFormat, is_skippable_arrow_error, tolerant_csv_stream,
};
use common_datasource::file_format::json::JsonFormat;
use common_datasource::file_format::orc::{ReaderAdapter, infer_orc_schema, new_orc_stream_reader};
use common_datasource::file_format::{FileFormat, Format, file_to_stream};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{LocalFileAccess, build_backend_with_path};
use common_query::{OutputCost, OutputRows};
use common_recordbatch::DfSendableRecordBatchStream;
use common_recordbatch::adapter::RecordBatchStreamTypeAdapter;
use common_telemetry::{debug, tracing};
use datafusion::datasource::physical_plan::{CsvSource, FileSource, JsonSource};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::error::ArrowError;
use datafusion_common::config::CsvOptions;
use datafusion_expr::Expr;
use datatypes::arrow::compute::can_cast_types;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::Helper;
use futures_util::StreamExt;
use object_store::{EntryMode, ObjectStore};
use regex::Regex;
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use table::requests::{CopyTableRequest, InsertRequest};
use table::table_reference::TableReference;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::error::{self, IntoVectorsSnafu, Result};
use crate::statement::StatementExecutor;

const DEFAULT_BATCH_SIZE: usize = 8192;
const DEFAULT_READ_BUFFER: usize = 256 * 1024;

enum FileMetadata {
    Parquet {
        schema: SchemaRef,
        metadata: ArrowReaderMetadata,
        path: String,
    },
    Orc {
        schema: SchemaRef,
        path: String,
    },
    Json {
        schema: SchemaRef,
        format: JsonFormat,
        path: String,
    },
    Csv {
        schema: SchemaRef,
        format: CsvFormat,
        path: String,
    },
}

impl FileMetadata {
    /// Returns the [SchemaRef]
    pub fn schema(&self) -> &SchemaRef {
        match self {
            FileMetadata::Parquet { schema, .. } => schema,
            FileMetadata::Orc { schema, .. } => schema,
            FileMetadata::Json { schema, .. } => schema,
            FileMetadata::Csv { schema, .. } => schema,
        }
    }
}

async fn list_copy_from_paths(
    req: &CopyTableRequest,
    local_file_access: &LocalFileAccess,
) -> Result<(ObjectStore, Vec<String>)> {
    let backend = build_backend_with_path(&req.location, &req.connection, local_file_access)
        .await
        .context(error::BuildBackendSnafu)?;
    let regex = req
        .pattern
        .as_ref()
        .map(|x| Regex::new(x))
        .transpose()
        .context(error::BuildRegexSnafu)?;

    // A known file needs only stat. Listing its parent directory for every
    // table makes COPY DATABASE do quadratic directory work.
    if let Some(filename) = backend.object_path {
        let metadata = backend
            .object_store
            .stat(&filename)
            .await
            .with_context(|_| common_datasource::error::ListObjectsSnafu {
                path: req.location.clone(),
            })
            .context(error::ListObjectsSnafu)?;
        let paths = if metadata.mode() == EntryMode::FILE {
            vec![filename]
        } else {
            vec![]
        };
        return Ok((backend.object_store, paths));
    }

    let lister = Lister::new(
        backend.object_store.clone(),
        Source::Dir,
        req.location.clone(),
        regex,
    );

    let entries = lister.list().await.context(error::ListObjectsSnafu)?;
    debug!(
        "Copy from location: {:?}, entries: {entries:?}",
        req.location
    );
    let paths = entries
        .into_iter()
        .filter(|entry| entry.metadata().mode() == EntryMode::FILE)
        .map(|entry| entry.path().to_string())
        .collect();
    Ok((backend.object_store, paths))
}

impl StatementExecutor {
    async fn collect_metadata(
        &self,
        object_store: &ObjectStore,
        format: Format,
        path: String,
    ) -> Result<FileMetadata> {
        match format {
            Format::Csv(format) => Ok(FileMetadata::Csv {
                schema: Arc::new(
                    format
                        .infer_schema(object_store, &path)
                        .await
                        .context(error::InferSchemaSnafu { path: &path })?,
                ),
                format,
                path,
            }),
            Format::Json(format) => Ok(FileMetadata::Json {
                schema: Arc::new(
                    format
                        .infer_schema(object_store, &path)
                        .await
                        .context(error::InferSchemaSnafu { path: &path })?,
                ),
                format,
                path,
            }),
            Format::Parquet(_) => {
                let meta = object_store
                    .stat(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?;
                let mut reader = object_store
                    .reader(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?
                    .into_futures_async_read(0..meta.content_length())
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?
                    .compat();
                let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                    .await
                    .context(error::ReadParquetMetadataSnafu)?;

                Ok(FileMetadata::Parquet {
                    schema: metadata.schema().clone(),
                    metadata,
                    path,
                })
            }
            Format::Orc(_) => {
                let meta = object_store
                    .stat(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?;

                let reader = object_store
                    .reader(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?;

                let schema = infer_orc_schema(ReaderAdapter::new(reader, meta.content_length()))
                    .await
                    .context(error::ReadOrcSnafu)?;

                Ok(FileMetadata::Orc {
                    schema: Arc::new(schema),
                    path,
                })
            }
        }
    }

    async fn build_read_stream(
        &self,
        compat_schema: SchemaRef,
        object_store: &ObjectStore,
        file_metadata: &FileMetadata,
        projection: Vec<usize>,
        filters: Vec<Expr>,
    ) -> Result<DfSendableRecordBatchStream> {
        match file_metadata {
            FileMetadata::Csv {
                format,
                path,
                schema,
            } => {
                let output_schema = Arc::new(
                    compat_schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );

                let options = CsvOptions::default()
                    .with_has_header(format.has_header)
                    .with_delimiter(format.delimiter);
                let csv_source = CsvSource::new(schema.clone())
                    .with_csv_options(options)
                    .with_batch_size(DEFAULT_BATCH_SIZE);
                let stream = if format.skip_bad_records {
                    let reader_schema =
                        csv_reader_schema_for_skip_bad_records(schema, &compat_schema);
                    tolerant_csv_stream(
                        object_store,
                        path,
                        Arc::new(reader_schema),
                        projection.clone(),
                        format,
                    )
                    .await
                    .context(error::BuildFileStreamSnafu)?
                } else {
                    file_to_stream(
                        object_store,
                        path,
                        csv_source,
                        Some(projection),
                        format.compression_type,
                    )
                    .await
                    .context(error::BuildFileStreamSnafu)?
                };

                let stream = Box::pin(
                    // The projection is already applied in the CSV reader when we created the stream,
                    // so we pass None here to avoid double projection which would cause schema mismatch errors.
                    RecordBatchStreamTypeAdapter::new(output_schema, stream, None)
                        .with_filter(filters)
                        .context(error::PhysicalExprSnafu)?,
                );
                if format.skip_bad_records {
                    Ok(Box::pin(SkipBadRecordsStream::new(stream, path)))
                } else {
                    Ok(stream)
                }
            }
            FileMetadata::Json {
                path,
                format,
                schema,
            } => {
                let output_schema = Arc::new(
                    compat_schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );

                let json_source =
                    JsonSource::new(schema.clone()).with_batch_size(DEFAULT_BATCH_SIZE);
                let stream = file_to_stream(
                    object_store,
                    path,
                    json_source,
                    Some(projection),
                    format.compression_type,
                )
                .await
                .context(error::BuildFileStreamSnafu)?;

                Ok(Box::pin(
                    // The projection is already applied in the JSON reader when we created the stream,
                    // so we pass None here to avoid double projection which would cause schema mismatch errors.
                    RecordBatchStreamTypeAdapter::new(output_schema, stream, None)
                        .with_filter(filters)
                        .context(error::PhysicalExprSnafu)?,
                ))
            }
            FileMetadata::Parquet { metadata, path, .. } => {
                let meta = object_store
                    .stat(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;
                let reader = object_store
                    .reader_with(path)
                    .chunk(DEFAULT_READ_BUFFER)
                    .await
                    .context(error::ReadObjectSnafu { path })?
                    .into_futures_async_read(0..meta.content_length())
                    .await
                    .context(error::ReadObjectSnafu { path })?
                    .compat();
                let builder =
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata.clone());
                let stream = builder
                    .build()
                    .context(error::BuildParquetRecordBatchStreamSnafu)?;

                let output_schema = Arc::new(
                    compat_schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );
                Ok(Box::pin(
                    RecordBatchStreamTypeAdapter::new(output_schema, stream, Some(projection))
                        .with_filter(filters)
                        .context(error::PhysicalExprSnafu)?,
                ))
            }
            FileMetadata::Orc { path, .. } => {
                let meta = object_store
                    .stat(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;

                let reader = object_store
                    .reader_with(path)
                    .chunk(DEFAULT_READ_BUFFER)
                    .await
                    .context(error::ReadObjectSnafu { path })?;
                let stream =
                    new_orc_stream_reader(ReaderAdapter::new(reader, meta.content_length()))
                        .await
                        .context(error::ReadOrcSnafu)?;

                let output_schema = Arc::new(
                    compat_schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );

                Ok(Box::pin(
                    RecordBatchStreamTypeAdapter::new(output_schema, stream, Some(projection))
                        .with_filter(filters)
                        .context(error::PhysicalExprSnafu)?,
                ))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn copy_table_from(
        &self,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref).await?;
        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;
        let (object_store, paths) = list_copy_from_paths(&req, &self.local_file_access).await?;
        let mut files = Vec::with_capacity(paths.len());
        let table_schema = table.schema().arrow_schema().clone();
        let filters = table
            .schema()
            .timestamp_column()
            .and_then(|c| {
                common_query::logical_plan::build_same_type_ts_filter(c, req.timestamp_range)
            })
            .into_iter()
            .collect::<Vec<_>>();

        for path in paths {
            let file_metadata = self
                .collect_metadata(&object_store, format.clone(), path)
                .await?;

            validate_csv_headers_if_required(&file_metadata, &table_schema)?;
            let schema_mapping = copy_from_schema_mapping(&file_metadata, &table_schema);
            let projected_file_schema = Arc::new(
                file_metadata
                    .schema()
                    .project(&schema_mapping.file_projection)
                    .context(error::ProjectSchemaSnafu)?,
            );
            let projected_table_schema = Arc::new(
                table_schema
                    .project(&schema_mapping.table_projection)
                    .context(error::ProjectSchemaSnafu)?,
            );
            ensure_schema_compatible(&projected_file_schema, &projected_table_schema)?;

            files.push((
                Arc::new(schema_mapping.compat_file_schema),
                schema_mapping.file_projection,
                projected_table_schema,
                file_metadata,
            ))
        }

        let mut rows_inserted = 0;
        let mut insert_cost = 0;
        let max_insert_rows = req
            .limit
            .map(|n| {
                usize::try_from(n).map_err(|_| {
                    error::InvalidCopyParameterSnafu {
                        key: "limit".to_string(),
                        value: n.to_string(),
                    }
                    .build()
                })
            })
            .transpose()?;
        if max_insert_rows == Some(0) {
            return Ok(gen_insert_output(rows_inserted, insert_cost));
        }

        let mut accepted_rows = 0;
        for (compat_schema, file_schema_projection, projected_table_schema, file_metadata) in files
        {
            let mut stream = self
                .build_read_stream(
                    compat_schema,
                    &object_store,
                    &file_metadata,
                    file_schema_projection,
                    filters.clone(),
                )
                .await?;

            let fields = projected_table_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();

            // TODO(hl): make this configurable through options.
            let pending_mem_threshold = ReadableSize::mb(32).as_bytes();
            let mut pending_mem_size = 0;
            let mut pending = vec![];

            while let Some(r) = stream.next().await {
                let record_batch = r.context(error::ReadDfRecordBatchSnafu)?;
                let record_batch = if let Some(max_insert_rows) = max_insert_rows {
                    let remaining_rows = max_insert_rows - accepted_rows;
                    if record_batch.num_rows() > remaining_rows {
                        record_batch.slice(0, remaining_rows)
                    } else {
                        record_batch
                    }
                } else {
                    record_batch
                };
                let record_batch_rows = record_batch.num_rows();
                let vectors =
                    Helper::try_into_vectors(record_batch.columns()).context(IntoVectorsSnafu)?;

                pending_mem_size += vectors.iter().map(|v| v.memory_size()).sum::<usize>();

                let columns_values = fields
                    .iter()
                    .cloned()
                    .zip(vectors)
                    .collect::<HashMap<_, _>>();

                pending.push(self.inserter.handle_table_insert(
                    InsertRequest {
                        catalog_name: req.catalog_name.clone(),
                        schema_name: req.schema_name.clone(),
                        table_name: req.table_name.clone(),
                        columns_values,
                        skip_wal: query_ctx.skip_wal(),
                    },
                    query_ctx.clone(),
                ));
                accepted_rows += record_batch_rows;

                if pending_mem_size as u64 >= pending_mem_threshold {
                    let (rows, cost) = batch_insert(&mut pending, &mut pending_mem_size).await?;
                    rows_inserted += rows;
                    insert_cost += cost;
                }

                if let Some(max_insert_rows) = max_insert_rows
                    && accepted_rows == max_insert_rows
                {
                    if !pending.is_empty() {
                        let (rows, cost) =
                            batch_insert(&mut pending, &mut pending_mem_size).await?;
                        rows_inserted += rows;
                        insert_cost += cost;
                    }
                    return Ok(gen_insert_output(rows_inserted, insert_cost));
                }
            }

            if !pending.is_empty() {
                let (rows, cost) = batch_insert(&mut pending, &mut pending_mem_size).await?;
                rows_inserted += rows;
                insert_cost += cost;
            }
        }

        Ok(gen_insert_output(rows_inserted, insert_cost))
    }
}

fn gen_insert_output(rows_inserted: usize, insert_cost: usize) -> Output {
    Output::new(
        OutputData::AffectedRows(rows_inserted),
        OutputMeta::new_with_cost(insert_cost),
    )
}

struct SkipBadRecordsStream {
    inner: DfSendableRecordBatchStream,
    path: String,
}

impl SkipBadRecordsStream {
    fn new(inner: DfSendableRecordBatchStream, path: impl Into<String>) -> Self {
        Self {
            inner,
            path: path.into(),
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for SkipBadRecordsStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for SkipBadRecordsStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Err(error))) if is_skippable_record_error(&error) => {
                    common_telemetry::warn!(
                        "Skipping bad record while copying from {}: {}",
                        this.path,
                        error
                    );
                    continue;
                }
                other => return other,
            }
        }
    }
}

fn is_skippable_record_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::ArrowError(error, _) => is_skippable_arrow_error(error),
        DataFusionError::External(error) => error
            .downcast_ref::<ArrowError>()
            .is_some_and(is_skippable_arrow_error),
        DataFusionError::Context(_, error) => is_skippable_record_error(error),
        _ => false,
    }
}

/// Executes all pending inserts all at once, drain pending requests and reset pending bytes.
async fn batch_insert(
    pending: &mut Vec<impl Future<Output = Result<Output>>>,
    pending_bytes: &mut usize,
) -> Result<(OutputRows, OutputCost)> {
    let batch = pending.drain(..);
    let result = futures::future::try_join_all(batch)
        .await?
        .iter()
        .map(|o| o.extract_rows_and_cost())
        .reduce(|(a, b), (c, d)| (a + c, b + d))
        .unwrap_or((0, 0));
    *pending_bytes = 0;
    Ok(result)
}

/// Custom type compatibility check for GreptimeDB that handles Map -> Binary (JSON) conversion
fn can_cast_types_for_greptime(from: &ArrowDataType, to: &ArrowDataType) -> bool {
    // Handle Map -> Binary conversion for JSON types
    if let ArrowDataType::Map(_, _) = from
        && let ArrowDataType::Binary = to
    {
        return true;
    }

    // For all other cases, use Arrow's built-in can_cast_types
    can_cast_types(from, to)
}

fn csv_reader_schema_for_skip_bad_records(file: &SchemaRef, compat: &SchemaRef) -> Schema {
    let fields = file
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, file_field)| match compat.fields().get(idx) {
            Some(compat_field) if can_csv_reader_parse_type(compat_field.data_type()) => {
                compat_field.clone()
            }
            _ => file_field.clone(),
        })
        .collect::<Vec<_>>();

    Schema::new_with_metadata(fields, file.metadata().clone())
}

fn can_csv_reader_parse_type(data_type: &ArrowDataType) -> bool {
    match data_type {
        ArrowDataType::Boolean
        | ArrowDataType::Decimal32(_, _)
        | ArrowDataType::Decimal64(_, _)
        | ArrowDataType::Decimal128(_, _)
        | ArrowDataType::Decimal256(_, _)
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Null
        | ArrowDataType::Utf8
        | ArrowDataType::Utf8View => true,
        ArrowDataType::Dictionary(_, value_type) => value_type.as_ref() == &ArrowDataType::Utf8,
        _ => false,
    }
}

fn ensure_schema_compatible(from: &SchemaRef, to: &SchemaRef) -> Result<()> {
    let not_match = from
        .fields
        .iter()
        .zip(to.fields.iter())
        .map(|(l, r)| (l.data_type(), r.data_type()))
        .enumerate()
        .find(|(_, (l, r))| !can_cast_types_for_greptime(l, r));

    if let Some((index, _)) = not_match {
        error::InvalidSchemaSnafu {
            index,
            table_schema: to.to_string(),
            file_schema: from.to_string(),
        }
        .fail()
    } else {
        Ok(())
    }
}

fn validate_csv_headers_if_required(file_metadata: &FileMetadata, table: &SchemaRef) -> Result<()> {
    let FileMetadata::Csv {
        schema,
        format,
        path,
    } = file_metadata
    else {
        return Ok(());
    };

    if !format.strict_headers {
        return Ok(());
    }

    let mut seen_file_columns = HashSet::with_capacity(schema.fields().len());
    let duplicate_columns = schema
        .fields()
        .iter()
        .filter_map(|field| {
            if seen_file_columns.insert(field.name().clone()) {
                None
            } else {
                Some(field.name().clone())
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let file_columns = seen_file_columns.into_iter().collect::<BTreeSet<_>>();
    let table_columns = table
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<BTreeSet<_>>();
    let unknown_columns = file_columns
        .difference(&table_columns)
        .cloned()
        .collect::<Vec<_>>();
    let missing_columns = table_columns
        .difference(&file_columns)
        .cloned()
        .collect::<Vec<_>>();

    ensure!(
        unknown_columns.is_empty() && missing_columns.is_empty() && duplicate_columns.is_empty(),
        error::CsvHeaderMismatchSnafu {
            path,
            unknown_columns,
            missing_columns,
            duplicate_columns,
        }
    );

    Ok(())
}

/// Generates a maybe compatible schema of the file schema.
///
/// If there is a field is found in table schema,
/// copy the field data type to maybe compatible schema(`compatible_fields`).
fn generated_schema_projection_and_compatible_file_schema(
    file: &SchemaRef,
    table: &SchemaRef,
) -> (Vec<usize>, Vec<usize>, Schema) {
    let mut file_projection = Vec::with_capacity(file.fields.len());
    let mut table_projection = Vec::with_capacity(file.fields.len());
    let mut compatible_fields = file.fields.iter().cloned().collect::<Vec<_>>();
    for (file_idx, file_field) in file.fields.iter().enumerate() {
        if let Some((table_idx, table_field)) = table.fields.find(file_field.name()) {
            file_projection.push(file_idx);
            table_projection.push(table_idx);

            // Safety: the compatible_fields has same length as file schema
            compatible_fields[file_idx] = table_field.clone();
        }
    }

    (
        file_projection,
        table_projection,
        Schema::new(compatible_fields),
    )
}

struct CopyFromSchemaMapping {
    file_projection: Vec<usize>,
    table_projection: Vec<usize>,
    compat_file_schema: Schema,
}

fn copy_from_schema_mapping(
    file_metadata: &FileMetadata,
    table: &SchemaRef,
) -> CopyFromSchemaMapping {
    match file_metadata {
        FileMetadata::Csv { schema, format, .. } if !format.has_header => {
            generated_positional_schema_projection_and_compatible_file_schema(schema, table)
        }
        _ => {
            let (file_projection, table_projection, compat_file_schema) =
                generated_schema_projection_and_compatible_file_schema(
                    file_metadata.schema(),
                    table,
                );
            CopyFromSchemaMapping {
                file_projection,
                table_projection,
                compat_file_schema,
            }
        }
    }
}

fn generated_positional_schema_projection_and_compatible_file_schema(
    file: &SchemaRef,
    table: &SchemaRef,
) -> CopyFromSchemaMapping {
    let len = file.fields.len().min(table.fields.len());
    let file_projection = (0..len).collect::<Vec<_>>();
    let table_projection = (0..len).collect::<Vec<_>>();
    let compatible_fields = file
        .fields
        .iter()
        .enumerate()
        .map(|(idx, file_field)| {
            if idx < len {
                table.fields[idx].clone()
            } else {
                file_field.clone()
            }
        })
        .collect::<Vec<_>>();

    CopyFromSchemaMapping {
        file_projection,
        table_projection,
        compat_file_schema: Schema::new(compatible_fields),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn copy_from_request(location: &str, pattern: Option<&str>) -> CopyTableRequest {
        CopyTableRequest {
            catalog_name: "greptime".into(),
            schema_name: "public".into(),
            table_name: "test".into(),
            location: location.into(),
            with: HashMap::new(),
            connection: HashMap::new(),
            pattern: pattern.map(String::from),
            direction: table::requests::CopyDirection::Import,
            timestamp_range: None,
            limit: None,
        }
    }

    #[tokio::test]
    async fn test_copy_from_local_paths() {
        use common_test_util::temp_dir::create_temp_dir;

        let dir = create_temp_dir("copy_from_paths");
        let nested = dir.path().join("nested");
        std::fs::create_dir_all(nested.join("data.dir")).unwrap();
        std::fs::write(nested.join("data file.parquet"), b"data").unwrap();
        std::fs::write(nested.join("other.csv"), b"other").unwrap();
        let access = LocalFileAccess::sandboxed(dir.path()).unwrap();
        for location in [
            "nested/data file.parquet".to_string(),
            url::Url::from_file_path(nested.join("data file.parquet"))
                .unwrap()
                .to_string(),
        ] {
            // PATTERN applies to directories, not an explicitly selected file.
            let req = copy_from_request(&location, Some("does-not-match"));
            let (store, paths) = list_copy_from_paths(&req, &access).await.unwrap();
            assert_eq!(paths, ["data file.parquet"]);
            assert_eq!(store.read(&paths[0]).await.unwrap().to_vec(), b"data");
        }

        let req = copy_from_request("nested/", Some("^data"));
        let (_, paths) = list_copy_from_paths(&req, &access).await.unwrap();
        assert_eq!(paths, ["data file.parquet"]);
        let req = copy_from_request("nested/", None);
        let (_, mut paths) = list_copy_from_paths(&req, &access).await.unwrap();
        paths.sort();
        assert_eq!(paths, ["data file.parquet", "other.csv"]);
        let req = copy_from_request("nested/data.dir", None);
        assert!(
            list_copy_from_paths(&req, &access)
                .await
                .unwrap()
                .1
                .is_empty()
        );
        let req = copy_from_request("nested/", Some("does-not-match"));
        assert!(
            list_copy_from_paths(&req, &access)
                .await
                .unwrap()
                .1
                .is_empty()
        );

        let req = copy_from_request("nested/missing.parquet", None);
        assert!(matches!(
            list_copy_from_paths(&req, &access).await,
            Err(error::Error::ListObjects {
                source: common_datasource::error::Error::ListObjects { error, .. }, ..
            }) if error.kind() == object_store::ErrorKind::NotFound
        ));
        let req = copy_from_request("nested/data file.parquet", Some("["));
        assert!(matches!(
            list_copy_from_paths(&req, &access).await,
            Err(error::Error::BuildRegex { .. })
        ));
        let req = copy_from_request("nested/data file.parquet", None);
        assert!(matches!(
            list_copy_from_paths(&req, &LocalFileAccess::Disabled).await,
            Err(error::Error::BuildBackend {
                source: common_datasource::error::Error::LocalFileAccessDisabled { .. },
                ..
            })
        ));
        let req = copy_from_request("../outside.parquet", None);
        assert!(matches!(
            list_copy_from_paths(&req, &access).await,
            Err(error::Error::BuildBackend {
                source: common_datasource::error::Error::LocalFileAccessDenied { .. },
                ..
            })
        ));
    }

    #[tokio::test]
    async fn test_copy_from_s3_paths_without_relisting() {
        use std::sync::Mutex;

        use axum::Router;
        use axum::http::{Method, StatusCode, Uri};
        use axum::response::IntoResponse;

        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let app = Router::new().fallback(move |method: Method, uri: Uri| {
            let recorded = recorded.clone();
            async move {
                recorded.lock().unwrap().push((method.clone(), uri.clone()));
                if method == Method::HEAD {
                    let status = if uri.path().ends_with("missing.parquet") {
                        StatusCode::NOT_FOUND
                    } else if uri.path().ends_with("denied.parquet") {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::OK
                    };
                    return (status, [("content-length", "0")]).into_response();
                }
                assert_eq!(method, Method::GET);
                let query =
                    axum::extract::Query::<HashMap<String, String>>::try_from_uri(&uri).unwrap();
                assert_eq!(query.get("prefix").unwrap(), "backup/schema/1/");
                assert_eq!(query.get("list-type").unwrap(), "2");
                let files = (0..16).map(|i| format!(
                    "<Contents><Key>backup/schema/1/data.{i}.parquet</Key><LastModified>2026-09-01T00:00:00Z</LastModified><Size>0</Size></Contents>"
                )).collect::<String>();
                format!(
                    "<ListBucketResult><IsTruncated>false</IsTruncated>{files}\
                    <Contents><Key>backup/schema/1/other.csv</Key><LastModified>2026-09-01T00:00:00Z</LastModified><Size>0</Size></Contents>\
                    <CommonPrefixes><Prefix>backup/schema/1/data.dir/</Prefix></CommonPrefixes>\
                    </ListBucketResult>"
                )
                .into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let connection = HashMap::from([
            ("endpoint".into(), endpoint),
            ("region".into(), "us-east-1".into()),
            ("access_key_id".into(), "test".into()),
            ("secret_access_key".into(), "test".into()),
            ("disable_ec2_metadata".into(), "true".into()),
        ]);
        for i in 0..16 {
            let filename = format!("data.{i}.parquet");
            let mut req = copy_from_request(
                &format!("s3://bucket/backup/schema/1/{filename}"),
                Some("does-not-match"),
            );
            req.connection = connection.clone();
            let (_, paths) = list_copy_from_paths(&req, &LocalFileAccess::Disabled)
                .await
                .unwrap();
            assert_eq!(paths, [filename]);
        }
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 16);
            assert!(requests.iter().all(|(method, uri)| method == Method::HEAD
                && uri.path().starts_with("/bucket/backup/schema/1/")));
        }
        let mut req = copy_from_request("s3://bucket/backup/schema/1/", Some(r"^data\."));
        req.connection = connection.clone();
        let (_, mut paths) = list_copy_from_paths(&req, &LocalFileAccess::Disabled)
            .await
            .unwrap();
        paths.sort();
        let mut expected = (0..16)
            .map(|i| format!("data.{i}.parquet"))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(paths, expected);
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(method, _)| *method == Method::GET)
                .count(),
            1
        );

        for (name, kind) in [
            ("missing", object_store::ErrorKind::NotFound),
            ("denied", object_store::ErrorKind::PermissionDenied),
        ] {
            let mut req =
                copy_from_request(&format!("s3://bucket/backup/schema/1/{name}.parquet"), None);
            req.connection = connection.clone();
            assert!(matches!(
                list_copy_from_paths(&req, &LocalFileAccess::Disabled).await,
                Err(error::Error::ListObjects {
                    source: common_datasource::error::Error::ListObjects { error, .. }, ..
                }) if error.kind() == kind
            ));
        }
        server.abort();
    }

    fn test_schema_matches(from: (DataType, bool), to: (DataType, bool), matches: bool) {
        let s1 = Arc::new(Schema::new(vec![Field::new("col", from.0.clone(), from.1)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("col", to.0.clone(), to.1)]));
        let res = ensure_schema_compatible(&s1, &s2);
        assert_eq!(
            matches,
            res.is_ok(),
            "from data type: {}, to data type: {}, expected: {}, but got: {}",
            from.0,
            to.0,
            matches,
            res.is_ok()
        )
    }

    #[test]
    fn test_ensure_datatype_matches_ignore_timezone() {
        test_schema_matches(
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("PDT".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches((DataType::Int8, true), (DataType::Int8, true), true);

        test_schema_matches((DataType::Int8, true), (DataType::Int16, true), true);
    }

    #[test]
    fn test_data_type_equals_ignore_timezone_with_options() {
        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("PDT".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches(
            (DataType::Utf8, true),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("PDT".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("PDT".into()),
                ),
                true,
            ),
            (DataType::Utf8, true),
            true,
        );
    }

    #[test]
    fn test_map_to_binary_json_compatibility() {
        // Test Map -> Binary conversion for JSON types
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        test_schema_matches((map_type, false), (DataType::Binary, true), true);

        test_schema_matches((DataType::Int8, true), (DataType::Int16, true), true);
        test_schema_matches((DataType::Utf8, true), (DataType::Binary, true), true);
    }

    fn make_test_schema(v: &[Field]) -> Arc<Schema> {
        Arc::new(Schema::new(v.to_vec()))
    }

    #[test]
    fn test_compatible_file_schema() {
        let file_schema0 = make_test_schema(&[
            Field::new("c1", DataType::UInt8, true),
            Field::new("c2", DataType::UInt8, true),
        ]);

        let table_schema = make_test_schema(&[
            Field::new("c1", DataType::Int16, true),
            Field::new("c2", DataType::Int16, true),
            Field::new("c3", DataType::Int16, true),
        ]);

        let compat_schema = make_test_schema(&[
            Field::new("c1", DataType::Int16, true),
            Field::new("c2", DataType::Int16, true),
        ]);

        let (_, tp, _) =
            generated_schema_projection_and_compatible_file_schema(&file_schema0, &table_schema);

        assert_eq!(table_schema.project(&tp).unwrap(), *compat_schema);
    }

    #[test]
    fn test_schema_projection() {
        let file_schema0 = make_test_schema(&[
            Field::new("c1", DataType::UInt8, true),
            Field::new("c2", DataType::UInt8, true),
            Field::new("c3", DataType::UInt8, true),
        ]);

        let file_schema1 = make_test_schema(&[
            Field::new("c3", DataType::UInt8, true),
            Field::new("c4", DataType::UInt8, true),
        ]);

        let file_schema2 = make_test_schema(&[
            Field::new("c3", DataType::UInt8, true),
            Field::new("c4", DataType::UInt8, true),
            Field::new("c5", DataType::UInt8, true),
        ]);

        let file_schema3 = make_test_schema(&[
            Field::new("c1", DataType::UInt8, true),
            Field::new("c2", DataType::UInt8, true),
        ]);

        let table_schema = make_test_schema(&[
            Field::new("c3", DataType::UInt8, true),
            Field::new("c4", DataType::UInt8, true),
            Field::new("c5", DataType::UInt8, true),
        ]);

        let tests = [
            (&file_schema0, &table_schema, true), // intersection
            (&file_schema1, &table_schema, true), // subset
            (&file_schema2, &table_schema, true), // full-eq
            (&file_schema3, &table_schema, true), // non-intersection
        ];

        for test in tests {
            let (fp, tp, _) =
                generated_schema_projection_and_compatible_file_schema(test.0, test.1);
            assert_eq!(test.0.project(&fp).unwrap(), test.1.project(&tp).unwrap());
        }
    }

    #[test]
    fn test_csv_reader_schema_for_skip_bad_records() {
        let file_schema = make_test_schema(&[
            Field::new("id", DataType::Utf8, true),
            Field::new("jsons", DataType::Utf8, true),
            Field::new("ts", DataType::Utf8, true),
        ]);
        let compat_schema = make_test_schema(&[
            Field::new("id", DataType::UInt32, true),
            Field::new("jsons", DataType::Binary, true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ]);

        let reader_schema = csv_reader_schema_for_skip_bad_records(&file_schema, &compat_schema);

        assert_eq!(reader_schema.field(0).data_type(), &DataType::UInt32);
        assert_eq!(reader_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            reader_schema.field(2).data_type(),
            compat_schema.field(2).data_type()
        );
    }

    fn make_csv_metadata(schema: Arc<Schema>, has_header: bool) -> FileMetadata {
        FileMetadata::Csv {
            schema,
            format: CsvFormat {
                has_header,
                ..CsvFormat::default()
            },
            path: "test.csv".to_string(),
        }
    }

    fn make_strict_csv_metadata(schema: Arc<Schema>) -> FileMetadata {
        FileMetadata::Csv {
            schema,
            format: CsvFormat {
                strict_headers: true,
                ..CsvFormat::default()
            },
            path: "test.csv".to_string(),
        }
    }

    fn assert_field(schema: &Schema, idx: usize, name: &str, data_type: &DataType) {
        let field = schema.field(idx);
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type);
    }

    #[test]
    fn test_strict_csv_headers_allows_reordered_columns() {
        let file_schema = make_test_schema(&[
            Field::new("ts", DataType::Utf8, true),
            Field::new("host_id", DataType::UInt8, true),
            Field::new("reading_value", DataType::Float64, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        validate_csv_headers_if_required(&make_strict_csv_metadata(file_schema), &table_schema)
            .unwrap();
    }

    #[test]
    fn test_strict_csv_headers_rejects_unknown_columns() {
        let file_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt8, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        let err =
            validate_csv_headers_if_required(&make_strict_csv_metadata(file_schema), &table_schema)
                .unwrap_err();

        assert!(matches!(
            err,
            error::Error::CsvHeaderMismatch {
                unknown_columns,
                missing_columns,
                duplicate_columns,
                ..
            } if unknown_columns == vec!["extra".to_string()]
                && missing_columns.is_empty()
                && duplicate_columns.is_empty()
        ));
    }

    #[test]
    fn test_strict_csv_headers_rejects_missing_columns() {
        let file_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt8, true),
            Field::new("ts", DataType::Utf8, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        let err =
            validate_csv_headers_if_required(&make_strict_csv_metadata(file_schema), &table_schema)
                .unwrap_err();

        assert!(matches!(
            err,
            error::Error::CsvHeaderMismatch {
                unknown_columns,
                missing_columns,
                duplicate_columns,
                ..
            } if unknown_columns.is_empty()
                && missing_columns == vec!["reading_value".to_string()]
                && duplicate_columns.is_empty()
        ));
    }

    #[test]
    fn test_strict_csv_headers_rejects_duplicate_columns() {
        let file_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt8, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
            Field::new("host_id", DataType::UInt16, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        let err =
            validate_csv_headers_if_required(&make_strict_csv_metadata(file_schema), &table_schema)
                .unwrap_err();

        assert!(matches!(
            err,
            error::Error::CsvHeaderMismatch {
                unknown_columns,
                missing_columns,
                duplicate_columns,
                ..
            } if unknown_columns.is_empty()
                && missing_columns.is_empty()
                && duplicate_columns == vec!["host_id".to_string()]
        ));
    }

    #[test]
    fn test_headerless_csv_schema_projection_is_positional() {
        let file_schema = make_test_schema(&[
            Field::new("column_1", DataType::UInt8, true),
            Field::new("column_2", DataType::Float64, true),
            Field::new("column_3", DataType::Utf8, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ]);

        let mapping =
            copy_from_schema_mapping(&make_csv_metadata(file_schema, false), &table_schema);

        assert_eq!(mapping.file_projection, vec![0, 1, 2]);
        assert_eq!(mapping.table_projection, vec![0, 1, 2]);
        assert_field(&mapping.compat_file_schema, 0, "host_id", &DataType::UInt32);
        assert_field(
            &mapping.compat_file_schema,
            1,
            "reading_value",
            &DataType::Float64,
        );
        assert_field(
            &mapping.compat_file_schema,
            2,
            "ts",
            table_schema.field(2).data_type(),
        );
        assert_eq!(
            mapping
                .compat_file_schema
                .project(&mapping.file_projection)
                .unwrap(),
            table_schema.project(&mapping.table_projection).unwrap()
        );
    }

    #[test]
    fn test_headerless_csv_schema_projection_ignores_extra_file_columns() {
        let file_schema = make_test_schema(&[
            Field::new("column_1", DataType::UInt8, true),
            Field::new("column_2", DataType::Float64, true),
            Field::new("column_3", DataType::Utf8, true),
            Field::new("column_4", DataType::Utf8, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        let mapping =
            copy_from_schema_mapping(&make_csv_metadata(file_schema, false), &table_schema);

        assert_eq!(mapping.file_projection, vec![0, 1, 2]);
        assert_eq!(mapping.table_projection, vec![0, 1, 2]);
        assert_eq!(mapping.compat_file_schema.fields().len(), 4);
        assert_field(&mapping.compat_file_schema, 0, "host_id", &DataType::UInt32);
        assert_field(
            &mapping.compat_file_schema,
            1,
            "reading_value",
            &DataType::Float64,
        );
        assert_field(&mapping.compat_file_schema, 2, "ts", &DataType::Utf8);
        assert_field(&mapping.compat_file_schema, 3, "column_4", &DataType::Utf8);
    }

    #[test]
    fn test_headerless_csv_schema_projection_supports_prefix_import() {
        let file_schema = make_test_schema(&[
            Field::new("column_1", DataType::UInt8, true),
            Field::new("column_2", DataType::Float64, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("reading_value", DataType::Float64, true),
            Field::new("ts", DataType::Utf8, true),
        ]);

        let mapping =
            copy_from_schema_mapping(&make_csv_metadata(file_schema, false), &table_schema);

        assert_eq!(mapping.file_projection, vec![0, 1]);
        assert_eq!(mapping.table_projection, vec![0, 1]);
        assert_field(&mapping.compat_file_schema, 0, "host_id", &DataType::UInt32);
        assert_field(
            &mapping.compat_file_schema,
            1,
            "reading_value",
            &DataType::Float64,
        );
        assert_eq!(
            mapping
                .compat_file_schema
                .project(&mapping.file_projection)
                .unwrap(),
            table_schema.project(&mapping.table_projection).unwrap()
        );
    }

    #[test]
    fn test_csv_reader_schema_for_skip_bad_records_uses_positional_mapping() {
        let file_schema = make_test_schema(&[
            Field::new("column_1", DataType::Utf8, true),
            Field::new("column_2", DataType::Utf8, true),
            Field::new("column_3", DataType::Utf8, true),
        ]);
        let table_schema = make_test_schema(&[
            Field::new("host_id", DataType::UInt32, true),
            Field::new("jsons", DataType::Binary, true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ]);
        let mapping = copy_from_schema_mapping(
            &make_csv_metadata(file_schema.clone(), false),
            &table_schema,
        );
        let compat_schema = Arc::new(mapping.compat_file_schema);

        let reader_schema = csv_reader_schema_for_skip_bad_records(&file_schema, &compat_schema);

        assert_eq!(reader_schema.field(0).data_type(), &DataType::UInt32);
        assert_eq!(reader_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            reader_schema.field(2).data_type(),
            table_schema.field(2).data_type()
        );
    }
}

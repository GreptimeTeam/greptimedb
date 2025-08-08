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
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use client::{Output, OutputData, OutputMeta};
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::CsvFormat;
use common_datasource::file_format::orc::{infer_orc_schema, new_orc_stream_reader, ReaderAdapter};
use common_datasource::file_format::{FileFormat, Format};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{build_backend, parse_url, FS_SCHEMA};
use common_datasource::util::find_dir_and_filename;
use common_query::{OutputCost, OutputRows};
use common_recordbatch::adapter::RecordBatchStreamTypeAdapter;
use common_recordbatch::DfSendableRecordBatchStream;
use common_telemetry::{debug, tracing};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, FileSource, FileStream, JsonSource,
};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_expr::Expr;
use datatypes::arrow::compute::can_cast_types;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use datatypes::vectors::Helper;
use futures_util::StreamExt;
use object_store::{Entry, EntryMode, ObjectStore};
use regex::Regex;
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use table::requests::{CopyTableRequest, InsertRequest};
use table::table_reference::TableReference;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::error::{self, IntoVectorsSnafu, PathNotFoundSnafu, Result};
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

impl StatementExecutor {
    async fn list_copy_from_entries(
        &self,
        req: &CopyTableRequest,
    ) -> Result<(ObjectStore, Vec<Entry>)> {
        let (schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;

        if schema.to_uppercase() == FS_SCHEMA {
            ensure!(Path::new(&path).exists(), PathNotFoundSnafu { path });
        }

        let object_store =
            build_backend(&req.location, &req.connection).context(error::BuildBackendSnafu)?;

        let (dir, filename) = find_dir_and_filename(&path);
        let regex = req
            .pattern
            .as_ref()
            .map(|x| Regex::new(x))
            .transpose()
            .context(error::BuildRegexSnafu)?;

        let source = if let Some(filename) = filename {
            Source::Filename(filename)
        } else {
            Source::Dir
        };

        let lister = Lister::new(object_store.clone(), source.clone(), dir.to_string(), regex);

        let entries = lister.list().await.context(error::ListObjectsSnafu)?;
        debug!("Copy from dir: {dir:?}, {source:?}, entries: {entries:?}");
        Ok((object_store, entries))
    }

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

    async fn build_file_stream(
        &self,
        store: &ObjectStore,
        filename: &str,
        file_schema: SchemaRef,
        file_source: Arc<dyn FileSource>,
    ) -> Result<DfSendableRecordBatchStream> {
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            file_schema,
            file_source.clone(),
        )
        .with_file_group(FileGroup::new(vec![PartitionedFile::new(filename, 0)]))
        .build();

        let store = Arc::new(object_store_opendal::OpendalStore::new(store.clone()));
        let file_opener = file_source.create_file_opener(store, &config, 0);
        let stream = FileStream::new(&config, 0, file_opener, &ExecutionPlanMetricsSet::new())
            .context(error::BuildFileStreamSnafu)?;

        Ok(Box::pin(stream))
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

                let projected_file_schema = Arc::new(
                    schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );

                let csv_source = CsvSource::new(format.has_header, format.delimiter, b'"')
                    .with_schema(projected_file_schema.clone())
                    .with_batch_size(DEFAULT_BATCH_SIZE);

                let stream = self
                    .build_file_stream(object_store, path, projected_file_schema, csv_source)
                    .await?;

                Ok(Box::pin(
                    // The projection is already applied in the CSV reader when we created the stream,
                    // so we pass None here to avoid double projection which would cause schema mismatch errors.
                    RecordBatchStreamTypeAdapter::new(output_schema, stream, None)
                        .with_filter(filters)
                        .context(error::PhysicalExprSnafu)?,
                ))
            }
            FileMetadata::Json { path, schema } => {
                let projected_file_schema = Arc::new(
                    schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );
                let output_schema = Arc::new(
                    compat_schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );
                let stream = self
                    .build_file_stream(
                        object_store,
                        path,
                        projected_file_schema,
                        Arc::new(JsonSource::new()),
                    )
                    .await?;

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
        let (object_store, entries) = self.list_copy_from_entries(&req).await?;
        let mut files = Vec::with_capacity(entries.len());
        let table_schema = table.schema().arrow_schema().clone();
        let filters = table
            .schema()
            .timestamp_column()
            .and_then(|c| {
                common_query::logical_plan::build_same_type_ts_filter(c, req.timestamp_range)
            })
            .into_iter()
            .collect::<Vec<_>>();

        for entry in entries.iter() {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let path = entry.path();
            let file_metadata = self
                .collect_metadata(&object_store, format, path.to_string())
                .await?;

            let file_schema = file_metadata.schema();
            let (file_schema_projection, table_schema_projection, compat_schema) =
                generated_schema_projection_and_compatible_file_schema(file_schema, &table_schema);
            let projected_file_schema = Arc::new(
                file_schema
                    .project(&file_schema_projection)
                    .context(error::ProjectSchemaSnafu)?,
            );
            let projected_table_schema = Arc::new(
                table_schema
                    .project(&table_schema_projection)
                    .context(error::ProjectSchemaSnafu)?,
            );
            ensure_schema_compatible(&projected_file_schema, &projected_table_schema)?;

            files.push((
                Arc::new(compat_schema),
                file_schema_projection,
                projected_table_schema,
                file_metadata,
            ))
        }

        let mut rows_inserted = 0;
        let mut insert_cost = 0;
        let max_insert_rows = req.limit.map(|n| n as usize);
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
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();

            // TODO(hl): make this configurable through options.
            let pending_mem_threshold = ReadableSize::mb(32).as_bytes();
            let mut pending_mem_size = 0;
            let mut pending = vec![];

            while let Some(r) = stream.next().await {
                let record_batch = r.context(error::ReadDfRecordBatchSnafu)?;
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
                        catalog_name: req.catalog_name.to_string(),
                        schema_name: req.schema_name.to_string(),
                        table_name: req.table_name.to_string(),
                        columns_values,
                    },
                    query_ctx.clone(),
                ));

                if pending_mem_size as u64 >= pending_mem_threshold {
                    let (rows, cost) = batch_insert(&mut pending, &mut pending_mem_size).await?;
                    rows_inserted += rows;
                    insert_cost += cost;
                }

                if let Some(max_insert_rows) = max_insert_rows {
                    if rows_inserted >= max_insert_rows {
                        return Ok(gen_insert_output(rows_inserted, insert_cost));
                    }
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
    if let ArrowDataType::Map(_, _) = from {
        if let ArrowDataType::Binary = to {
            return true;
        }
    }

    // For all other cases, use Arrow's built-in can_cast_types
    can_cast_types(from, to)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

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
}

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
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::{CsvConfigBuilder, CsvOpener};
use common_datasource::file_format::json::JsonOpener;
use common_datasource::file_format::orc::{
    infer_orc_schema, new_orc_stream_reader, OrcArrowStreamReaderAdapter,
};
use common_datasource::file_format::{FileFormat, Format};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{build_backend, parse_url};
use common_datasource::util::find_dir_and_filename;
use common_recordbatch::adapter::ParquetRecordBatchStreamAdapter;
use common_recordbatch::DfSendableRecordBatchStream;
use common_telemetry::{debug, tracing};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileStream};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::compute::can_cast_types;
use datatypes::arrow::datatypes::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use futures_util::StreamExt;
use object_store::{Entry, EntryMode, ObjectStore};
use regex::Regex;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::{CopyTableRequest, InsertRequest};
use tokio::io::BufReader;

use crate::error::{self, IntoVectorsSnafu, Result};
use crate::statement::StatementExecutor;

const DEFAULT_BATCH_SIZE: usize = 8192;

impl StatementExecutor {
    async fn list_copy_from_entries(
        &self,
        req: &CopyTableRequest,
    ) -> Result<(ObjectStore, Vec<Entry>)> {
        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;

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

    async fn infer_schema(
        &self,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<SchemaRef> {
        match format {
            Format::Csv(format) => Ok(Arc::new(
                format
                    .infer_schema(&object_store, path)
                    .await
                    .context(error::InferSchemaSnafu { path })?,
            )),
            Format::Json(format) => Ok(Arc::new(
                format
                    .infer_schema(&object_store, path)
                    .await
                    .context(error::InferSchemaSnafu { path })?,
            )),
            Format::Parquet(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;

                let buf_reader = BufReader::new(reader);

                let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                    .await
                    .context(error::ReadParquetSnafu)?;
                Ok(builder.schema().clone())
            }
            Format::Orc(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;

                let schema = infer_orc_schema(reader)
                    .await
                    .context(error::ReadOrcSnafu)?;

                Ok(Arc::new(schema))
            }
        }
    }

    async fn build_file_stream<F: FileOpener + Send + 'static>(
        &self,
        opener: F,
        filename: &str,
        file_schema: SchemaRef,
    ) -> Result<DfSendableRecordBatchStream> {
        let stream = FileStream::new(
            &FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("empty://").unwrap(), // won't be used
                file_schema,
                file_groups: vec![vec![PartitionedFile::new(filename.to_string(), 10)]],
                statistics: Default::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![],
                infinite_source: false,
            },
            0,
            opener,
            &ExecutionPlanMetricsSet::new(),
        )
        .context(error::BuildFileStreamSnafu)?;

        Ok(Box::pin(stream))
    }

    async fn build_read_stream(
        &self,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<DfSendableRecordBatchStream> {
        match format {
            Format::Csv(format) => {
                let csv_conf = CsvConfigBuilder::default()
                    .batch_size(DEFAULT_BATCH_SIZE)
                    .file_schema(schema.clone())
                    .file_projection(Some(projection))
                    .build()
                    .context(error::BuildCsvConfigSnafu)?;

                self.build_file_stream(
                    CsvOpener::new(csv_conf, object_store, format.compression_type),
                    path,
                    schema,
                )
                .await
            }
            Format::Json(format) => {
                let projected_schema = Arc::new(
                    schema
                        .project(&projection)
                        .context(error::ProjectSchemaSnafu)?,
                );

                self.build_file_stream(
                    JsonOpener::new(
                        DEFAULT_BATCH_SIZE,
                        projected_schema,
                        object_store,
                        format.compression_type,
                    ),
                    path,
                    schema,
                )
                .await
            }
            Format::Parquet(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;
                let buf_reader = BufReader::new(reader);

                let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                    .await
                    .context(error::ReadParquetSnafu)?;

                let upstream = builder
                    .build()
                    .context(error::BuildParquetRecordBatchStreamSnafu)?;

                Ok(Box::pin(ParquetRecordBatchStreamAdapter::new(
                    schema,
                    upstream,
                    Some(projection),
                )))
            }
            Format::Orc(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;
                let stream = new_orc_stream_reader(reader)
                    .await
                    .context(error::ReadOrcSnafu)?;
                let stream = OrcArrowStreamReaderAdapter::new(schema, stream, Some(projection));

                Ok(Box::pin(stream))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn copy_table_from(
        &self,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
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

        for entry in entries.iter() {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let path = entry.path();
            let file_schema = self
                .infer_schema(&format, object_store.clone(), path)
                .await?;
            let (file_schema_projection, table_schema_projection, compat_schema) =
                generated_schema_projection_and_compatible_file_schema(&file_schema, &table_schema);

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
                path,
            ))
        }

        let mut rows_inserted = 0;
        for (schema, file_schema_projection, projected_table_schema, path) in files {
            let mut stream = self
                .build_read_stream(
                    &format,
                    object_store.clone(),
                    path,
                    schema,
                    file_schema_projection,
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
                    rows_inserted += batch_insert(&mut pending, &mut pending_mem_size).await?;
                }
            }

            if !pending.is_empty() {
                rows_inserted += batch_insert(&mut pending, &mut pending_mem_size).await?;
            }
        }

        Ok(rows_inserted)
    }
}

/// Executes all pending inserts all at once, drain pending requests and reset pending bytes.
async fn batch_insert(
    pending: &mut Vec<impl Future<Output = Result<usize>>>,
    pending_bytes: &mut usize,
) -> Result<usize> {
    let batch = pending.drain(..);
    let res: usize = futures::future::try_join_all(batch).await?.iter().sum();
    *pending_bytes = 0;
    Ok(res)
}

fn ensure_schema_compatible(from: &SchemaRef, to: &SchemaRef) -> Result<()> {
    let not_match = from
        .fields
        .iter()
        .zip(to.fields.iter())
        .map(|(l, r)| (l.data_type(), r.data_type()))
        .enumerate()
        .find(|(_, (l, r))| !can_cast_types(l, r));

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

/// Allows the file schema is a subset of table
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
